import hashlib
import uuid
from contextlib import nullcontext

from fastapi import HTTPException
from sqlalchemy import and_, or_, select

from src.config.settings import cache
from src.constants.workflow import Status
from src.models.tables import audit, cases, evidence, runs
from src.services import migration, topology
from src.utils.encoding import canonical
from src.utils.time import utc_time


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action,
                                       reference=str(reference), created_at=utc_time()))


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


# ---------------------------------------------------------------------------
# Read routing helpers
# ---------------------------------------------------------------------------

def _engine_aliases():
    """Source first, target second, de-duplicating identical engine objects."""
    pairs = [(topology.SOURCE, topology.source_engine()),
             (topology.TARGET, topology.target_engine())]
    result = []
    seen = set()
    for alias, engine in pairs:
        marker = id(engine)
        if marker in seen:
            continue
        seen.add(marker)
        result.append((alias, engine))
    return result


def _safe_rows(alias, engine, statement):
    try:
        with engine.connect() as conn:
            return [dict(row) for row in conn.execute(statement).mappings()]
    except Exception:
        if alias == topology.SOURCE:
            raise
        # Destination temporarily unavailable: reads fall back to source.
        return []


def _run_case_map():
    mapping = {}
    for alias, engine in _engine_aliases():
        for row in _safe_rows(alias, engine, select(runs.c.id, runs.c.case_id)):
            mapping.setdefault(int(row['id']), int(row['case_id']))
    return mapping


def recent(table):
    """Recent rows across libraries.

    Archive-scoped correctness lives in :func:`runs_for_case` /
    :func:`evidence_for_case`, which touch exactly the archive's home library.
    This global feed merges both libraries for the workbench list pages and
    de-duplicates migrated rows by original id, preferring the home copy.
    """
    statement = select(table).order_by(table.c.id.desc()).limit(100)

    # Audit is a control trail: never collapse distinct events that happen to
    # share an auto-increment id across the two libraries.
    if table is audit:
        merged = []
        for alias, engine in _engine_aliases():
            merged.extend(_safe_rows(alias, engine, statement))
        merged.sort(key=lambda r: (r.get('created_at') or '', r.get('id') or 0), reverse=True)
        return merged[:100]

    home_map = topology.home_map()
    run_case = _run_case_map() if table is evidence else {}

    def row_home(row):
        if table is cases:
            cid = row.get('id')
        elif table is runs:
            cid = row.get('case_id')
        else:  # evidence -> run -> case
            cid = run_case.get(int(row.get('run_id'))) if row.get('run_id') is not None else None
        return home_map.get(int(cid), topology.SOURCE) if cid is not None else topology.SOURCE

    best: dict[int, dict] = {}
    for alias, engine in _engine_aliases():
        for row in _safe_rows(alias, engine, statement):
            identity = row.get('id')
            authoritative = row_home(row)
            if identity not in best:
                best[identity] = row
            elif alias == authoritative:
                # This copy comes from the archive's current home library.
                best[identity] = row
    rows = list(best.values())
    rows.sort(key=lambda r: r.get('id') or 0, reverse=True)
    return rows[:100]


class ScopedRows(list):
    """A JSON list that also records which single library served it."""

    def __init__(self, rows, database):
        super().__init__(rows)
        self.database = database
        self.active_database = database


def runs_for_case(case_id):
    """试算记录只读档案当前所属的同一座库。"""
    with topology.case_lock(int(case_id)):
        home = topology.home_name(int(case_id))
        engine = topology.get_engine(home)
    with engine.connect() as conn:
        rows = conn.execute(select(runs).where(runs.c.case_id == int(case_id))
                            .order_by(runs.c.id.desc())).mappings()
        result = [dict(row) for row in rows]
    return ScopedRows(result, home)


def evidence_for_case(case_id):
    """报告（含复核意见）与试算来自同一座库。"""
    case_id = int(case_id)
    with topology.case_lock(case_id):
        home = topology.home_name(case_id)
        engine = topology.get_engine(home)
    statement = (select(evidence).join(runs, evidence.c.run_id == runs.c.id)
                 .where(runs.c.case_id == case_id).order_by(evidence.c.id.desc()))
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(statement).mappings()]
    return ScopedRows(rows, home)


def evidence_for_run(run_id):
    alias, run_row = _find_run_anywhere(run_id)
    if run_row is None:
        raise HTTPException(404, '找不到该试算')
    case_id = int(run_row['case_id'])
    home = topology.home_name(case_id)
    engine = topology.get_engine(home)
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(
            select(evidence).where(evidence.c.run_id == int(run_id))
            .order_by(evidence.c.id.desc())).mappings()]
    return ScopedRows(rows, home)


def case_detail(case_id):
    case_id = int(case_id)
    with topology.case_lock(case_id):
        home = topology.home_name(case_id)
        engine = topology.get_engine(home)
    with engine.connect() as conn:
        row = found(conn, cases, case_id)
    row['database'] = home
    row['active_database'] = home
    return row


def _locate_evidence(identity):
    """Return (alias, evidence_row, case_id) probing home first."""
    candidates = []
    run_case = _run_case_map()
    for alias, engine in _engine_aliases():
        try:
            with engine.connect() as conn:
                row = conn.execute(select(evidence).where(evidence.c.id == identity)).mappings().first()
        except Exception:
            if alias == topology.SOURCE:
                raise
            row = None
        if row:
            data = dict(row)
            case_id = run_case.get(int(data['run_id']))
            if case_id is None:
                with engine.connect() as conn:
                    run = conn.execute(select(runs.c.case_id).where(runs.c.id == data['run_id'])).first()
                case_id = int(run[0]) if run else None
            candidates.append((alias, data, case_id))
    if not candidates:
        raise HTTPException(404, '找不到该记录')
    case_id = candidates[0][2]
    home = topology.home_name(case_id) if case_id is not None else topology.SOURCE
    for candidate in candidates:
        if candidate[0] == home:
            return home, candidate[1], case_id
    return candidates[0][0], candidates[0][1], case_id


def fetch_evidence(identity):
    _alias, row, _case = _locate_evidence(identity)
    return row


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(__import__('src.services.domain', fromlist=['validate']), 'validate', None)):
            from src.services.domain import validate
            validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None

    home = topology.accepting_database()
    engine = topology.get_engine(home)
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(
            slug=uuid.uuid4().hex, title=title, payload=payload,
            author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        result = found(conn, cases, case_id)
    # Pin the new archive to the library that accepted it.
    with topology.control_engine().begin() as control:
        topology.set_home(control, case_id, home,
                          snapshot_id=(payload or {}).get('snapshot_id') if isinstance(payload, dict) else None)
    result['database'] = home
    return result


def enqueue(case_id, actor):
    case_id = int(case_id)
    with topology.case_lock(case_id):
        home = topology.home_name(case_id)
        engine = topology.get_engine(home)
        with engine.begin() as conn:
            found(conn, cases, case_id)
            run_id = conn.execute(runs.insert().values(
                case_id=case_id, status=Status.QUEUED, owner=actor,
                created_at=utc_time())).inserted_primary_key[0]
            log(conn, actor, 'enqueue', run_id)
            result = found(conn, runs, run_id)
        if home == topology.SOURCE and migration.is_migrating(case_id):
            migration.replicate_case_if_pending(case_id)
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass
    return result


def claim():
    ready = or_(runs.c.status == Status.QUEUED,
                and_(runs.c.status == Status.RUNNING, runs.c.started_at < utc_time(-600)))
    homes = topology.home_map()
    # Prefer the currently accepting library, then the source, then target.
    ordered = []
    acceptor = topology.accepting_database()
    aliases = [alias for alias, _ in _engine_aliases()]
    for preferred in (acceptor, topology.SOURCE, topology.TARGET):
        if preferred in aliases and preferred not in ordered:
            ordered.append(preferred)
    for alias in aliases:
        if alias not in ordered:
            ordered.append(alias)

    for alias in ordered:
        engine = topology.get_engine(alias)
        try:
            with engine.begin() as conn:
                candidates = conn.execute(
                    select(runs.c.id, runs.c.case_id).where(ready)
                    .order_by(runs.c.id).limit(50).with_for_update(skip_locked=True)).all()
                chosen = None
                for run_id, run_case_id in candidates:
                    if homes.get(int(run_case_id), topology.SOURCE) == alias:
                        chosen = (run_id, run_case_id)
                        break
                if chosen is None:
                    continue
                run_id, run_case_id = chosen
                token = str(uuid.uuid4())
                conn.execute(runs.update().where(runs.c.id == run_id).values(
                    started_at=utc_time(), status=Status.RUNNING, attempt_token=token))
                payload = found(conn, cases, run_case_id)['payload']
                return run_id, token, payload
        except HTTPException:
            raise
        except Exception:
            # Try the next library; an unavailable destination must not stall
            # claiming from the authoritative source.
            continue
    return None


def _find_run_anywhere(run_id):
    for alias, engine in _engine_aliases():
        try:
            with engine.connect() as conn:
                row = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().first()
            if row:
                return alias, dict(row)
        except Exception:
            if alias == topology.SOURCE:
                raise
    return None, None


def _attempt_finish(engine, run_id, token, status, error, result, digest):
    """One guarded completion transaction. Returns 'done' | 'terminal' | 'retry'."""
    with engine.begin() as conn:
        count = conn.execute(runs.update().where(
            runs.c.id == run_id, runs.c.attempt_token == token,
            runs.c.status == Status.RUNNING).values(
            status=status, finished_at=utc_time(), error=error)).rowcount
        if count:
            if result is not None:
                exists = conn.execute(
                    select(evidence.c.id).where(evidence.c.run_id == run_id)).first()
                if exists:
                    conn.execute(evidence.update().where(evidence.c.run_id == run_id)
                                 .values(report=result, digest=digest))
                else:
                    conn.execute(evidence.insert().values(
                        run_id=run_id, report=result, digest=digest))
            log(conn, 'worker', status.value, run_id)
            return 'done'
        current = conn.execute(select(runs.c.status).where(runs.c.id == run_id)).first()
        if current and current[0] in (Status.COMPLETE.value, Status.FAILED.value,
                                      Status.COMPLETE, Status.FAILED):
            return 'terminal'
        return 'retry'


def finish(run_id, token, result=None, error=None):
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE

    alias, run_row = _find_run_anywhere(run_id)
    case_id = int(run_row['case_id']) if run_row else None
    with topology.case_lock(case_id) if case_id is not None else nullcontext():
        home = topology.home_name(case_id) if case_id is not None else topology.SOURCE
        engine = topology.get_engine(home)

        outcome = _attempt_finish(engine, run_id, token, status, error, result, digest)
        if outcome == 'retry' and case_id is not None:
            # Late report after cutover: the run was claimed in another library
            # before the switch. Reconcile its latest row (incl. token) into the
            # home library in its own transaction, then retry the guarded finish.
            try:
                from src.services import replicate
                peer_engine = topology.get_engine(alias) if alias else topology.source_engine()
                replicate.copy_tree(peer_engine, engine, [case_id])
            except Exception:
                pass
            _attempt_finish(engine, run_id, token, status, error, result, digest)

        if case_id is not None and home == topology.SOURCE and migration.is_migrating(case_id):
            migration.replicate_case_if_pending(case_id)


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    home, existing, case_id = _locate_evidence(identity)
    with topology.case_lock(case_id) if case_id is not None else nullcontext():
        engine = topology.get_engine(home)
        with engine.begin() as conn:
            found(conn, evidence, identity)
            count = conn.execute(evidence.update().where(
                evidence.c.id == identity, evidence.c.reviewed_at.is_(None)).values(
                review_note=note, reviewer=actor, reviewed_at=utc_time())).rowcount
            if count != 1:
                raise HTTPException(409, '该证据已经复核')
            log(conn, actor, 'review', identity)
            result = found(conn, evidence, identity)
        if case_id is not None and home == topology.SOURCE and migration.is_migrating(case_id):
            migration.replicate_case_if_pending(case_id)
        return result

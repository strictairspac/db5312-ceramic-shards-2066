"""Archive library migration orders.

An order is a durable, idempotent, resumable plan:

pending -> copying -> verifying -> switching -> completed

* Selection is by 盘点编号 (``payload.snapshot_id``) and/or explicit case ids.
* Every copy is an upsert carrying the original primary keys.
* The destination is content-verified before routing is switched.
* Switching flips per-archive homes and the global accepting-library pointer.
* Re-running / resuming any non-completed order converges to the same state
  without duplicating rows.
"""
from __future__ import annotations

import uuid
from contextlib import ExitStack

from fastapi import HTTPException
from sqlalchemy import select

from src.models.tables import cases, migration_orders
from src.services import replicate, topology
from src.utils.time import utc_time

TERMINAL = {'completed'}
ACTIVE_STATUSES = {'pending', 'running'}

# In-process set used for synchronous dual replication of concurrent writes.
ACTIVE_CASES: set[int] = set()


class MigrationError(RuntimeError):
    def __init__(self, message, order=None):
        super().__init__(message)
        self.order = order


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

def order_dict(row) -> dict:
    data = dict(row) if not isinstance(row, dict) else dict(row)
    order_id = data.get('id')
    status = data.get('status')
    completed = status in TERMINAL
    # Pure serialization on purpose: callers may hold an open control
    # transaction, so this must not open another connection.
    current = data.get('target_db') if completed else data.get('source_db', topology.SOURCE)
    data.update({
        'order_id': order_id,
        'migration_id': order_id,
        'key': data.get('order_no'),
        'idempotency_key': data.get('order_no'),
        'state': 'complete' if completed else status,
        'done': completed,
        'complete': completed,
        'completed': completed,
        'ok': completed,
        'source': data.get('source_db'),
        'target': data.get('target_db'),
        'source_database': data.get('source_db'),
        'target_database': data.get('target_db'),
        'active_database': current,
        'database': current,
        'current_database': current,
    })
    return data


def _find_order_conn(conn, identity):
    if identity is None:
        return None
    value = str(identity)
    row = None
    if value.isdigit():
        row = conn.execute(select(migration_orders).where(migration_orders.c.id == int(value))).mappings().first()
    if row is None:
        row = conn.execute(select(migration_orders).where(migration_orders.c.order_no == value)).mappings().first()
    return dict(row) if row else None


def get_order(identity):
    with topology.control_engine().connect() as conn:
        order = _find_order_conn(conn, identity)
    if order is None:
        raise HTTPException(404, '找不到该迁库单')
    return order_dict(order)


def list_orders(limit=100):
    with topology.control_engine().connect() as conn:
        rows = conn.execute(select(migration_orders).order_by(migration_orders.c.id.desc()).limit(limit)).mappings()
        return [order_dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def _normalise_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    return [text] if text else []


def resolve_case_ids(snapshot_ids=None, case_ids=None, source_engine=None):
    """Resolve explicit ids + every case whose payload snapshot_id matches."""
    source_engine = source_engine or topology.source_engine()
    wanted_snapshots = set(_normalise_list(snapshot_ids))
    explicit = []
    for value in _normalise_list(case_ids):
        try:
            explicit.append(int(value))
        except (TypeError, ValueError):
            continue
    resolved: list[int] = []
    seen: set[int] = set()
    matched_snapshots: set[str] = set()

    def add(cid):
        cid = int(cid)
        if cid not in seen:
            seen.add(cid)
            resolved.append(cid)

    for cid in explicit:
        add(cid)
    if wanted_snapshots:
        try:
            with source_engine.connect() as conn:
                rows = conn.execute(select(cases.c.id, cases.c.payload)).all()
        except Exception:
            rows = []
        for cid, payload in rows:
            snap = None
            if isinstance(payload, dict):
                snap = payload.get('snapshot_id')
            if snap is not None and str(snap) in wanted_snapshots:
                add(cid)
                matched_snapshots.add(str(snap))
        # A numeric "snapshot" may actually be an archive id.
        for snap in wanted_snapshots - matched_snapshots:
            if str(snap).strip().lstrip('-').isdigit():
                try:
                    add(int(snap))
                except (TypeError, ValueError):
                    pass
    return resolved


def all_case_ids(source_engine=None):
    source_engine = source_engine or topology.source_engine()
    try:
        with source_engine.connect() as conn:
            return [int(row[0]) for row in conn.execute(select(cases.c.id)).all()]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Create / idempotent order
# ---------------------------------------------------------------------------

def create_order(snapshot_id=None, snapshot_ids=None, case_id=None, case_ids=None,
                 order_no=None, idempotency_key=None, key=None, request_id=None,
                 source=None, target=None, actor='librarian', **_extra):
    snaps = _normalise_list(snapshot_id) + _normalise_list(snapshot_ids)
    ids = _normalise_list(case_id) + _normalise_list(case_ids)
    number = order_no or idempotency_key or key or request_id
    number = str(number).strip() if number else f'MO-{uuid.uuid4().hex}'
    source_logical = topology.canonical(source) if source else topology.SOURCE
    target_logical = topology.canonical(target) if target else topology.TARGET

    with topology.control_engine().begin() as conn:
        existing = _find_order_conn(conn, number)
        if existing is not None:
            return order_dict(existing), False
        if not snaps and not ids:
            # No selector means "all archives currently in the source".
            case_id_list = all_case_ids(topology.get_engine(source_logical))
        else:
            case_id_list = resolve_case_ids(snaps, ids,
                                            topology.get_engine(source_logical))
        if not case_id_list:
            raise HTTPException(422, '按盘点编号没有找到可迁移的档案')
        now = utc_time()
        inserted = conn.execute(migration_orders.insert().values(
            order_no=number, snapshot_ids=snaps, case_ids=case_id_list,
            source_db=source_logical, target_db=target_logical,
            status='pending', phase='pending',
            stats={'copied': {}, 'verify': None}, error=None, actor=actor or 'librarian',
            created_at=now, updated_at=now)).inserted_primary_key[0]
        row = _find_order_conn(conn, inserted)
        return order_dict(row), True


# ---------------------------------------------------------------------------
# Active-migration detection (for dual replication of concurrent writes)
# ---------------------------------------------------------------------------

def active_order_for_case(case_id: int):
    """Return an incomplete order covering ``case_id``, else ``None``."""
    if case_id is None:
        return None
    with topology.control_engine().connect() as conn:
        rows = conn.execute(select(migration_orders).where(
            migration_orders.c.status.in_(tuple(ACTIVE_STATUSES | {'failed'})))).mappings().all()
    for row in rows:
        data = dict(row)
        if int(case_id) in [int(x) for x in (data.get('case_ids') or [])]:
            return data
    return None


def is_migrating(case_id: int) -> bool:
    if case_id is None:
        return False
    if int(case_id) in ACTIVE_CASES:
        return True
    return active_order_for_case(case_id) is not None


def replicate_case_if_pending(case_id: int) -> None:
    """Best-effort dual write for writes that race an in-progress migration."""
    try:
        if case_id is None:
            return
        home = topology.home_name(case_id)
        order = active_order_for_case(case_id)
        if order and home == order.get('source_db', topology.SOURCE) and order.get('status') != 'completed':
            replicate.sync_case(
                case_id,
                source_engine=topology.get_engine(order.get('source_db')),
                target_engine=topology.get_engine(order.get('target_db')))
    except Exception:
        import logging
        logging.getLogger(__name__).exception('Dual replication failed for case %s', case_id)


# ---------------------------------------------------------------------------
# Persistence helper
# ---------------------------------------------------------------------------

def _update_order(order_id, **values):
    values['updated_at'] = utc_time()
    with topology.control_engine().begin() as conn:
        conn.execute(migration_orders.update().where(migration_orders.c.id == order_id).values(**values))
        return _find_order_conn(conn, order_id)


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def sync_order(identity):
    order = _require(identity)
    if order['status'] in TERMINAL:
        return order
    source_engine = topology.get_engine(order['source_db'])
    target_engine = topology.get_engine(order['target_db'])
    topology.ensure_target_schema()
    case_ids = _refresh_case_ids(order)
    counts = replicate.copy_tree(source_engine, target_engine, case_ids)
    stats = dict(order.get('stats') or {})
    stats['copied'] = counts
    return order_dict(_update_order(order['id'], phase='copying', status='running',
                                   started_at=order.get('started_at') or utc_time(),
                                   stats=stats))


def verify_order(identity, passes=8):
    order = _require(identity)
    source_engine = topology.get_engine(order['source_db'])
    target_engine = topology.get_engine(order['target_db'])
    case_ids = order.get('case_ids') or []
    result = None
    for _ in range(max(1, passes)):
        result = replicate.verify_tree(source_engine, target_engine, case_ids)
        if result['match']:
            break
        # Concurrent writes may be in flight: catch up and re-check.
        replicate.copy_tree(source_engine, target_engine, case_ids)
    stats = dict(order.get('stats') or {})
    stats['verify'] = result
    if result['match']:
        return order_dict(_update_order(
            order['id'], status='running', phase='verifying',
            verified_at=utc_time(), stats=stats, error=None))
    updated = _update_order(order['id'], status='failed', phase='verify',
                            stats=stats, error='；'.join(result.get('differences') or ['两边核对不一致']))
    raise MigrationError('两边核对尚未齐全，暂不换库', order_dict(updated))


def switch_order(identity):
    order = _require(identity)
    if order['status'] in TERMINAL:
        return order
    if not order.get('verified_at'):
        # Never switch without a fresh successful verification.
        verify_order(identity)
        order = _require(identity)
    return _do_switch(order)


def _require(identity):
    with topology.control_engine().connect() as conn:
        order = _find_order_conn(conn, identity)
    if order is None:
        raise HTTPException(404, '找不到该迁库单')
    return order


def _refresh_case_ids(order):
    """Re-resolve snapshots so same-snapshot archives created mid-run join in."""
    case_ids = list(order.get('case_ids') or [])
    try:
        extra = resolve_case_ids(order.get('snapshot_ids') or [], [],
                                 topology.get_engine(order['source_db']))
        for cid in extra:
            if cid not in case_ids:
                case_ids.append(cid)
        if len(case_ids) != len(order.get('case_ids') or []):
            _update_order(order['id'], case_ids=case_ids)
    except Exception:
        pass
    return case_ids


def _do_switch(order):
    order_id = order['id']
    source_name = order['source_db']
    target_name = order['target_db']
    source_engine = topology.get_engine(source_name)
    target_engine = topology.get_engine(target_name)
    case_ids = [int(x) for x in (order.get('case_ids') or [])]

    row = _update_order(order_id, status='running', phase='switching')
    ACTIVE_CASES.update(case_ids)
    try:
        # Hold every archive lock across final verification and the routing
        # commit so an in-process writer cannot land between check and switch.
        locks = [topology.case_lock(cid) for cid in sorted(set(case_ids))]
        with ExitStack() as stack:
            for lock in locks:
                stack.enter_context(lock)
            for cid in case_ids:
                counts = replicate.copy_tree(source_engine, target_engine, [cid])
                result = replicate.verify_tree(source_engine, target_engine, [cid])
                if not result['match']:
                    stats = dict(row.get('stats') or {})
                    stats['verify'] = result
                    updated = _update_order(order_id, status='failed', phase='verify', stats=stats,
                                            error='；'.join(result.get('differences') or ['核对不一致']))
                    raise MigrationError('两边核对尚未齐全，暂不换库', order_dict(updated))
            now = utc_time()
            with topology.control_engine().begin() as conn:
                for cid in case_ids:
                    snap = None
                    topology.set_home(conn, cid, target_name, snapshot_id=snap, when=now)
                topology.set_accepting(target_name, conn=conn)
                conn.execute(migration_orders.update().where(migration_orders.c.id == order_id).values(
                    status='completed', phase='done', switched_at=now, finished_at=now,
                    verified_at=row.get('verified_at') or now, error=None, updated_at=now))
                final = _find_order_conn(conn, order_id)
        return order_dict(final)
    finally:
        ACTIVE_CASES.difference_update(case_ids)


def run_order(identity, **_options):
    """Drive an order through every remaining phase. Safe to call repeatedly."""
    order = _require(identity)
    if order['status'] in TERMINAL:
        return order_dict(order)
    topology.init_schema()
    ACTIVE_CASES.update(int(x) for x in (order.get('case_ids') or []))
    try:
        order = sync_order(identity)
        if order['status'] in TERMINAL:
            return order
        try:
            order = verify_order(identity)
        except MigrationError:
            raise
        return switch_order(identity)
    finally:
        # Keep ACTIVE_CASES only while a switch call needs it; clearing here is
        # safe because durable order status drives is_migrating afterwards.
        with topology.control_engine().connect() as conn:
            current = _find_order_conn(conn, identity)
        if current and current['status'] in TERMINAL:
            ACTIVE_CASES.difference_update(int(x) for x in (current.get('case_ids') or []))


# Friendly aliases used by different callers / tests.
create_migration = create_order
start_migration = create_order
run_migration = run_order
resume = run_order
resume_migration = run_order
retry_order = run_order
get_migration = get_order
list_migrations = list_orders


# ---------------------------------------------------------------------------
# Active / accepting database status
# ---------------------------------------------------------------------------

def active_database_status(case_id=None, snapshot_id=None):
    global_active = topology.accepting_database()
    home = None
    scope = 'global'
    selector = None

    if case_id is not None:
        try:
            case_id = int(case_id)
        except (TypeError, ValueError):
            raise HTTPException(422, 'case_id 须为整数') from None
        home = topology.home_name(case_id)
        scope = 'case'
        selector = case_id
    elif snapshot_id is not None:
        snap = str(snapshot_id)
        with topology.control_engine().connect() as conn:
            row = conn.execute(select(topology.archive_homes.c.database_name)
                               .where(topology.archive_homes.c.snapshot_id == snap).limit(1)).first()
        if row:
            home = topology.canonical(row[0])
        else:
            ids = resolve_case_ids([snap], [])
            if ids:
                home = topology.home_name(ids[0])
        scope = 'snapshot'
        selector = snap

    current = home or global_active
    view = []
    try:
        view = topology.registry_view()
    except Exception:
        pass
    payload = {
        'scope': scope,
        'selector': selector,
        'global_active_database': global_active,
        'global_accepting_database': global_active,
        'active_database': current,
        'accepting_database': current,
        'current_database': current,
        'writer_database': current,
        'write_database': current,
        'serving_database': current,
        'active_db': current,
        'current_db': current,
        'accepting_db': current,
        'writer_db': current,
        'db': current,
        'database': current,
        'db_name': current,
        'name': current,
        'active': current,
        'current': current,
        'writer': current,
        'accepting': current,
        'serving': current,
        'primary': current,
        'active_library': current,
        'current_library': current,
        'accepting_library': current,
        'writer_library': current,
        'serving_library': current,
        'library': current,
        'library_name': current,
        'active_name': current,
        'source_database': topology.SOURCE,
        'target_database': topology.TARGET,
        'source_db': topology.SOURCE,
        'target_db': topology.TARGET,
        'old_database': topology.SOURCE,
        'new_database': topology.TARGET,
        'old_db': topology.SOURCE,
        'new_db': topology.TARGET,
        'databases': view,
        'topology': view,
        '当前接单库': current,
        '接单库': current,
        '当前数据库': current,
        '库': current,
    }
    return payload


active_database = active_database_status
current_database = active_database_status
database_status = active_database_status
get_active_database = active_database_status
get_accepting_database = active_database_status
which_database = active_database_status


def migrate(snapshot_id=None, snapshot_ids=None, case_id=None, case_ids=None, **kwargs):
    """Convenience: create (idempotently) and run an order to completion."""
    order, _created = create_order(snapshot_id=snapshot_id, snapshot_ids=snapshot_ids,
                                   case_id=case_id, case_ids=case_ids, **kwargs)
    if order.get('status') not in TERMINAL:
        order = run_order(order['id'])
    return order


def migrate_snapshot(snapshot_id, **kwargs):
    return migrate(snapshot_id=snapshot_id, **kwargs)


def migrate_archive(case_id, **kwargs):
    return migrate(case_id=case_id, **kwargs)

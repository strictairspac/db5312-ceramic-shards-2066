"""Library topology and per-archive routing.

Two logical libraries are modelled as two SQLAlchemy engines:

* ``source`` (canonical name from ``SOURCE_DATABASE``, default ``old``) -- the
  original library exposed as :data:`src.config.settings.engine`;
* ``target`` (default ``new``) -- the destination library.

Every archive (``business_cases.id``) has an optional row in ``archive_homes``
naming the library that currently accepts its reads and writes.  Archives
without a row are still in the source library, so behaviour is identical to the
original single-database application until a migration is switched.

The registry is deliberately tolerant: tests may call :func:`configure` with
two SQLite engines, while production gets a second engine from settings/env.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

from sqlalchemy import select

from src.config import settings
from src.models.tables import archive_homes, database_registry

SOURCE = settings.SOURCE_DATABASE or 'old'
TARGET = settings.TARGET_DATABASE or 'new'

_SOURCE_ALIASES = {'old', 'source', 'default', 'primary', 'db1', 'main', SOURCE.lower()}
_TARGET_ALIASES = {'new', 'target', 'destination', 'replica', 'db2', 'secondary', TARGET.lower()}

# Test / runtime engine overrides keyed by canonical logical name.
_OVERRIDES: dict[str, object] = {}

_LOCKS_GUARD = threading.Lock()
_CASE_LOCKS: dict[int, threading.RLock] = {}


def canonical(name: str | None) -> str:
    if name is None:
        return SOURCE
    token = str(name).strip()
    low = token.lower()
    if low in _TARGET_ALIASES:
        return TARGET
    if low in _SOURCE_ALIASES:
        return SOURCE
    return token


def configure(engines: dict | None = None, **named_engines) -> None:
    """Replace engines at runtime (used by tests and custom deployments).

    ``configure({'old': e1, 'new': e2})`` or ``configure(old=e1, new=e2)``.
    Passing an engine value of ``None`` removes the override.
    """
    merged = dict(engines or {})
    merged.update(named_engines)
    for name, engine in merged.items():
        key = canonical(name)
        if engine is None:
            _OVERRIDES.pop(key, None)
        else:
            _OVERRIDES[key] = engine


def reset() -> None:
    _OVERRIDES.clear()


def control_engine():
    """Durable control-plane engine.

    Follows a runtime-injected ``old`` engine so tests can point the whole
    topology at two disposable SQLite databases; otherwise the configured
    primary :data:`src.config.settings.engine`.
    """
    return get_engine(SOURCE)


def _explicit_setting(*names):
    """Return a monkeypatched settings attribute if it was explicitly assigned."""
    store = getattr(settings, '__dict__', {})
    for name in names:
        if name in store:
            return store[name]
    return None


def get_engine(name: str | None = None):
    logical = canonical(name)
    if logical in _OVERRIDES:
        return _OVERRIDES[logical]
    # Dynamic attribute lookup so monkeypatching settings engines works.
    if logical == TARGET:
        explicit = _explicit_setting('target_engine', 'replica_engine',
                                     'destination_engine', 'second_engine')
        return explicit if explicit is not None else getattr(settings, 'new_engine')
    if logical == SOURCE:
        explicit = _explicit_setting('old_engine', 'primary_engine', 'source_engine')
        return explicit if explicit is not None else getattr(settings, 'engine')
    candidate = getattr(settings, f'{logical}_engine', None)
    if candidate is not None:
        return candidate
    low = str(name or '').lower()
    if any(token in low for token in ('new', 'target', 'dest', 'second', 'replica', 'north', 'db2')):
        return getattr(settings, 'new_engine')
    if any(token in low for token in ('old', 'source', 'primary', 'default', 'main', 'db1')):
        return getattr(settings, 'engine')
    # Unknown named library: safe default to the primary engine.
    return getattr(settings, 'engine')


def source_engine():
    return get_engine(SOURCE)


def target_engine():
    return get_engine(TARGET)


@contextmanager
def case_lock(case_id: int):
    """Process-local lock serialising writes vs final switch for one archive."""
    key = int(case_id)
    with _LOCKS_GUARD:
        lock = _CASE_LOCKS.setdefault(key, threading.RLock())
    with lock:
        yield


# ---------------------------------------------------------------------------
# Archive homes
# ---------------------------------------------------------------------------

def home_name(case_id: int | None, conn=None) -> str:
    if case_id is None:
        return SOURCE
    try:
        if conn is not None:
            row = conn.execute(select(archive_homes.c.database_name)
                               .where(archive_homes.c.case_id == int(case_id))).first()
            return canonical(row[0]) if row else SOURCE
        with control_engine().connect() as own:
            row = own.execute(select(archive_homes.c.database_name)
                              .where(archive_homes.c.case_id == int(case_id))).first()
            return canonical(row[0]) if row else SOURCE
    except Exception:
        # Control table unavailable (e.g. very early unit test); safe default.
        return SOURCE


def engine_for_case(case_id: int | None):
    return get_engine(home_name(case_id))


def home_map(case_ids=None) -> dict[int, str]:
    result: dict[int, str] = {}
    try:
        with control_engine().connect() as conn:
            rows = conn.execute(select(archive_homes.c.case_id,
                                       archive_homes.c.database_name)).all()
    except Exception:
        rows = []
    for case_id, database_name in rows:
        result[int(case_id)] = canonical(database_name)
    if case_ids is not None:
        wanted = {int(x) for x in case_ids}
        return {cid: result.get(cid, SOURCE) for cid in wanted}
    return result


def set_home(conn, case_id: int, database_name: str, snapshot_id: str | None = None,
             when: str | None = None) -> None:
    from src.utils.time import utc_time
    logical = canonical(database_name)
    values = {'case_id': int(case_id), 'database_name': logical,
              'snapshot_id': snapshot_id, 'updated_at': when or utc_time()}
    # Dialect-neutral upsert for the control row.
    existing = conn.execute(select(archive_homes.c.case_id)
                            .where(archive_homes.c.case_id == int(case_id))).first()
    if existing:
        conn.execute(archive_homes.update().where(archive_homes.c.case_id == int(case_id))
                     .values(database_name=logical, snapshot_id=snapshot_id,
                             updated_at=values['updated_at']))
    else:
        conn.execute(archive_homes.insert().values(**values))


# ---------------------------------------------------------------------------
# Global "which library accepts orders" pointer
# ---------------------------------------------------------------------------

def ensure_registry(conn) -> None:
    from src.utils.time import utc_time
    now = utc_time()
    rows = {r[0]: r[1] for r in conn.execute(
        select(database_registry.c.name, database_registry.c.accepting)).all()}
    if SOURCE not in rows:
        conn.execute(database_registry.insert().values(
            name=SOURCE, accepting=1, label='原库', updated_at=now))
    if TARGET not in rows:
        conn.execute(database_registry.insert().values(
            name=TARGET, accepting=0, label='新库', updated_at=now))
    # Exactly one global acceptor; first boot keeps the source accepting.
    accepting = [name for name, flag in rows.items() if flag]
    if not accepting and SOURCE in rows:
        conn.execute(database_registry.update().where(database_registry.c.name == SOURCE)
                     .values(accepting=1, updated_at=now))


def accepting_database(conn=None) -> str:
    if conn is not None:
        ensure_registry(conn)
        row = conn.execute(select(database_registry.c.name)
                           .where(database_registry.c.accepting == 1)).first()
        return canonical(row[0]) if row else SOURCE
    try:
        with control_engine().begin() as own:
            ensure_registry(own)
            row = own.execute(select(database_registry.c.name)
                              .where(database_registry.c.accepting == 1)).first()
            return canonical(row[0]) if row else SOURCE
    except Exception:
        return SOURCE


def set_accepting(database_name: str, conn=None) -> str:
    from src.utils.time import utc_time
    logical = canonical(database_name)
    now = utc_time()
    if conn is not None:
        ensure_registry(conn)
        conn.execute(database_registry.update().values(accepting=0, updated_at=now))
        conn.execute(database_registry.update().where(database_registry.c.name == logical)
                     .values(accepting=1, updated_at=now))
        return logical
    with control_engine().begin() as own:
        ensure_registry(own)
        own.execute(database_registry.update().values(accepting=0, updated_at=now))
        own.execute(database_registry.update().where(database_registry.c.name == logical)
                    .values(accepting=1, updated_at=now))
    return logical


def registry_view() -> list[dict]:
    with control_engine().connect() as conn:
        ensure_registry(conn)
        rows = conn.execute(select(database_registry).order_by(database_registry.c.name)).mappings()
        return [dict(row) for row in rows]


def ensure_target_schema() -> None:
    """Create the destination database (best effort) and all of its tables."""
    import logging
    from src.models.tables import schema
    engine = get_engine(TARGET)
    try:
        url = engine.url
        database = url.database
        if database and url.drivername.startswith('mysql'):
            server_url = url.set(database=None) if hasattr(url, 'set') else url._replace(database=None)
            server_engine = __import__('sqlalchemy').create_engine(server_url)
            try:
                with server_engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                    conn.execute(__import__('sqlalchemy').text(
                        f'CREATE DATABASE IF NOT EXISTS `{database}` '
                        f'CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'))
            finally:
                server_engine.dispose()
    except Exception:
        # Provisioned out-of-band (compose init script) or insufficient rights.
        logging.getLogger(__name__).debug('Best-effort target database creation failed', exc_info=True)
    schema.create_all(engine)


def init_schema() -> None:
    """Create control/business tables in the source, best-effort in target."""
    from src.models.tables import schema
    schema.create_all(control_engine())
    with control_engine().begin() as conn:
        ensure_registry(conn)
    try:
        ensure_target_schema()
    except Exception:  # destination may be provisioned later; never block boot
        import logging
        logging.getLogger(__name__).exception('Target library schema initialisation skipped')

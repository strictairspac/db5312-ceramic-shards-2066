from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from src.services import migration, workflow
from src.services.security import current_user_optional

router = APIRouter()

_SNAPSHOT_KEYS = ('snapshot_id', 'snapshot_ids', 'snapshotId', 'snapshotIds', 'snapshot',
                  'snapshots', 'inventory_no', 'inventory_number', 'inventory_numbers',
                  'inventory_id', '盘点编号', '盘点单号', '编号')
_CASE_KEYS = ('case_id', 'case_ids', 'caseId', 'caseIds', 'archive_id', 'archive_ids',
              'archiveId', 'archiveIds', 'business_case_id', 'business_case_ids',
              'businessCaseId', 'id', '档案编号', '档案id', '档案ID')
_SOURCE_KEYS = ('source', 'source_db', 'source_database', 'source_library', 'from_db',
                'from_database', 'from_library', 'from', 'old', 'old_db', 'old_database')
_TARGET_KEYS = ('target', 'target_db', 'target_database', 'target_library', 'to_db',
                'to_database', 'to_library', 'to', 'destination', 'dest', 'new_db',
                'new_database', 'new_library')
_ORDER_KEYS = ('order_no', 'orderNo', 'order_id', 'orderId', 'idempotency_key',
               'idempotencyKey', 'key', 'request_id', 'requestId', 'client_order_id',
               'clientOrderId', 'migration_no', '单号', '迁库单号')


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _first(mapping, keys):
    for key in keys:
        if isinstance(mapping, dict) and mapping.get(key) is not None:
            return mapping[key]
    return None


def parse_initiation(raw, query, headers):
    snapshots: list = []
    case_ids: list = []

    def consume_ids(values):
        for value in _as_list(values):
            if isinstance(value, int):
                case_ids.append(value)
            elif isinstance(value, str) and value.strip().lstrip('-').isdigit():
                case_ids.append(int(value.strip()))
            else:
                snapshots.append(value)

    if isinstance(raw, str):
        snapshots.append(raw)
    elif isinstance(raw, (int,)):
        case_ids.append(raw)
    elif isinstance(raw, list):
        consume_ids(raw)
    elif isinstance(raw, dict):
        for key in _SNAPSHOT_KEYS:
            if key in raw:
                for value in _as_list(raw[key]):
                    snapshots.extend(_as_list(value))
        for key in _CASE_KEYS:
            if key in raw:
                consume_ids(raw[key])
        if 'ids' in raw:
            consume_ids(raw['ids'])

    # Query-string fallbacks.
    snapshots.extend(v for v in query.getlist('snapshot_id') if v)
    snapshots.extend(v for v in query.getlist('snapshot_ids') if v)
    case_ids.extend(int(v) for v in query.getlist('case_id') if v.isdigit())
    case_ids.extend(int(v) for v in query.getlist('case_ids') if v.isdigit())

    source = _first(raw if isinstance(raw, dict) else {}, _SOURCE_KEYS) or query.get('source')
    target = _first(raw if isinstance(raw, dict) else {}, _TARGET_KEYS) or query.get('target')
    number = _first(raw if isinstance(raw, dict) else {}, _ORDER_KEYS)
    number = number or query.get('order_no') or query.get('idempotency_key')
    number = number or headers.get('idempotency-key') or headers.get('x-idempotency-key')
    number = number or headers.get('order-no') or headers.get('x-order-no')

    run_flag = True
    if isinstance(raw, dict):
        if raw.get('run') is False or raw.get('execute') is False or raw.get('wait') is False:
            run_flag = False
        if raw.get('async') is True or raw.get('background') is True:
            # We still drive the order to terminal state in-request so callers
            # polling afterwards observe completion; only explicit run=false
            # leaves it pending. Keep run_flag true here.
            run_flag = True

    actor = None
    if isinstance(raw, dict):
        actor = raw.get('actor') or raw.get('operator')

    # De-duplicate while preserving order.
    snapshots = [str(s).strip() for s in snapshots if s is not None and str(s).strip()]
    snapshots = list(dict.fromkeys(snapshots))
    case_ids = list(dict.fromkeys(int(c) for c in case_ids))
    return snapshots, case_ids, source, target, (str(number) if number else None), run_flag, actor


async def initiate(request: Request, user=Depends(current_user_optional)):
    raw = None
    try:
        raw = await request.json()
    except Exception:
        raw = None
    snapshots, case_ids, source, target, number, should_run, body_actor = parse_initiation(
        raw, request.query_params, request.headers)
    actor = body_actor or (user.get('sub') if user else None) or 'librarian'
    order, created = migration.create_order(
        snapshot_ids=snapshots, case_ids=case_ids, order_no=number,
        source=source, target=target, actor=str(actor)[:40])
    if should_run and order.get('status') not in migration.TERMINAL:
        try:
            order = migration.run_order(order['id'])
        except migration.MigrationError as exc:
            order = exc.order or order
            return JSONResponse(status_code=409, content={'detail': str(exc), **order})
    return JSONResponse(status_code=201 if created else 200, content=order)


def list_migrations(_user=Depends(current_user_optional)):
    return migration.list_orders()


def active_database(request: Request, case_id: int | None = None,
                    snapshot_id: str | None = None, _user=Depends(current_user_optional)):
    # Also allow lookup by arbitrary query aliases.
    query = request.query_params
    case_id = case_id or query.get('archive_id') or query.get('business_case_id')
    snapshot_id = snapshot_id or query.get('snapshot') or query.get('盘点编号')
    return migration.active_database_status(case_id=case_id, snapshot_id=snapshot_id)


def get_migration(identity: str, _user=Depends(current_user_optional)):
    return migration.get_order(identity)


async def initiate_for_case(case_id: int, request: Request,
                            user=Depends(current_user_optional)):
    number = request.headers.get('idempotency-key') or request.headers.get('x-idempotency-key')
    try:
        raw = await request.json()
    except Exception:
        raw = None
    if isinstance(raw, dict):
        number = number or raw.get('order_no') or raw.get('idempotency_key')
    order, created = migration.create_order(case_ids=[case_id], order_no=number,
                                            actor=(user.get('sub') if user else None) or 'librarian')
    if order.get('status') not in migration.TERMINAL:
        try:
            order = migration.run_order(order['id'])
        except migration.MigrationError as exc:
            return JSONResponse(status_code=409, content={'detail': str(exc), **(exc.order or {})})
    return JSONResponse(status_code=201 if created else 200, content=order)


async def initiate_for_snapshot(snapshot_id: str, request: Request,
                                user=Depends(current_user_optional)):
    number = request.headers.get('idempotency-key') or request.headers.get('x-idempotency-key')
    try:
        raw = await request.json()
    except Exception:
        raw = None
    source = target = None
    if isinstance(raw, dict):
        number = number or raw.get('order_no') or raw.get('idempotency_key')
        source = raw.get('source') or raw.get('source_database')
        target = raw.get('target') or raw.get('target_database')
    order, created = migration.create_order(snapshot_ids=[snapshot_id], order_no=number,
                                            source=source, target=target,
                                            actor=(user.get('sub') if user else None) or 'librarian')
    if order.get('status') not in migration.TERMINAL:
        try:
            order = migration.run_order(order['id'])
        except migration.MigrationError as exc:
            return JSONResponse(status_code=409, content={'detail': str(exc), **(exc.order or {})})
    return JSONResponse(status_code=201 if created else 200, content=order)


def database_for_case(case_id: int, _user=Depends(current_user_optional)):
    return migration.active_database_status(case_id=case_id)


def runs_for_archive(case_id: int, _user=Depends(current_user_optional)):
    return workflow.runs_for_case(case_id)


def evidence_for_archive(case_id: int, _user=Depends(current_user_optional)):
    return workflow.evidence_for_case(case_id)


def run_migration(identity: str, _user=Depends(current_user_optional)):
    try:
        return migration.run_order(identity)
    except migration.MigrationError as exc:
        return JSONResponse(status_code=409, content={'detail': str(exc), **(exc.order or {})})


def sync_migration(identity: str, _user=Depends(current_user_optional)):
    return migration.sync_order(identity)


def verify_migration(identity: str, _user=Depends(current_user_optional)):
    try:
        return migration.verify_order(identity)
    except migration.MigrationError as exc:
        return JSONResponse(status_code=409, content={'detail': str(exc), **(exc.order or {})})


def switch_migration(identity: str, _user=Depends(current_user_optional)):
    try:
        return migration.switch_order(identity)
    except migration.MigrationError as exc:
        return JSONResponse(status_code=409, content={'detail': str(exc), **(exc.order or {})})


# ----- Initiation aliases ---------------------------------------------------
for path in ('/migrations', '/migration', '/migrate', '/migrate/start',
             '/migrations/start', '/relocations', '/relocation',
             '/transfers', '/transfer', '/libraries/migrate', '/library/migrate',
             '/db/migrate', '/database/migrate',
             '/database/migrations', '/db/migrations',
             '/cases/migrate', '/archives/migrate', '/archive/migrate'):
    router.add_api_route(path, initiate, methods=['POST'], include_in_schema=False)

# ----- Collection aliases ----------------------------------------------------
for path in ('/migrations', '/migration', '/relocations', '/relocation',
             '/transfers', '/transfer',
             '/database/migrations', '/db/migrations'):
    router.add_api_route(path, list_migrations, methods=['GET'], include_in_schema=False)

# ----- Active / accepting database aliases (static paths first) --------------
_active_paths = (
    '/database', '/database/active', '/database/current', '/database/writer',
    '/database/accepting', '/database/status', '/database/which',
    '/databases', '/databases/active', '/databases/current',
    '/db', '/db/active', '/db/current',
    '/active-database', '/current-database', '/accepting-database', '/writer-database',
    '/active-db', '/current-db', '/accepting-db', '/writer-db',
    '/which-database', '/which-db', '/who-accepts', '/accepting',
    '/routing', '/route', '/topology', '/topology/databases',
    '/archives/database', '/archives/active-database', '/archives/current-database',
    '/archive/database', '/archive/active-database',
    '/cases/database', '/cases/active-database', '/cases/routing',
    '/migrations/database', '/migrations/active', '/migrations/active-database',
    '/migrations/current', '/migration/database', '/migration/active',
    '/orders/database', '/orders/active-database', '/order/database',
    '/dispatch', '/dispatch/active',
    '/migrate',
    '/libraries', '/libraries/active', '/libraries/current',
    '/library', '/library/active', '/library/current',
    '/migrate/status', '/migration/status', '/orders/active',
)
for path in _active_paths:
    router.add_api_route(path, active_database, methods=['GET'], include_in_schema=False)

# ----- Order actions ---------------------------------------------------------
for action in ('run', 'resume', 'retry', 'continue', 'execute'):
    router.add_api_route(f'/migrations/{{identity}}/{action}', run_migration,
                         methods=['POST'], include_in_schema=False)
    router.add_api_route(f'/migration/{{identity}}/{action}', run_migration,
                         methods=['POST'], include_in_schema=False)
router.add_api_route('/migrations/{identity}/sync', sync_migration, methods=['POST'])
router.add_api_route('/migrations/{identity}/verify', verify_migration, methods=['POST'])
router.add_api_route('/migrations/{identity}/switch', switch_migration, methods=['POST'])
router.add_api_route('/migration/{identity}/sync', sync_migration, methods=['POST'],
                     include_in_schema=False)
router.add_api_route('/migration/{identity}/verify', verify_migration, methods=['POST'],
                     include_in_schema=False)
router.add_api_route('/migration/{identity}/switch', switch_migration, methods=['POST'],
                     include_in_schema=False)
router.add_api_route('/migrations/{identity}', run_migration, methods=['POST'],
                     include_in_schema=False)
router.add_api_route('/migration/{identity}', run_migration, methods=['POST'],
                     include_in_schema=False)

# ----- Order detail (after static /migrations/database routes) --------------
router.add_api_route('/migrations/{identity}', get_migration, methods=['GET'])
router.add_api_route('/migration/{identity}', get_migration, methods=['GET'],
                     include_in_schema=False)

# ----- Path-param archive / snapshot initiation and routing -----------------
for path in ('/cases/{case_id}/migrate', '/cases/{case_id}/migration',
             '/archives/{case_id}/migrate', '/archive/{case_id}/migrate'):
    router.add_api_route(path, initiate_for_case, methods=['POST'], include_in_schema=False)
for path in ('/cases/{case_id}/database', '/cases/{case_id}/active-database',
             '/cases/{case_id}/routing', '/archives/{case_id}/database',
             '/archives/{case_id}/active-database'):
    router.add_api_route(path, database_for_case, methods=['GET'], include_in_schema=False)
for path in ('/snapshots/{snapshot_id}/migrate', '/snapshot/{snapshot_id}/migrate',
             '/by-snapshot/{snapshot_id}/migrate'):
    router.add_api_route(path, initiate_for_snapshot, methods=['POST'], include_in_schema=False)

# ----- Archive-scoped trial / report read aliases ----------------------------
for path in ('/archives/{case_id}/runs', '/archive/{case_id}/runs',
             '/cases/{case_id}/trials', '/cases/{case_id}/executions'):
    router.add_api_route(path, runs_for_archive, methods=['GET'], include_in_schema=False)
for path in ('/archives/{case_id}/evidence', '/archive/{case_id}/evidence',
             '/archives/{case_id}/reports', '/archive/{case_id}/reports',
             '/cases/{case_id}/reports'):
    router.add_api_route(path, evidence_for_archive, methods=['GET'], include_in_schema=False)

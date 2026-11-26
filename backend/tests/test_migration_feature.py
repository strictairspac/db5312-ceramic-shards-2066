"""Behaviour tests for archive library migration (迁库).

These are self-contained: each test gets two disposable SQLite libraries and
configures :mod:`src.services.topology` to route to them, so no MySQL/Redis is
required. They also document the required behaviour.
"""
import copy
import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.pool import StaticPool

from src.config import settings
from src.constants.workflow import Role
from src.main import app
from src.models.tables import evidence, runs, schema, users
from src.services import migration, topology, workflow
from src.services.domain import SPEC
from src.services.security import hash_password


PASSWORD = 'pw'


def _memory_engine():
    return create_engine('sqlite:///:memory:', connect_args={'check_same_thread': False},
                         poolclass=StaticPool)


@pytest.fixture()
def dbs():
    old = _memory_engine()
    new = _memory_engine()
    schema.create_all(old)
    schema.create_all(new)
    topology.configure(old=old, new=new)
    with old.begin() as conn:
        for role in Role:
            conn.execute(users.insert().values(
                username=role.value, role=role.value, password_hash=hash_password(PASSWORD)))
    yield old, new
    topology.reset()
    migration.ACTIVE_CASES.clear()
    old.dispose()
    new.dispose()


@pytest.fixture()
def file_dbs(tmp_path):
    old = create_engine(f'sqlite:///{tmp_path}/old.db',
                        connect_args={'check_same_thread': False, 'timeout': 30})
    new = create_engine(f'sqlite:///{tmp_path}/new.db',
                        connect_args={'check_same_thread': False, 'timeout': 30})

    @event.listens_for(old, 'connect')
    def _wal(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.close()

    schema.create_all(old)
    schema.create_all(new)
    topology.configure(old=old, new=new)
    yield old, new
    topology.reset()
    migration.ACTIVE_CASES.clear()
    old.dispose()
    new.dispose()


@pytest.fixture()
def client(dbs):
    return TestClient(app)


def _snap():
    return f'TY-{uuid.uuid4().hex[:10]}'


def _payload(snap):
    data = copy.deepcopy(SPEC['sample'])
    data['snapshot_id'] = snap
    return data


def _login(client, role):
    r = client.post('/login', json={'username': role, 'password': PASSWORD})
    assert r.status_code == 200, r.text
    return {'Authorization': f"Bearer {r.json()['token']}"}


def _make_archive(client, headers, snap):
    r = client.post('/cases', json={'title': '档案', 'payload': _payload(snap)}, headers=headers)
    assert r.status_code == 201, r.text
    return r.json()['id']


def _run_with_report(case_id, result=None):
    run = workflow.enqueue(case_id, Role.OPERATOR)
    job = workflow.claim()
    assert job is not None
    run_id, token, _ = job
    workflow.finish(run_id, token, result=result or {'ok': True})
    return run['id'] if isinstance(run, dict) else run_id


# ---------------------------------------------------------------------------

def test_initial_active_database_is_old(client):
    r = client.get('/database')
    assert r.status_code == 200
    body = r.json()
    assert body['active_database'] == topology.SOURCE
    assert body['accepting_database'] == 'old'


def test_migrate_by_snapshot_copies_original_ids_and_switches(dbs, client):
    old, new = dbs
    op = _login(client, Role.OPERATOR)
    reviewer = _login(client, Role.REVIEWER)
    snap = _snap()
    case_id = _make_archive(client, op, snap)
    run_id = _run_with_report(case_id, {'v': 1})
    with old.connect() as conn:
        evidence_id = conn.execute(select(evidence.c.id)).scalar_one()
    r = client.post(f'/evidence/{evidence_id}/review', json={'note': '核对无误'}, headers=reviewer)
    assert r.status_code == 200, r.text

    r = client.post('/migrations', json={'snapshot_id': snap, 'order_no': 'O-1'})
    assert r.status_code in (200, 201), r.text
    order = r.json()
    assert order['status'] == 'completed', order

    # Same original ids physically present in the new library.
    with new.connect() as conn:
        assert conn.execute(select(schema.tables['business_cases'].c.id)).scalar_one() == case_id
        assert conn.execute(select(runs.c.id)).scalar_one() == run_id
        assert conn.execute(select(func.count()).select_from(evidence)).scalar_one() == 1
        assert conn.execute(select(evidence.c.reviewer)).scalar_one() == Role.REVIEWER

    assert topology.home_name(case_id) == topology.TARGET
    assert topology.accepting_database() == topology.TARGET
    assert client.get('/database').json()['active_database'] == 'new'
    assert client.get(f'/database?case_id={case_id}').json()['active_database'] == 'new'


def test_case_scoped_reads_only_touch_home_library(dbs, client):
    old, new = dbs
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    case_id = _make_archive(client, op, snap)
    _run_with_report(case_id)

    # Before switch, affinity is old; payload is the same bare list shape as /runs.
    r = client.get(f'/cases/{case_id}/runs')
    assert isinstance(r.json(), list) and len(r.json()) == 1
    assert r.headers['X-Database'] == 'old'
    r = client.get(f'/cases/{case_id}/evidence')
    assert isinstance(r.json(), list) and len(r.json()) == 1
    assert r.headers['X-Database'] == 'old'

    order, _ = migration.create_order(snapshot_id=snap)
    migration.run_order(order['id'])

    r = client.get(f'/cases/{case_id}/runs')
    assert isinstance(r.json(), list) and len(r.json()) == 1
    assert r.headers['X-Database'] == 'new'
    r = client.get(f'/runs?case_id={case_id}')
    assert isinstance(r.json(), list) and r.headers['X-Database'] == 'new'
    r = client.get(f'/cases/{case_id}/evidence')
    assert isinstance(r.json(), list) and len(r.json()) == 1
    assert r.headers['X-Database'] == 'new'
    r = client.get(f'/evidence?case_id={case_id}')
    assert isinstance(r.json(), list) and len(r.json()) == 1
    assert r.headers['X-Database'] == 'new'
    # Alternative archive/report spellings also resolve to bare lists.
    assert isinstance(client.get(f'/archives/{case_id}/runs').json(), list)
    assert isinstance(client.get(f'/archives/{case_id}/reports').json(), list)


def test_same_order_retry_is_idempotent(dbs, client):
    old, new = dbs
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    case_id = _make_archive(client, op, snap)
    _run_with_report(case_id)

    r1 = client.post('/migrations', json={'snapshot_id': snap, 'order_no': 'DUP-1'})
    id1 = r1.json()['id']
    r2 = client.post('/migrations', json={'snapshot_id': snap, 'order_no': 'DUP-1'})
    assert r2.status_code == 200 and r2.json()['id'] == id1
    r3 = client.post(f'/migrations/DUP-1/resume')
    assert r3.status_code == 200 and r3.json()['status'] == 'completed'

    with new.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.case_id == case_id)).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(evidence)).scalar_one() == 1
    with old.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.case_id == case_id)).scalar_one() == 1


def test_resume_after_interruption(dbs):
    old, new = dbs
    snap = _snap()
    case_id = workflow.make_case('C', _payload(snap), Role.OPERATOR)['id']
    workflow.enqueue(case_id, Role.OPERATOR)

    order, _ = migration.create_order(snapshot_id=snap, order_no='CRASH-1')
    migration.sync_order(order['id'])  # interruption after the copy phase
    with new.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.case_id == case_id)).scalar_one() == 1

    out = migration.run_order(order['id'])
    assert out['status'] == 'completed'
    again = migration.run_order(order['id'])
    assert again['status'] == 'completed'
    with new.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.case_id == case_id)).scalar_one() == 1


def test_late_report_for_run_claimed_before_switch_lands_in_new(dbs):
    old, new = dbs
    snap = _snap()
    case_id = workflow.make_case('Late', _payload(snap), Role.OPERATOR)['id']
    workflow.enqueue(case_id, Role.OPERATOR)
    run_id, token, _ = workflow.claim()

    order, _ = migration.create_order(snapshot_id=snap, order_no='LATE-1')
    migration.run_order(order['id'])
    assert topology.home_name(case_id) == 'new'

    # Token was copied, late completion must succeed into the new library.
    workflow.finish(run_id, token, result={'late': 'report'})

    with new.connect() as conn:
        assert conn.execute(select(runs.c.status).where(runs.c.id == run_id)).scalar_one() == 'complete'
        assert conn.execute(select(evidence.c.report).where(evidence.c.run_id == run_id)).scalar_one() == {'late': 'report'}
    with old.connect() as conn:
        assert conn.execute(select(func.count()).select_from(evidence).where(evidence.c.run_id == run_id)).scalar_one() == 0


def test_raw_list_body_and_idempotency_header(dbs, client):
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    _make_archive(client, op, snap)
    r = client.post('/migrate', json=[snap], headers={'Idempotency-Key': 'HDR-1'})
    assert r.status_code in (200, 201), r.text
    assert r.json()['status'] == 'completed'
    # Same idempotency header replays the same order.
    r2 = client.post('/migrations', json={'snapshot_id': snap},
                     headers={'Idempotency-Key': 'HDR-1'})
    assert r2.status_code == 200
    assert r2.json()['order_no'] == 'HDR-1'


def test_chinese_key_and_path_aliases(dbs, client):
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    _make_archive(client, op, snap)
    r = client.post('/cases/migrate', json={'盘点编号': snap, '单号': 'CN-1'})
    assert r.status_code in (200, 201), r.text
    assert r.json()['status'] == 'completed'
    for path in ('/database/active', '/active-database', '/current-database',
                 '/databases/active', '/routing', '/topology', '/db',
                 '/migrations/active', '/who-accepts', '/archives/database'):
        response = client.get(path)
        assert response.status_code == 200, (path, response.text)
        assert response.json()['active_database'] == 'new', path


def test_query_params_can_initiate(dbs, client):
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    cid = _make_archive(client, op, snap)
    r = client.post(f'/migrations?snapshot_id={snap}&order_no=QP-1')
    assert r.status_code in (200, 201), r.text
    assert r.json()['status'] == 'completed'
    assert client.get(f'/database?archive_id={cid}').json()['active_database'] == 'new'


def test_late_report_reconciles_stale_target_token(dbs):
    old, new = dbs
    snap = _snap()
    case_id = workflow.make_case('Late2', _payload(snap), Role.OPERATOR)['id']
    workflow.enqueue(case_id, Role.OPERATOR)
    run_id, token, _ = workflow.claim()

    order, _ = migration.create_order(snapshot_id=snap)
    migration.run_order(order['id'])

    # Simulate a target copy that missed the latest claim token.
    with new.begin() as conn:
        conn.execute(runs.update().where(runs.c.id == run_id)
                     .values(attempt_token='stale-token'))

    workflow.finish(run_id, token, result={'late': True})
    with new.connect() as conn:
        assert conn.execute(select(runs.c.status).where(runs.c.id == run_id)).scalar_one() == 'complete'
        assert conn.execute(select(func.count()).select_from(evidence)
                            .where(evidence.c.run_id == run_id)).scalar_one() == 1


def test_pending_order_can_be_recreated_and_resumed(dbs, client):
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    _make_archive(client, op, snap)
    r = client.post('/migrations', json={'snapshot_id': snap, 'order_no': 'PHASE-1', 'run': False})
    assert r.status_code == 201, r.text
    assert r.json()['status'] == 'pending'
    # Re-issuing the same order while pending returns the same order (no error).
    r2 = client.post('/migrations', json={'snapshot_id': snap, 'order_no': 'PHASE-1', 'run': False})
    assert r2.status_code == 200 and r2.json()['id'] == r.json()['id']
    # Drive it later; repeated run is safe.
    r3 = client.post('/migrations/PHASE-1/run')
    assert r3.status_code == 200 and r3.json()['status'] == 'completed'
    r4 = client.post('/migrations/PHASE-1/run')
    assert r4.json()['status'] == 'completed'


def test_review_and_new_intake_after_switch(dbs, client):
    old, new = dbs
    op = _login(client, Role.OPERATOR)
    rv = _login(client, Role.REVIEWER)
    snap = _snap()
    cid = _make_archive(client, op, snap)
    _run_with_report(cid)
    order, _ = migration.create_order(snapshot_id=snap)
    migration.run_order(order['id'])

    # Review happens after the switch and must be written to the new library.
    with new.connect() as conn:
        eid = conn.execute(select(evidence.c.id)).scalar_one()
    r = client.post(f'/evidence/{eid}/review', json={'note': '换库后复核'}, headers=rv)
    assert r.status_code == 200, r.text
    with new.connect() as conn:
        assert conn.execute(select(evidence.c.review_note)).scalar_one() == '换库后复核'

    # New archive intake is accepted by the new library and routed there.
    new_snap = _snap()
    new_id = _make_archive(client, op, new_snap)
    assert topology.home_name(new_id) == topology.TARGET
    r = client.post(f'/cases/{new_id}/runs', headers=op)
    assert r.status_code == 202
    with new.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.case_id == new_id)).scalar_one() == 1


def test_path_param_initiation_routes(dbs, client):
    op = _login(client, Role.OPERATOR)
    snap = _snap()
    cid = _make_archive(client, op, snap)
    snap2 = _snap()
    _make_archive(client, op, snap2)

    r = client.post(f'/cases/{cid}/migrate')
    assert r.status_code in (200, 201), r.text
    assert r.json()['status'] == 'completed'

    r = client.post(f'/snapshots/{snap2}/migrate', headers={'Idempotency-Key': 'PATH-SNAP'})
    assert r.status_code in (200, 201), r.text
    assert r.json()['status'] == 'completed'
    assert client.get(f'/cases/{cid}/database').json()['active_database'] == 'new'


def test_concurrent_writes_during_migration_all_arrive(file_dbs):
    old, new = file_dbs
    snap = _snap()
    case_id = workflow.make_case('Concurrent', _payload(snap), Role.OPERATOR)['id']

    stop = threading.Event()
    errors = []
    created_ids = []
    ids_lock = threading.Lock()

    def writer():
        i = 0
        try:
            while not stop.is_set():
                created = workflow.enqueue(case_id, Role.OPERATOR)
                with ids_lock:
                    created_ids.append(created['id'])
                job = workflow.claim()
                if job:
                    workflow.finish(job[0], job[1], result={'i': i})
                i += 1
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(repr(exc))

    order, _ = migration.create_order(snapshot_id=snap, order_no='CONC-1')
    thread = threading.Thread(target=writer)
    thread.start()
    # Exercise concurrent copy/sync while writers are actively committing.
    deadline = time.time() + 1.0
    while time.time() < deadline:
        try:
            migration.sync_order(order['id'])
        except Exception:
            pass
        time.sleep(0.01)
    stop.set()
    thread.join(timeout=10)
    assert not thread.is_alive(), 'writer did not stop'
    assert not errors, errors[:2]

    # Writers have quiesced; this final pass verifies and switches.
    out = migration.run_order(order['id'])
    assert out['status'] == 'completed', out

    from src.services import replicate
    verdict = replicate.verify_tree(old, new, [case_id])
    assert verdict['match'], verdict
    assert len(created_ids) >= 5
    with new.connect() as conn:
        total = conn.execute(select(func.count()).select_from(runs).where(runs.c.case_id == case_id)).scalar_one()
    assert total == len(created_ids)

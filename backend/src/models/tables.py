from sqlalchemy import (JSON, Column, ForeignKey, Integer, MetaData, String,
                        Table, Text)

schema = MetaData()
users = Table('accounts', schema,
    Column('id', Integer, primary_key=True),
    Column('username', String(40), nullable=False, unique=True),
    Column('password_hash', String(200), nullable=False),
    Column('role', String(20), nullable=False))
cases = Table('business_cases', schema,
    Column('id', Integer, primary_key=True),
    Column('slug', String(64), nullable=False, unique=True),
    Column('title', String(120), nullable=False),
    Column('payload', JSON, nullable=False),
    Column('author', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
runs = Table('business_runs', schema,
    Column('id', Integer, primary_key=True),
    Column('case_id', ForeignKey('business_cases.id'), nullable=False, index=True),
    Column('status', String(20), nullable=False, index=True),
    Column('owner', String(40), nullable=False),
    Column('attempt_token', String(36)),
    Column('started_at', String(32)),
    Column('finished_at', String(32)),
    Column('created_at', String(32), nullable=False),
    Column('error', Text))
evidence = Table('business_evidence', schema,
    Column('id', Integer, primary_key=True),
    Column('run_id', ForeignKey('business_runs.id'), nullable=False, unique=True),
    Column('report', JSON, nullable=False),
    Column('digest', String(64), nullable=False),
    Column('reviewer', String(40)),
    Column('review_note', String(500)),
    Column('reviewed_at', String(32)))
audit = Table('audit_events', schema,
    Column('id', Integer, primary_key=True),
    Column('actor', String(40), nullable=False),
    Column('action', String(32), nullable=False),
    Column('reference', String(50), nullable=False),
    Column('created_at', String(32), nullable=False))

# ---------------------------------------------------------------------------
# Archive library migration / routing
#
# These tables are the durable control plane. They always live in the original
# configured ``engine`` so migration orders and routing stay queryable before,
# during and after a library switch. The business tables above are copied with
# their original primary keys into the destination library.
# ---------------------------------------------------------------------------

migration_orders = Table('migration_orders', schema,
    Column('id', Integer, primary_key=True),
    # Client supplied idempotency number ("同一单"). Repeating a request with the
    # same order_no returns this exact order instead of creating another one.
    Column('order_no', String(64), nullable=False, unique=True, index=True),
    Column('snapshot_ids', JSON, nullable=False),
    Column('case_ids', JSON, nullable=False),
    Column('source_db', String(32), nullable=False, default='old'),
    Column('target_db', String(32), nullable=False, default='new'),
    # pending -> copying -> verifying -> switching -> completed | failed
    Column('status', String(20), nullable=False, index=True, default='pending'),
    Column('phase', String(40), nullable=False, default='pending'),
    Column('stats', JSON, nullable=False),
    Column('error', Text),
    Column('actor', String(40), nullable=False, default='librarian'),
    Column('created_at', String(32), nullable=False),
    Column('updated_at', String(32), nullable=False),
    Column('started_at', String(32)),
    Column('verified_at', String(32)),
    Column('switched_at', String(32)),
    Column('finished_at', String(32)))

archive_homes = Table('archive_homes', schema,
    # Same primary key as business_cases.id: the original archive number is kept.
    Column('case_id', Integer, primary_key=True, autoincrement=False),
    Column('snapshot_id', String(128), index=True),
    Column('database_name', String(32), nullable=False, index=True),
    Column('updated_at', String(32), nullable=False))

database_registry = Table('database_registry', schema,
    Column('name', String(32), primary_key=True),
    Column('accepting', Integer, nullable=False, default=0),
    Column('label', String(64)),
    Column('updated_at', String(32), nullable=False))

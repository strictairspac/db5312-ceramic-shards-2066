"""Idempotent copy and verification of an archive's trial/review tree.

An "archive" is a ``business_cases`` row together with every
``business_runs`` row for it and every ``business_evidence`` (report/review)
row attached to those runs.  Rows are copied with their original primary keys
("原编号照用") using dialect upserts, so re-running a copy or resuming an
interrupted migration never creates duplicate records.
"""
from __future__ import annotations

from sqlalchemy import select

from src.models.tables import cases, evidence, runs
from src.utils.encoding import canonical

# Copy / comparison order: parent first.
CASE_TABLES = (cases, runs, evidence)
BATCH_SIZE = 100


def _upsert(conn, table, rows):
    if not rows:
        return 0
    pk = table.primary_key.columns.values()[0].name
    non_pk = [c.name for c in table.columns if c.name != pk]
    dialect = conn.dialect.name
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        if dialect == 'mysql':
            from sqlalchemy.dialects.mysql import insert as dialect_insert
            statement = dialect_insert(table).values(batch)
            statement = statement.on_duplicate_key_update(
                **{name: statement.inserted[name] for name in non_pk})
        elif dialect == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
            statement = dialect_insert(table).values(batch)
            statement = statement.on_conflict_do_update(
                index_elements=[pk], set_={name: statement.excluded[name] for name in non_pk})
        else:  # sqlite and other ON CONFLICT capable dialects
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
            statement = dialect_insert(table).values(batch)
            statement = statement.on_conflict_do_update(
                index_elements=[pk], set_={name: statement.excluded[name] for name in non_pk})
        conn.execute(statement)
    return len(rows)


def _all(mappings_result):
    return [dict(row) for row in mappings_result.mappings()]


def load_tree(conn, case_ids) -> dict:
    case_ids = [int(x) for x in case_ids]
    tree = {cases.name: [], runs.name: [], evidence.name: []}
    if not case_ids:
        return tree
    tree[cases.name] = _all(conn.execute(select(cases).where(cases.c.id.in_(case_ids))))
    tree[runs.name] = _all(conn.execute(select(runs).where(runs.c.case_id.in_(case_ids))))
    run_ids = [row['id'] for row in tree[runs.name]]
    if run_ids:
        tree[evidence.name] = _all(conn.execute(
            select(evidence).where(evidence.c.run_id.in_(run_ids))))
    return tree


def copy_tree(source_engine, target_engine, case_ids) -> dict[str, int]:
    """Upsert every scoped row from source into target. Returns row counts."""
    counts = {table.name: 0 for table in CASE_TABLES}
    with source_engine.connect() as src:
        tree = load_tree(src, case_ids)
    # One transaction on the destination for the whole tree.
    with target_engine.begin() as dst:
        for table in CASE_TABLES:
            rows = tree[table.name]
            counts[table.name] = _upsert(dst, table, rows)
    return counts


def _fingerprints(rows) -> list[bytes]:
    return sorted(canonical(row) for row in rows)


def verify_tree(source_engine, target_engine, case_ids) -> dict:
    """Compare both libraries for the selected archives.

    Returns ``{"match": bool, "tables": {name: {source,target,match}}}`` and a
    small sample of differences useful for diagnostics.
    """
    with source_engine.connect() as src:
        source_tree = load_tree(src, case_ids)
    with target_engine.connect() as dst:
        target_tree = load_tree(dst, case_ids)

    tables: dict[str, dict] = {}
    match = True
    differences: list[str] = []
    for table in CASE_TABLES:
        name = table.name
        left = source_tree[name]
        right = target_tree[name]
        left_fp = _fingerprints(left)
        right_fp = _fingerprints(right)
        same = left_fp == right_fp
        match = match and same
        tables[name] = {'source': len(left), 'target': len(right), 'match': same}
        if not same and len(differences) < 5:
            only_source = len(left_fp) - len(set(left_fp) & set(right_fp))
            differences.append(f'{name}: 源 {len(left)} 行 / 目标 {len(right)} 行，'
                               f'约 {max(only_source, 0)} 行不一致')
    return {'match': match, 'tables': tables, 'differences': differences}


def sync_case(case_id: int, source_engine=None, target_engine=None) -> dict[str, int]:
    from src.services import topology
    source_engine = source_engine or topology.source_engine()
    target_engine = target_engine or topology.target_engine()
    return copy_tree(source_engine, target_engine, [int(case_id)])

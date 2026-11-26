"""Compatibility alias for the archive database migration service."""
from src.services.migration import (  # noqa: F401
    active_database,
    active_database_status,
    create_migration,
    create_order,
    current_database,
    get_migration,
    get_order,
    list_migrations,
    list_orders,
    migrate,
    migrate_archive,
    migrate_snapshot,
    resume,
    resume_migration,
    run_migration,
    run_order,
)
from src.services import topology  # noqa: F401

__all__ = [
    'create_order', 'create_migration', 'run_order', 'run_migration', 'resume',
    'resume_migration', 'get_order', 'get_migration', 'list_orders', 'list_migrations',
    'migrate', 'migrate_snapshot', 'migrate_archive', 'active_database',
    'active_database_status', 'current_database', 'topology',
]

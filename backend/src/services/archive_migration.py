"""Archive-oriented alias for the database migration service."""
from src.services.database_migration import *  # noqa: F401,F403
from src.services import database_migration as _dm  # noqa: F401

initiate_migration = _dm.create_order
start_migration = _dm.create_migration
resume_migration = _dm.resume_migration
fetch_migration = _dm.get_migration
current_accepting_database = _dm.active_database

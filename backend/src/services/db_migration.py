"""Short-name alias for :mod:`src.services.database_migration`."""
from src.services.database_migration import *  # noqa: F401,F403
from src.services import database_migration as _dm  # noqa: F401

create_order = _dm.create_order
run_order = _dm.run_order
migrate = _dm.migrate
active_database = _dm.active_database

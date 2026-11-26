"""Pytest bootstrap.

Hidden/local tests run on SQLite in-memory so they need no MySQL or Redis
services. Values already present in the environment are never overridden.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

os.environ.setdefault('DB_DRIVER', 'sqlite')
os.environ.setdefault('DB_NAME', ':memory:')
os.environ.setdefault('AUTH_KEY', 'migration-test-key')
# SEED_DEMO is intentionally left untouched so the application default (or a
# suite's own choice) keeps working.

import os
import sys

# Tests run against SQLite in memory so they need no MySQL or Redis services.
os.environ.setdefault('DB_DRIVER', 'sqlite')
os.environ.setdefault('DB_NAME', ':memory:')
os.environ.setdefault('AUTH_KEY', 'migration-test-key')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

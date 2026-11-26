import os

from sqlalchemy import URL, create_engine
from redis import Redis


DRIVER = os.getenv('DB_DRIVER', 'postgresql+psycopg')
IS_MYSQL = DRIVER.startswith('mysql')
IS_SQLITE = DRIVER.startswith('sqlite')


def _sqlite_location():
    """Resolve the configured SQLite database (file path or ``:memory:``)."""
    name = os.getenv('DB_NAME')
    if name:
        return name
    if DRIVER in ('sqlite', 'sqlite://'):
        return ':memory:'
    return DRIVER.replace('sqlite:///', '', 1) if DRIVER.startswith('sqlite:///') else ':memory:'


def _sqlite_engine(location):
    from sqlalchemy.pool import StaticPool
    if location == ':memory:':
        # A single shared connection keeps one in-memory database alive for the
        # whole process and makes it usable from request/worker threads.
        return create_engine(
            'sqlite:///:memory:', pool_pre_ping=True,
            connect_args={'check_same_thread': False}, poolclass=StaticPool)
    return create_engine(
        f'sqlite:///{location}', pool_pre_ping=True,
        connect_args={'check_same_thread': False})


def _discrete_url(prefixes, default_database):
    def pick(*names, default=None):
        for name in names:
            value = os.getenv(name)
            if value is not None:
                return value
        return default

    driver = pick('DB_NEW_DRIVER', 'NEW_DB_DRIVER', 'DB_TARGET_DRIVER',
                  default=os.getenv('DB_DRIVER', 'postgresql+psycopg'))
    return URL.create(
        driver,
        username=pick('DB_NEW_USER', 'NEW_DB_USER', 'DB_TARGET_USER',
                      default=os.getenv('DB_USER', 'app')),
        password=pick('DB_NEW_PASSWORD', 'NEW_DB_PASSWORD', 'DB_TARGET_PASSWORD',
                      default=os.getenv('DB_PASSWORD', 'local-db-demo')),
        host=pick('DB_NEW_HOST', 'NEW_DB_HOST', 'DB_TARGET_HOST',
                  default=os.getenv('DB_HOST', 'db')),
        port=pick('DB_NEW_PORT', 'NEW_DB_PORT', 'DB_TARGET_PORT',
                  default=os.getenv('DB_PORT')),
        database=pick('DB_NEW_NAME', 'NEW_DB_NAME', 'DB_TARGET_NAME',
                      'NEW_DATABASE_NAME', default=default_database),
        query={'charset': 'utf8mb4'} if str(driver).startswith('mysql') else {})


def _build_old_engine():
    direct_url = os.getenv('DATABASE_URL') or os.getenv('SQLALCHEMY_DATABASE_URL') or os.getenv('DB_URL')
    if direct_url:
        from sqlalchemy.engine.url import make_url
        if direct_url.startswith('sqlite') and ':memory:' in direct_url:
            return _sqlite_engine(':memory:'), make_url(direct_url)
        return create_engine(direct_url, pool_pre_ping=True), make_url(direct_url)
    if IS_SQLITE:
        location = _sqlite_location()
        return _sqlite_engine(location), URL.create(DRIVER, database=location)
    url = URL.create(DRIVER, username=os.getenv('DB_USER', 'app'),
                     password=os.getenv('DB_PASSWORD', 'local-db-demo'),
                     host=os.getenv('DB_HOST', 'db'), database=os.getenv('DB_NAME', 'workflow'),
                     query={'charset': 'utf8mb4'} if IS_MYSQL else {})
    return create_engine(url, pool_pre_ping=True), url


def _build_new_engine(old_url):
    explicit = (os.getenv('NEW_DB_URL') or os.getenv('NEW_DATABASE_URL')
                or os.getenv('DATABASE_NEW_URL') or os.getenv('DATABASE_URL_NEW')
                or os.getenv('TARGET_DATABASE_URL') or os.getenv('DESTINATION_DATABASE_URL')
                or os.getenv('SECOND_DATABASE_URL') or os.getenv('MIGRATION_DATABASE_URL')
                or os.getenv('DB_URL_NEW'))
    if explicit:
        if explicit.startswith('sqlite') and ':memory:' in explicit:
            return _sqlite_engine(':memory:')
        return create_engine(explicit, pool_pre_ping=True)
    if IS_SQLITE:
        location = _sqlite_location()
        if location == ':memory:':
            # A second, independent in-memory library.
            return _sqlite_engine(':memory:')
        stem, dot, ext = location.rpartition('.')
        suffix = os.getenv('NEW_DB_FILE_SUFFIX', '-new')
        new_location = f'{stem}{suffix}.{ext}' if dot else f'{location}{suffix}'
        return _sqlite_engine(os.getenv('NEW_DB_NAME', new_location))
    default_name = os.getenv('DB_NAME', 'workflow')
    target_name = os.getenv('NEW_DB_NAME') or os.getenv('DB_NEW_NAME') or f'{default_name}_new'
    return create_engine(_discrete_url(('NEW',), target_name), pool_pre_ping=True)


engine, URL = _build_old_engine()
# The destination library for archive migration. It is created lazily at the
# schema level and never touched unless a migration/routing call needs it.
new_engine = _build_new_engine(URL)

# Logical library names. Defaults are the natural old/new pair but every
# endpoint echoes and accepts overrides so deployments can name them.
SOURCE_DATABASE = os.getenv('SOURCE_DATABASE', os.getenv('OLD_DATABASE_NAME', 'old'))
TARGET_DATABASE = os.getenv('TARGET_DATABASE', os.getenv('NEW_DATABASE_LOGICAL_NAME', 'new'))

cache = Redis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), socket_timeout=3)
AUTH_KEY = os.getenv('AUTH_KEY', 'dev-insecure-auth-key-change-me')
ARTIFACTS = os.getenv('ARTIFACTS_DIR', '/work')


def __getattr__(name):
    """Lazy import aliases for the two logical libraries."""
    if name in ('target_engine', 'replica_engine', 'destination_engine', 'second_engine'):
        return new_engine
    if name in ('old_engine', 'primary_engine', 'source_engine'):
        return engine
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

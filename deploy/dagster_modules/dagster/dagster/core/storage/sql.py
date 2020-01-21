from contextlib import contextmanager

# pylint chokes on the perfectly ok import from alembic.migration
import alembic
import six
import sqlalchemy as db
from alembic.command import stamp, upgrade
from alembic.config import Config
from alembic.migration import MigrationContext  # pylint: disable=import-error
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory

from dagster.core.errors import DagsterInstanceMigrationRequired
from dagster.utils import file_relative_path
from dagster.utils.log import quieten

create_engine = db.create_engine  # exported


def get_alembic_config(dunder_file, config_path='alembic/alembic.ini', script_path='alembic/'):
    alembic_config = Config(file_relative_path(dunder_file, config_path))
    alembic_config.set_main_option('script_location', file_relative_path(dunder_file, script_path))
    return alembic_config


def run_alembic_upgrade(alembic_config, conn, run_id=None, rev='head'):
    alembic_config.attributes['connection'] = conn
    alembic_config.attributes['run_id'] = run_id
    upgrade(alembic_config, rev)


def stamp_alembic_rev(alembic_config, conn, rev='head', quiet=True):
    with quieten(quiet):
        alembic_config.attributes['connection'] = conn
        stamp(alembic_config, rev)


def check_alembic_revision(alembic_config, conn):
    migration_context = MigrationContext.configure(conn)
    db_revision = migration_context.get_current_revision()
    script = ScriptDirectory.from_config(alembic_config)
    with EnvironmentContext(alembic_config, script):
        # alembic dynamically populates the contents of alembic.context based on this context
        # manager, so pylint can't figure out what members are available
        head_revision = alembic.context.get_head_revision()  # pylint: disable=no-member

    return (db_revision, head_revision)


@contextmanager
def handle_schema_errors(conn, alembic_config, msg=None):
    try:
        yield
    except (db.exc.OperationalError, db.exc.ProgrammingError, db.exc.StatementError) as exc:
        db_revision, head_revision = (None, None)

        try:
            with quieten():
                db_revision, head_revision = check_alembic_revision(alembic_config, conn)
        # If exceptions were raised during the revision check, we want to swallow them and
        # allow the original exception to fall through
        except Exception:  # pylint: disable=broad-except
            pass

        if db_revision != head_revision:
            six.raise_from(
                DagsterInstanceMigrationRequired(
                    msg=msg, db_revision=db_revision, head_revision=head_revision
                ),
                exc,
            )

        raise


def run_migrations_offline(context, config, target_metadata):
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    connectable = config.attributes.get('connection', None)

    if connectable is None:
        raise Exception(
            'No connection set in alembic config. If you are trying to run this script from the '
            'command line, STOP and read the README.'
        )

    context.configure(
        url=connectable.url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online(context, config, target_metadata):
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = config.attributes.get('connection', None)

    if connectable is None:
        raise Exception(
            'No connection set in alembic config. If you are trying to run this script from the '
            'command line, STOP and read the README.'
        )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()

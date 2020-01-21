"""Base revision for SQL-backed event log storage

Revision ID: 567bc23fd1ac
Revises: 
Create Date: 2019-11-21 09:59:57.028730

"""
# pylint: disable=no-member
# alembic dynamically populates the alembic.context module

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.engine import reflection

from dagster.core.storage.event_log import SqlEventLogStorageTable

# revision identifiers, used by Alembic.
revision = '567bc23fd1ac'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # This is our root migration, and we don't have a common base. Before this revision, sqlite- and
    # postgres-based event logs had different schemas. The conditionality below is to deal with dev
    # databases that might not have been stamped by Alembic.
    bind = op.get_context().bind

    inspector = reflection.Inspector.from_engine(bind)

    if 'sqlite' not in inspector.dialect.dialect_description:
        raise Exception(
            'Bailing: refusing to run a migration for sqlite-backed event log storage against '
            'a non-sqlite database of dialect {dialect}'.format(
                dialect=inspector.dialect.dialect_description
            )
        )

    has_columns = [col['name'] for col in inspector.get_columns('event_logs')]
    with op.batch_alter_table('event_logs') as batch_op:
        if 'row_id' in has_columns:
            batch_op.alter_column(column_name='row_id', new_column_name='id')
        if 'run_id' not in has_columns:
            batch_op.add_column(column=sa.Column('run_id', sa.String(255)))

    op.execute(
        SqlEventLogStorageTable.update(None)
        .where(SqlEventLogStorageTable.c.run_id == None)
        .values({'run_id': context.config.attributes.get('run_id', None)})
    )


def downgrade():
    raise Exception('Base revision, no downgrade is possible')

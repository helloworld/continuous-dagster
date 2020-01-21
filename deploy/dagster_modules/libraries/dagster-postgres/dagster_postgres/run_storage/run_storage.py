from contextlib import contextmanager

import sqlalchemy as db

from dagster import check
from dagster.core.serdes import ConfigurableClass, ConfigurableClassData
from dagster.core.storage.runs import RunStorageSqlMetadata, SqlRunStorage
from dagster.core.storage.sql import create_engine, get_alembic_config, run_alembic_upgrade

from ..utils import pg_config, pg_url_from_config


class PostgresRunStorage(SqlRunStorage, ConfigurableClass):
    def __init__(self, postgres_url, inst_data=None):
        self.postgres_url = postgres_url
        with self.get_engine() as engine:
            RunStorageSqlMetadata.create_all(engine)
        self._inst_data = check.opt_inst_param(inst_data, 'inst_data', ConfigurableClassData)

    @contextmanager
    def get_engine(self):
        engine = create_engine(
            self.postgres_url, isolation_level='AUTOCOMMIT', poolclass=db.pool.NullPool
        )
        try:
            yield engine
        finally:
            engine.dispose()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return pg_config()

    @staticmethod
    def from_config_value(inst_data, config_value):
        return PostgresRunStorage(
            inst_data=inst_data, postgres_url=pg_url_from_config(config_value)
        )

    @staticmethod
    def create_clean_storage(postgres_url):
        engine = create_engine(
            postgres_url, isolation_level='AUTOCOMMIT', poolclass=db.pool.NullPool
        )
        try:
            RunStorageSqlMetadata.drop_all(engine)
        finally:
            engine.dispose()
        return PostgresRunStorage(postgres_url)

    @contextmanager
    def connect(self, _run_id=None):  # pylint: disable=arguments-differ
        with self.get_engine() as engine:
            yield engine

    def upgrade(self):
        alembic_config = get_alembic_config(__file__)
        with self.get_engine() as engine:
            run_alembic_upgrade(alembic_config, engine)

#!/usr/bin/env python3
# coding: utf-8

import logging

import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

_logger = logging.getLogger(__name__)


class SQLInterface:
    def __init__(self, engine: Engine, metadata: MetaData):
        self.engine = engine
        self.metadata = metadata

    @classmethod
    def from_config(cls, options: dict, metadata: MetaData = None):
        engine = sqlalchemy.create_engine(**options)
        return cls(engine, metadata or MetaData())

    def execute_script(self, path: str):
        with self.engine.connect() as conn:
            conn.execute('COMMIT;')
            return conn.execute(open(path).read())

    def execute(self, statement):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(str(statement))
        with self.engine.connect() as conn:
            return conn.execute(statement)

    def get_all_schemas(self) -> set:
        schemas = {'serial'}
        for tbl in self.metadata.tables.values():
            if tbl.schema is None:
                continue
            schemas.add(tbl.schema)
        return schemas

    def create_all_schemas(self):
        schemas = self.get_all_schemas()
        schemas.add('public')
        for schema in schemas:
            _logger.info('creating schema: %s', schema)
            self.engine.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};')

    def create_all_tables(self):
        tables = self.metadata.tables.values()
        tables = [t for t in tables if not t.name.endswith('_view')]
        _logger.info('creating tables: %s', tables)
        self.metadata.create_all(self.engine, tables=tables)

    def refresh_materialized_views(self, mviews: list, concurrently=True):
        for v in mviews:
            if concurrently:
                self.execute(f'REFRESH MATERIALIZED VIEW CONCURRENTLY {v};')
            else:
                self.execute(f'REFRESH MATERIALIZED VIEW {v};')
            _logger.info('mview refreshed: %s', v)
        return mviews

#!/usr/bin/env python3
# coding: utf-8

import logging
import time
import os
from fnmatch import fnmatchcase

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

_logger = logging.getLogger(__name__)


# noinspection SqlNoDataSourceInspection
class SQLInterface:
    def __init__(self, engine: Engine, metadata: MetaData):
        self.engine = engine
        self.metadata = metadata
        self._pid = os.getpid()

    @classmethod
    def from_config(cls, options: dict, metadata: MetaData = None):
        engine = sqlalchemy.create_engine(**options)
        return cls(engine, metadata or MetaData())

    def execute(self, statement):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(str(statement))
        with self.engine.connect() as conn:
            return conn.execute(statement)

    def execute_script(self, path: str):
        with self.engine.connect() as conn:
            conn.execute('COMMIT;')
            _logger.debug('execute sql script: %s', path)
            return conn.execute(open(path).read())

    def just_after_fork(self):
        # http://docs.sqlalchemy.org/en/latest/core/pooling.html
        # section "Using Connection Pools with Multiprocessing"
        if self._pid != os.getpid():
            self.engine.dispose()
            self._pid = os.getpid()

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

    def create_all_tables(self, ignore='*_view'):
        tables = self.metadata.tables.values()
        if ignore:
            tables = [t for t in tables if not fnmatchcase(t, ignore)]
        _logger.info('creating tables: %s', tables)
        self.metadata.create_all(self.engine, tables=tables)

    def get_sibling_engine(self, database: str, **kwargs) -> Engine:
        url = self.engine.url.set(database=database)
        return sqlalchemy.create_engine(url, **kwargs)

    def get_sibling(self, database: str, **kwargs) -> 'SQLInterface':
        metadata = kwargs.get('metadata') or sqlalchemy.MetaData()
        engine = self.get_sibling_engine(database, **kwargs)
        return SQLInterface(engine, metadata)

    def refresh_materialized_views(self, mviews: list, concurrently=True):
        for v in mviews:
            if concurrently:
                self.execute(f'REFRESH MATERIALIZED VIEW CONCURRENTLY {v};')
            else:
                self.execute(f'REFRESH MATERIALIZED VIEW {v};')
            _logger.info('mview refreshed: %s', v)
        return mviews

    def wait_until_server_ready(self, timeout: int = 30, period: int = 3):
        start = time.time()
        for _ in range(0, timeout, period):
            try:
                # language=SQL
                self.execute('SELECT 1;').scalar()
                _logger.info('database server is ready now!')
                return
            except sqlalchemy.exc.OperationalError as exc:
                _logger.info(
                    'failed to connect to %s -- %s',
                    self.engine.url, exc.args[0],
                )
                time.sleep(period)
        if (remaining := timeout + start - time.time()) > 0:
            time.sleep(remaining)

    def exists(self, database: str):
        sql = f"SELECT 1 FROM pg_database WHERE datname='{database}';"
        try:
            return bool(self.execute(sql).scalar())
        except sqlalchemy.exc.OperationalError:
            return False

    def create_database(self, name: str):
        if self.exists(name):
            _logger.info('database exists already, creation skipped')
            return
        with self.engine.connect():
            # language=SQL
            sql = f'CREATE DATABASE {name};'
            self.execute(sql)

#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import json
import logging
from functools import cached_property

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import MetaData, Table, select, insert
from sqlalchemy.dialects import postgresql

from joker.relational import PostgreSQLInterface, PostgreSQLAdminInterface
from joker.relational.utils import _stringify_statement

_logger = logging.getLogger(__name__)


class ExtendedPostgreSQLInterface(PostgreSQLInterface):
    _loglevel = logging.DEBUG

    def execute(self, statement, *multiparams, **params):
        if _logger.isEnabledFor(self._loglevel):
            _logger.log(self._loglevel, _stringify_statement(statement))
        with self.engine.begin() as conn:
            return conn.execute(statement, *multiparams, **params)

    @cached_property
    def admin(self) -> PostgreSQLAdminInterface:
        # at least for postgresql, you have to be on
        # an existing database to execute a sql statement
        url = self.engine.url.set(database='postgres')
        kwargs = {
            'url': url,
            "pool_size": 1,
            "isolation_level": "AUTOCOMMIT"
        }
        engine = sqlalchemy.create_engine(**kwargs)
        return PostgreSQLAdminInterface(engine, MetaData())

    def get_table(self, table_name: str) -> Table:
        metadata = self.metadata
        try:
            return metadata.tables[table_name]
        except KeyError:
            raise sqlalchemy.exc.NoSuchTableError(str(table_name))

    def upsert_one(
            self, table_name: str, data: dict,
            constraint=None, index_elements=None):
        if not data:
            return 0
        updating = data.copy()
        updating.pop('id_', 0)
        tbl = self.get_table(table_name)
        xmax = sqlalchemy.column('xmax')
        stmt = postgresql.insert(tbl)
        stmt = stmt.on_conflict_do_update(
            constraint=constraint,
            index_elements=index_elements,
            set_=updating,
        )
        stmt = stmt.returning(
            tbl.c.id_,
            # sqlalchemy.text('(xmax = 0) AS inserted'),
            (xmax == 0).label('inserted'),
        )
        # xmax stores either
        # - the id of the transaction (xid) that deleted the tuple, or
        # - row locks on the tuple
        # a tuple cannot be locked and deleted at the same time
        # https://www.cybertec-postgresql.com/en/whats-in-an-xmax/
        return self.execute(stmt, data).fetchone()

    def import_from_jsonl(self, path: str, target):
        """
        Args:
            path: a jsonl file (https://jsonlines.org/)
            target: table, model or table name as in metadata.tables
        Returns: None
        """
        if isinstance(target, str):
            target = self.metadata.tables[target]
        if hasattr(target, '__table__'):
            target = target.__table__
        stmt = select(target).limit(1)
        if self.execute(stmt).fetchone():
            return
        cnt = 0
        for line in open(path):
            doc = json.loads(line)
            # https://docs.sqlalchemy.org/en/14/core/dml.html#sqlalchemy.sql.expression.insert
            stmt = insert(target).values(**doc)
            try:
                self.execute(stmt)
                cnt += 1
                _logger.debug('inserted: %s', doc)
            except sqlalchemy.exc.IntegrityError as e:
                if 'duplicate key' in str(e):
                    _logger.error('duplicate: %s', doc)
                    continue
                raise
        _logger.info(
            '%s rows inserted into %s from %s',
            cnt, target.name, path,
        )

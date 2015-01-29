# -*- coding: utf-8 -*-
from __future__ import print_function
from contextlib import contextmanager

import psycopg2
from flask import _app_ctx_stack as stack, current_app

from logger import Logger


class PostgresWrapper(object):
    """
    Postgres connection abstraction over psycopg2
    """
    def __init__(self, connect_string):
        """
        :param connect_string: "host=localhost dbname=prkng user=user password=***"
        """
        self.db = psycopg2.connect(connect_string)

    @contextmanager
    def _query(self):
        """
        Context manager over queries.
        Initialize cursor, yield it and manage afterwards the rollback or commit
        """
        cur = self.db.cursor()
        try:
            yield cur
            self.db.commit()
        except psycopg2.Error as err:
            Logger.error(err.message.strip())
            Logger.error("Query : {}".format(cur.query))
            Logger.warning("Rollbacking")
            self.db.rollback()
            raise err

    def query(self, stmt):
        """
        Execute query
        """
        res = []
        with self._query() as cur:
            res = cur.execute(stmt)

            if cur.rowcount != -1:
                try:
                    res = cur.fetchall()
                except psycopg2.ProgrammingError:
                    # in case of update or insert
                    pass
        return res

    def queries(self, stmts):
        """
        Execute several statements in the same transaction.
        Useful when doing a lot of queries (strings) and keep rollback possible.

        :param list stmts: list of string statements
        """
        res = []
        with self._query() as cur:
            for stmt in stmts:
                cur.execute(stmt)
            if cur.rowcount != -1:
                try:
                    res = cur.fetchall()
                except psycopg2.ProgrammingError:
                    # in case of update or insert
                    pass
        return res

    def index_exists(self, table, index_name, schema='public'):
        """
        Check if an index named ``index_name`` already exists for column
        """
        count = self.query("""
            SELECT count(*)
            FROM pg_indexes
                WHERE schemaname = '{schema}'
                AND tablename = '{table}'
                AND indexname = '{index_name}'
        """.format(**locals()))

        if int(count[0][0]) == 1:
            return True
        return False

    def create_index(self, table, column, index_type='btree'):
        """
        Create indexes on ``column`` using ``index_type``
        """
        self.query("CREATE INDEX on {table} USING {index_type}({column})"
                   .format(**locals()))

    def vacuum_analyze(self, schema, table):
        """
        Free spaces for given table and collect statistics

        :param connection: pg connection instance
        :param schema: schema name
        :param table: table name

        """
        # for executing in a non transaction block
        self.db.set_session(autocommit=True)

        Logger.debug("VACUUM ANALYZE {schema}.{table}".format(schema=schema, table=table))

        self.query("VACUUM ANALYZE {schema}.{table}".format(
            schema=schema, table=table))

        # switch to default isolation level
        self.db.set_session(autocommit=False)


class Postgres(object):
    """
    Postgres connection for flask application.
    """
    def __init__(self, app=None):
        """
        :connection_string: "host=localhost dbname=prkng user=user password=***"
        """
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        # Use the newstyle teardown_appcontext
        app.teardown_appcontext(self.teardown)

    def teardown(self, exception):
        ctx = stack.top
        if hasattr(ctx, 'postgres_db'):
            ctx.pgdb.close()

    def connect(self):
        return PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**current_app.config)
        )

    @property
    def connection(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'postgres_db'):
                ctx.pgdb = self.connect()
            return ctx.pgdb


# instance of postgresql
db = Postgres()


def init_db(app):
    """
    Initialize DB into flask application
    """
    db.init_app(app)

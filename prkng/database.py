# -*- coding: utf-8 -*-
from __future__ import print_function
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import NamedTupleCursor
from sqlalchemy import create_engine

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
    def _query(self, namedtuple=None):
        """
        Context manager over queries.
        Initialize cursor, yield it and manage afterwards the rollback or commit
        """
        if namedtuple:
            cur = self.db.cursor(cursor_factory=NamedTupleCursor)
        else:
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

    def query(self, stmt, namedtuple=None):
        """
        Execute query
        """
        res = []
        with self._query(namedtuple=namedtuple) as cur:
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

    def copy_from(self, schema, table, columns, values):
        """
        Uses the efficient PostgreSQL COPY command to move data from files to tables
        """
        from cStringIO import StringIO
        cur = self.db.cursor()
        cur.copy_from(
            StringIO(
                '\n'.join('\t'.join(str(col) if col else '\\N' for col in line) for line in values)
            ),
            '{}.{}'.format(schema, table),
            columns=columns,
        )
        self.db.commit()


class Engine(object):
    """
    Postgres connection for flask application.
    """
    def __init__(self, app=None):
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

    @property
    def engine(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'postgres_db'):
                # ctx.pgdb = PostgresWrapper(
                #     "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
                #     "user={PG_USERNAME} password={PG_PASSWORD} "
                #     .format(**current_app.config)
                # )
                ctx.pgdb = create_engine(
                    '{SQLALCHEMY_DATABASE_URI}'.format(**current_app.config),
                    strategy='threadlocal'
                )
            return ctx.pgdb


def init_db(app):
    """
    Initialize DB engine and create tables
    """
    DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=app.config['PG_USERNAME'],
            password=app.config['PG_PASSWORD'],
            host=app.config['PG_HOST'],
            port=app.config['PG_PORT'],
            database=app.config['PG_DATABASE'],
        )

    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI


# instance of Engine
db = Engine()

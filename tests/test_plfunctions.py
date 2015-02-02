# -*- coding: utf-8 -*-
import pytest

from prkng.database import PostgresWrapper
from prkng import create_app
from prkng.processing.plfunctions import date_equality_func, st_isleft_func, to_time_func


@pytest.fixture(scope="module")
def db(scope="module"):
    CONFIG = create_app().config
    return PostgresWrapper(
        "host='{PG_TEST_HOST}' port={PG_TEST_PORT} dbname={PG_TEST_DATABASE} "
        "user={PG_TEST_USERNAME} password={PG_TEST_PASSWORD} ".format(**CONFIG))


def test_date_equality_func(db):
    db.query(date_equality_func)
    assert db.query('select date_equality(1, 1, 1, 4, 1, 4)')[0][0] == True
    assert db.query('select date_equality(1, 1, 1, 4, 1, 1)')[0][0] == True
    assert db.query('select date_equality(1, 1, 1, 4, 10, 4)')[0][0] == False
    assert db.query('select date_equality(1, 12, 1, 4, 1, 4)')[0][0] == True
    assert db.query('select date_equality(1, 12, 1, 4, 2, 4)')[0][0] == False
    assert db.query('select date_equality(1, 4, 1, 12, 1, 2)')[0][0] == False
    assert db.query('select date_equality(1, 3, 1, 1, 1, 2)')[0][0] == False


def test_totime_func(db):
    db.query(to_time_func)
    assert db.query('select to_time(12)')[0][0] == '12:00'
    assert db.query('select to_time(4.5)')[0][0] == '04:30'
    assert db.query('select to_time(4.25)')[0][0] == '04:15'
    assert db.query('select to_time(4.75)')[0][0] == '04:45'


def test_st_isleft(db):
    db.query('create extension if not exists postgis')
    db.query(st_isleft_func)

    # should be on the left side
    assert db.query(
        """SELECT st_isLeft(
            'linestring(0 0, 2 0, 2 2, 5 2, 5 1, 7 1)'::geometry,
            'point(1 2)'::geometry)"""
    )[0][0] == 1

    # should be on the linestring
    assert db.query(
        """SELECT st_isLeft(
            'linestring(0 0, 2 2)'::geometry,
            'point(1 1)'::geometry)"""
    )[0][0] == 0

    # should be on the right side
    assert db.query(
            """SELECT st_isLeft(
                'linestring(0 0, 2 2)'::geometry,
                'point(2 0)'::geometry)"""
        )[0][0] == -1

    # should be on the right side
    assert db.query(
            """SELECT st_isLeft(
                'linestring(0 0, 2 2)'::geometry,
                'point(2 0)'::geometry)"""
        )[0][0] == -1

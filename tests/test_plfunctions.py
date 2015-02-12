# -*- coding: utf-8 -*-
import pytest

from prkng.database import PostgresWrapper
from prkng import create_app
from prkng.processing.plfunctions import *


@pytest.fixture(scope="module")
def db(scope="module"):
    CONFIG = create_app().config
    return PostgresWrapper(
        "host='{PG_TEST_HOST}' port={PG_TEST_PORT} dbname={PG_TEST_DATABASE} "
        "user={PG_TEST_USERNAME} password={PG_TEST_PASSWORD} ".format(**CONFIG))


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

    # should be on the left side (horizontal line)
    assert db.query(
            """SELECT st_isLeft(
                'linestring(0 0, 2 0)'::geometry,
                'point(2 2)'::geometry)"""
        )[0][0] == 1


def test_get_max_range(db):
    db.query(get_max_range)
    assert db.query("select * from get_max_range(ARRAY[0.2, 0.5])")[0] == (0.5, 1)
    assert db.query("select * from get_max_range(ARRAY[0.05, 0.8])")[0] == (0.05, 0.8)
    assert db.query("select * from get_max_range(ARRAY[0, 1])")[0] == (0, 1)


def test_array_sort(db):
    db.query(array_sort)
    req = "select array_sort(ARRAY{}::float[])"

    arrays = (
        ('[0.5, 0.7, 0.1, 0.2]', [0.1, 0.2, 0.5, 0.7]),
        ('[0.1, 0.01, 30, -16]', [-16, 0.01, 0.1, 30])
    )
    for array, result in arrays:
        assert db.query(req.format(array))[0][0] == result

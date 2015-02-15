# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
import json
import pytest

from flask.ext.login import login_user, current_user

from prkng import create_app
from prkng.api import init_api
from prkng.database import init_db, db
from prkng.models import init_model, User, metadata
from prkng.processing.common import create_slots
from prkng.login import init_login


@pytest.fixture(scope="session")
def app(request):
    app = create_app(env='Testing')

    init_db(app)
    init_model(app)
    init_api(app)
    init_login(app)

    # Establish an application context before running the tests.
    ctx = app.app_context()
    ctx.push()

    def teardown():
        metadata.drop_all()
        db.engine.execute("drop table if exists slots")
        ctx.pop()

    request.addfinalizer(teardown)

    # hack to cause a new request context to be pushed and have a current user logged in
    @app.route('/test_auto_login')
    def auto_login():
        login_user(User.add_user(email='test@test', name='test_user'), True)
        return "ok"

    # create slots table
    db.engine.execute(create_slots)

    with app.test_client() as client:
        # add a user in order to honor foreign key of checkins
        client.get("/test_auto_login")

    # add some checkins
    db.engine.execute("truncate table checkins")
    for num in range(3):
        db.engine.execute("""
            INSERT INTO slots (geom)
            VALUES('SRID=3857;LINESTRING(2.765 42.988, 2.865 43.988)'::geometry)
            """.format(num))
        db.engine.execute("""
            INSERT INTO checkins (user_id, slot_id, way_name, long, lat)
            VALUES(1, {}, 'way_name', 2.765, 42.988)""".format(num))

    return app


@pytest.yield_fixture
def client(app):
    with app.test_client() as client:
        client.get("/test_auto_login")
        yield client


def test_api_me(client):
    resp = client.get('/me')
    data = json.loads(resp.data)
    assert data == {'email': 'test@test', 'gender': None, 'name': 'test_user'}
    assert not current_user.is_anonymous()


def test_api_logout(client):
    client.get('/logout')
    assert current_user.is_anonymous()


def test_api_checkin_count(client):
    resp = client.get('/checkin')
    data = json.loads(resp.data)
    assert len(data) == 3


def test_api_getcheckin(client):
    resp = client.post('/checkin', data={'slot_id': '20'})
    assert resp.status_code == 404


def test_api_postcheckin(client):
    resp = client.post('/checkin', data={'slot_id': '2'})
    assert resp.status_code == 201


def test_api_getslot(client):
    resp = client.get('/slot/2')
    data = json.loads(resp.data)
    assert data['id'] == '2'
    assert resp.status_code == 200


def test_api_getbadslot(client):
    resp = client.get('/slot/20')
    assert resp.status_code == 404

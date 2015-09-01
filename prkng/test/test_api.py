# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
import json
import pytest

from flask import g

from prkng import create_app
from prkng.api.public import init_api, v1
from prkng.models import db, init_model, User, metadata
from prkng.processing.common import create_slots
from prkng.login import init_login


@pytest.fixture(scope="session")
def app(request):
    app = create_app(env='Testing')

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
        User.get(1)
        return "ok"

    # create slots table
    db.engine.execute(create_slots)

    with app.test_client() as client:
        # add a user in order to honor foreign key of checkins
        g.user = User.add_user(email='test@test', name='test_user')
        client.get("/test_auto_login")

    db.engine.execute("""
        DROP TABLE IF EXISTS cities;
        CREATE TABLE cities AS
            SELECT 'montreal' AS name,
             'POLYGON((4.28302949061661931 42.34791957104560112,
                4.28302949061661931 44.39724932975872917,
                1.43423592493298013 44.44228954423594047,
                1.31037533512064774 42.483040214477235,
                4.28302949061661931 42.34791957104560112))'::GEOMETRY AS geom
    """)

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
    resp = client.get('/v1/user/profile', headers={'X-API-KEY': g.user.apikey})
    data = json.loads(resp.data)
    assert data['email'] == 'test@test'
    assert data['gender'] == None
    assert data['name'] == 'test_user'
    assert data['apikey']  # apikey must be non empty


def test_api_checkin_count(client):
    resp = client.get('/v1/checkins', headers={'X-API-KEY': g.user.apikey})
    data = json.loads(resp.data)
    assert len(data) == 3


def test_api_getcheckin(client):
    resp = client.post(
        '/v1/checkins',
        data={'slot_id': '20'},
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 404


def test_api_postcheckin(client):
    resp = client.post(
        '/v1/checkins',
        data={'slot_id': '2'},
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 201


def test_api_getslot(client):
    resp = client.get('/v1/slots/2', headers={'X-API-KEY': g.user.apikey})
    data = json.loads(resp.data)
    assert data['id'] == '2'
    assert resp.status_code == 200


def test_api_getbadslot(client):
    resp = client.get('/v1/slots/20', headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 404


def test_api_getslots(client):
    resp = client.get('/v1/slots?latitude=4.5&longitude=-7.5'
                      '&radius=1000&checkin=2015-03-27T09:30&duration=1',
                      headers={'X-API-KEY': g.user.apikey})
    data = json.loads(resp.data)
    assert data['message'] == 'no feature found'
    assert resp.status_code == 404


def test_api_register(client):
    resp = client.post('/v1/login/register', data=dict(
        email='test@prkng.com',
        password='incrediblepass',
        name='john doe',
        gender='male',
        birthyear=1900),
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 201


def test_api_register_already_registered(client):
    resp = client.post('/v1/login/register', data=dict(
        email='test@prkng.com',
        password='incrediblepass',
        name='john doe',
        gender='male',
        birthyear=1900),
        headers={'X-API-KEY': g.user.apikey}
    )
    assert json.loads(resp.data) == "User already exists"
    assert resp.status_code == 404


def test_api_loginemail_ok(client):
    resp = client.post('/v1/login', data=dict(
        email='test@prkng.com',
        password='incrediblepass'),
        headers={'X-API-KEY': g.user.apikey}
    )
    assert json.loads(resp.data)['name'] == 'john doe'
    assert resp.status_code == 200


def test_api_loginemail_badpass(client):
    resp = client.post('/v1/login', data=dict(
        email='test@prkng.com',
        password='incrediblep'),
        headers={'X-API-KEY': g.user.apikey})
    assert json.loads(resp.data) == 'Incorrect password'
    assert resp.status_code == 401


def test_api_update_profile(client):
    resp = client.put('/v1/user/profile', data=dict(
        email='test@prk.ng',
        password='newpassword',
        image_url='http://prk.ng/img/logo.png'),
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 200


def test_api_generate_s3_url(client):
    resp = client.post('/v1/images', data=dict(
        image_type='avatar',
        file_name='test_image.jpg'),
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 200


def test_submit_report(client):
    resp = client.post('/v1/reports', data=dict(
        slot_id=2,
        longitude=-73.5860631065519,
        latitude=45.5204204797004,
        image_url='http://prk.ng/img/logo.png'),
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 201


def test_submit_report_no_slot(client):
    resp = client.post('/v1/reports', data=dict(
        longitude=-73.5860631065519,
        latitude=45.5204204797004,
        image_url='http://prk.ng/img/logo.png'),
        headers={'X-API-KEY': g.user.apikey})
    assert resp.status_code == 201

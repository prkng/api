# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from __future__ import unicode_literals
from base64 import encodestring
from datetime import datetime
from time import time

from boto.s3.connection import S3Connection

from flask import current_app
from flask.ext.login import UserMixin
from sqlalchemy import Table, MetaData, Integer, String, Boolean, func, \
                       Float, Column, ForeignKey, DateTime, text, Index
from sqlalchemy.dialects.postgresql import JSONB, ENUM
from sqlalchemy import create_engine

from passlib.hash import pbkdf2_sha256

from itsdangerous import JSONWebSignatureSerializer

from prkng.processing.filters import on_restriction
from prkng.utils import random_string

AUTH_PROVIDERS = (
    'facebook',
    'google',
    'email'
)

metadata = MetaData()


class db(object):
    """lazy loading of db"""
    engine = None


def init_model(app):
    """
    Initialize DB engine and create tables
    """
    if app.config['TESTING']:
        DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=app.config['PG_TEST_USERNAME'],
            password=app.config['PG_TEST_PASSWORD'],
            host=app.config['PG_TEST_HOST'],
            port=app.config['PG_TEST_PORT'],
            database=app.config['PG_TEST_DATABASE'],
        )
    else:
        DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=app.config['PG_USERNAME'],
            password=app.config['PG_PASSWORD'],
            host=app.config['PG_HOST'],
            port=app.config['PG_PORT'],
            database=app.config['PG_DATABASE'],
        )

    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI

    # lazy bind the sqlalchemy engine
    with app.app_context():
        db.engine = create_engine(
            '{SQLALCHEMY_DATABASE_URI}'.format(**app.config),
            strategy='threadlocal',
            pool_size=10
        )

    metadata.bind = db.engine
    # create model
    metadata.create_all()

user_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('gender', String(10)),
    Column('email', String(60), index=True, unique=True, nullable=False),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('apikey', String),
    Column('image_url', String)
)

# creating a functional index on apikey field
user_api_index = Index(
    'idx_users_apikey',
    func.substr(user_table.c.apikey, 0, 6)
)

checkin_table = Table(
    'checkins',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('slot_id', Integer),
    Column('way_name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    # The time the check-in was created.
    Column('active', Boolean)
)

report_table = Table(
    'reports',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('slot_id', Integer),
    Column('way_name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('image_url', String)
)


userauth_table = Table(
    'users_auth',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('auth_id', String(1024), index=True, unique=True),  # id given by oauth provider
    Column('auth_type', ENUM(*AUTH_PROVIDERS, name='auth_provider')),  # oauth_type
    Column('password', String),  # for the email accounts
    Column('fullprofile', JSONB),
)


class User(UserMixin):
    """
    Subclassed UserMixin for the methods that Flask-Login expects user objects to have
    """
    def __init__(self, kwargs):
        super(UserMixin, self).__init__()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return u"<User {} : {}>".format(self.id, self.name)

    def update_apikey(self, newkey):
        """
        Update key in the database
        """
        db.engine.execute("""
            update users set apikey = '{key}'
            where id = {user_id}
            """.format(key=newkey, user_id=self.id))
        self.apikey = newkey

    def update_profile(self, name=None, email=None, gender=None, image_url=None):
        """
        Update profile information
        """
        db.engine.execute("""
            UPDATE users
            SET
                name = '{name}',
                email = '{email}',
                image_url = '{image_url}'
            WHERE id = {user_id}
            """.format(email=email or self.email,
                name=name or self.name,
                gender=gender or self.gender,
                image_url=image_url or self.image_url,
                user_id=self.id))
        self.name = name or self.name
        self.email = email or self.email
        self.gender = gender or self.gender
        self.image_url = image_url or self.image_url

    @property
    def json(self):
        vals = {
            key: value for key, value in self.__dict__.items()
        }
        # since datetime is not JSON serializable
        vals['created'] = self.created.strftime("%Y-%m-%d %H:%M:%S")
        return vals

    @staticmethod
    def generate_apikey(email):
        """
        Generate a user API key
        """
        serial = JSONWebSignatureSerializer(current_app.config['SECRET_KEY'])
        return serial.dumps({
            'email': email,
            'time': time()
        })

    @staticmethod
    def get(id):
        """
        Static method to search the database and see if user with ``id`` exists.  If it
        does exist then return a User Object.  If not then return None as
        required by Flask-Login.
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_byemail(email):
        """
        Static method to search the database and see if user with ``id`` exists.  If it
        does exist then return a User Object.  If not then return None as
        required by Flask-Login.
        """
        res = user_table.select(user_table.c.email == email).execute().first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_byapikey(apikey):
        """
        Static method to search the database and see if user with ``apikey`` exists.  If it
        does exist then return a User Object.  If not then return None as
        required by Flask-Login.
        """
        res = db.engine.execute("""
            select * from users where
            substr(apikey::text, 0, 6) = substr('{0}', 0, 6)
            AND apikey = '{0}'
            """.format(apikey)).first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_profile(id):
        """
        Static method to search the database and get a user profile.
        :returns: RowProxy object (ordereddict) or None if not exists
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return res

    @staticmethod
    def add_user(name=None, email=None, gender=None, image_url=None):
        """
        Add a new user.
        Raise an exception in case of already exists.
        """
        apikey = User.generate_apikey(email)
        # insert data
        db.engine.execute(user_table.insert().values(
            name=name, email=email, apikey=apikey, gender=gender, image_url=image_url))
        # retrieve new user informations
        res = user_table.select(user_table.c.email == email).execute().first()
        return User(res)


class UserAuth(object):
    """
    Represent an authentication method per user.
    On user can have several authentication methods (google + facebook for example).
    """
    @staticmethod
    def exists(auth_id):
        res = userauth_table.select(userauth_table.c.auth_id == auth_id).execute().first()
        return res

    @staticmethod
    def update(auth_id, birthyear):
        userauth_table.update().where(userauth_table.c.auth_id == auth_id).values(fullprofile={'birthyear': birthyear})

    @staticmethod
    def update_password(auth_id, password):
        crypt_pass = pbkdf2_sha256.encrypt(password, rounds=200, salt_size=16)
        userauth_table.update().where(userauth_table.c.auth_id == auth_id).values(password=crypt_pass)

    @staticmethod
    def add_userauth(user_id=None, name=None, auth_id=None, auth_type=None,
                     email=None, fullprofile=None, password=None):
        db.engine.execute(userauth_table.insert().values(
            user_id=user_id,
            auth_id=auth_id,
            auth_type=auth_type,
            password=password,
            fullprofile=fullprofile
        ))


class Checkins(object):
    @staticmethod
    def get(user_id):
        """
        Get info on the user's current check-in
        """
        res = db.engine.execute("""
            SELECT id, slot_id, way_name, long, lat, created::text as created, active
            FROM checkins
            WHERE user_id = {}
            AND active = true
        """.format(user_id)).first()
        if not res:
            return None
        return dict(res)

    @staticmethod
    def get_all(user_id, limit):
        res = db.engine.execute("""
            SELECT id, slot_id, way_name, long, lat, created::text as created, active
            FROM checkins
            WHERE user_id = {uid}
            ORDER BY created DESC
            LIMIT {limit}
            """.format(uid=user_id, limit=limit)).fetchall()
        return [dict(row) for row in res]

    @staticmethod
    def get_all_admin(city):
        res = db.engine.execute("""
            SELECT
                c.way_name,
                to_char(c.created, 'YYYY-Mon-D HH24:MI:SS') as created,
                u.name,
                u.email,
                u.gender,
                c.long,
                c.lat,
                c.active
            FROM {}_district d
            JOIN slots s ON ST_intersects(s.geom, d.geom)
            JOIN checkins c ON s.id = c.slot_id
            JOIN users u ON c.user_id = u.id
            """.format(city)).fetchall()

        return [
            {key: unicode(value) for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def add(user_id, slot_id):
        exists = db.engine.execute("""
            select 1 from slots where id = {slot_id}
            """.format(slot_id=slot_id)).first()
        if not exists:
            return False

        # if the user is already checked in elsewhere, deactivate their old checkin
        db.engine.execute(checkin_table.update().where(checkin_table.c.user_id == user_id).values(active=False))

        db.engine.execute("""
            INSERT INTO checkins (user_id, slot_id, way_name, long, lat, active)
            SELECT
                {user_id}, {slot_id}, way_name,
                (button_location->>'long')::float,
                (button_location->>'lat')::float,
                true
            FROM slots WHERE id = {slot_id}
        """.format(user_id=user_id, slot_id=slot_id))  # FIXME way_name
        return True

    @staticmethod
    def delete(user_id, checkin_id):
        db.engine.execute("""
            UPDATE checkins
            SET active = false
            WHERE user_id = {}
            AND id = {}
        """.format(user_id, checkin_id))
        return True


class SlotsModel(object):
    properties = (
        'id',
        'geojson',
        'rules',
        'button_location'
    )

    @staticmethod
    def get_within(x, y, radius, duration, checkin):
        """
        Retrieve the nearest slots within ``radius`` meters of a
        given location (x, y).

        Apply restrictions before sending the response
        """
        checkin = checkin or datetime.now()

        req = """
        SELECT {properties}
        FROM slots
        WHERE
            ST_Dwithin(
                st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                geom,
                {radius}
            )
        """.format(
            properties=','.join(SlotsModel.properties),
            x=x,
            y=y,
            radius=radius
        )

        features = db.engine.execute(req).fetchall()

        return filter(
            lambda x: not on_restriction(x.rules, checkin, duration),
            features
        )

    @staticmethod
    def get_byid(sid):
        """
        Retrieve the nearest slots within ``radius`` meters of a
        given location (x, y)
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM slots
            WHERE id = {sid}
            """.format(sid=sid, properties=','.join(SlotsModel.properties))).fetchall()


# associate fields for each city provider
district_field = {
    'montreal': (
        'gid as id',
        'nom_qr as name',
        'ST_AsGeoJSON(st_transform(st_simplify(geom, 10), 4326)) as geom'
    ),
    'quebec': (
        'gid as id',
        'nom as name',
        'ST_AsGeoJSON(st_transform(st_simplify(geom, 10), 4326)) as geom'
    ),
}


class District(object):
    @staticmethod
    def get(city):
        res = db.engine.execute("""
            SELECT {0}
            FROM {1}_district d
            WHERE exists(select 1 from slots where ST_intersects(slots.geom, d.geom))
            """.format(','.join(district_field[city]), city)).fetchall()
        return res

    @staticmethod
    def get_checkins(city, district_id, startdate, enddate):
        res = db.engine.execute("""
            SELECT
                c.way_name,
                to_char(c.created, 'YYYY-Mon-D HH24:MI:SS') as created,
                u.name,
                u.email,
                u.gender,
                c.long,
                c.lat,
                c.active
            FROM {1}_district d
            JOIN slots s ON ST_intersects(s.geom, d.geom)
            JOIN checkins c ON s.id = c.slot_id
            JOIN users u ON c.user_id = u.id
            WHERE d.gid = {2}
            AND c.created >= '{3}'::timestamp
            AND c.created <= '{4}'::timestamp
            """.format(
                ','.join(district_field[city]),
                city,
                district_id,
                startdate,
                enddate
            )).fetchall()

        return [
            {key: unicode(value) for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def get_reports(city, district_id):
        res = db.engine.execute("""
            SELECT
                r.id,
                to_char(r.created, 'YYYY-Mon-D HH24:MI:SS') as created,
                r.slot_id,
                u.name,
                u.email,
                s.way_name,
                r.long,
                r.lat,
                r.image_url
            FROM {}_district d
            JOIN reports r ON ST_intersects(ST_transform(ST_SetSRID(ST_MakePoint(r.long, r.lat), 4326), 3857), d.geom)
            JOIN users u ON r.user_id = u.id
            LEFT JOIN slots s ON r.slot_id = s.id
            WHERE d.gid = {}
            """.format(city, district_id)).fetchall()

        return [
            {key: unicode(value) for key, value in row.items()}
            for row in res
        ]


class Images(object):
    @staticmethod
    def generate_s3_url(image_type, file_name):
        """
        Generate S3 submission URL valid for 24h, with which the user can upload an
        avatar or a report image.
        """
        file_name = random_string(16) + "." + file_name.rsplit(".")[1]

        c = S3Connection(current_app.config["AWS_ACCESS_KEY"],
            current_app.config["AWS_SECRET_KEY"])
        url = c.generate_url(86400, "PUT", current_app.config["AWS_S3_BUCKET"],
            image_type+"/"+file_name, headers={"x-amz-acl": "public-read",
                "Content-Type": "image/jpeg"})

        return {"request_url": url, "access_url": url.split("?")[0]}


class Reports(object):
    @staticmethod
    def add(user_id, slot_id, lng, lat, url):
        db.engine.execute(report_table.insert().values(user_id=user_id, slot_id=slot_id,
            long=lng, lat=lat, image_url=url))

    @staticmethod
    def get(city):
        res = db.engine.execute("""
            SELECT
                r.id,
                to_char(r.created, 'YYYY-Mon-D HH24:MI:SS') as created,
                r.slot_id,
                u.name,
                u.email,
                s.way_name,
                r.long,
                r.lat,
                r.image_url
            FROM {}_district d
            JOIN reports r ON ST_intersects(ST_transform(ST_SetSRID(ST_MakePoint(r.long, r.lat), 4326), 3857), d.geom)
            JOIN users u ON r.user_id = u.id
            LEFT JOIN slots s ON r.slot_id = s.id
            """.format(city)).fetchall()

        return [
            {key: unicode(value) for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def delete(id):
        db.engine.execute(report_table.delete().where(report_table.c.id == id))

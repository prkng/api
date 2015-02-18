# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from __future__ import unicode_literals
from datetime import datetime
from time import time

from flask import current_app
from flask.ext.login import UserMixin
from sqlalchemy import Table, MetaData, Integer, String, func, \
                       Float, Column, ForeignKey, DateTime, text, Index
from sqlalchemy.dialects.postgresql import JSONB, ENUM
from itsdangerous import JSONWebSignatureSerializer

from prkng.processing.filters import on_restriction
from .database import db

AUTH_PROVIDERS = (
    'facebook',
    'google',
    'email'
)

metadata = MetaData()


def init_model(app):
    # lazy bind to the engine
    with app.app_context():
        metadata.bind = db.engine
    # # reflect changes in db
    metadata.create_all()


user_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('gender', String(10)),
    Column('email', String(60), index=True, unique=True, nullable=False),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('apikey', String)
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
    def add_user(name=None, email=None, gender=None):
        """
        Add a new user.
        Raise an exception in case of already exists.
        """
        apikey = User.generate_apikey(email)
        # insert data
        db.engine.execute(user_table.insert().values(
            name=name, email=email, apikey=apikey, gender=gender))
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
    def get(user_id, limit):
        res = db.engine.execute("""
            SELECT slot_id, way_name, long, lat, created::text as created
            FROM checkins
            WHERE user_id = {uid}
            ORDER BY created DESC
            LIMIT {limit}
            """.format(uid=user_id, limit=limit)).fetchall()
        return [dict(row) for row in res]

    @staticmethod
    def add(user_id, slot_id):
        exists = db.engine.execute("""
            select 1 from slots where id = {slot_id}
            """.format(slot_id=slot_id)).first()

        if not exists:
            return False
        db.engine.execute("""
            INSERT INTO checkins (user_id, slot_id, way_name, long, lat)
            SELECT
                {user_id}, {slot_id}, way_name,
                ST_X(st_centroid(geom)), ST_Y(st_centroid(geom))
            FROM slots WHERE id = {slot_id}
        """.format(user_id=user_id, slot_id=slot_id))  # FIXME way_name
        return True


class SlotsModel(object):
    properties = (
        'id',
        'geojson',
        'rules'
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
            lambda x: not on_restriction(x[2], checkin, duration),
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

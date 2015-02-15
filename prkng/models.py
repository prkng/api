# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from __future__ import unicode_literals
from datetime import datetime

from flask.ext.login import UserMixin
from sqlalchemy import Table, MetaData, Integer, String, Float, Column, ForeignKey, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB, ENUM

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
    Column('name', String(16), nullable=False),
    Column('gender', String(10)),
    Column('email', String(60), index=True, unique=True, nullable=False),
    Column('token', String(60)),
    Column('password', String),  # in case of email provider (local auth)
)


checkin_table = Table(
    'checkins',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True),
    Column('slot_id', Integer),
    Column('way_name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('created_time', DateTime, server_default=text('NOW()'), index=True),  # The time the check-in was created.
)


userauth_table = Table(
    'users_auth',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True),
    Column('auth_id', String(1024), index=True, unique=True),
    Column('auth_type', ENUM(*AUTH_PROVIDERS, name='auth_provider')),
    Column('fullprofile', JSONB)
)


class User(UserMixin):
    """
    Subclassed UserMixin for the methods that Flask-Login expects user objects to have
    """
    def __init__(self, id, name):
        super(UserMixin, self).__init__()
        self.id = id
        self.name = name

    def __repr__(self):
        return u"<User {} : {}>".format(self.id, self.name)

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
        return User(res.id, res.name)

    @staticmethod
    def get_profile(id):
        """
        Static method to search the database and get a user profile.
        :returns: RowProxy object (ordereddict) or None if not exists
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return {
            key: value
            for key, value in res.items()
            if key in ('name', 'gender', 'email')
        }

    @staticmethod
    def add_user(email=None, name=None):
        """
        Add a new user. If already exists returns himself
        """
        res = user_table.select(user_table.c.email == email).execute().first()
        if not res:
            # add the user
            db.engine.execute(user_table.insert().values(name=name, email=email))
            res = user_table.select(user_table.c.email == email).execute().first()

        return User(res.id, res.name)


class UserAuth(object):
    """
    Represent an authentication method per user.
    On user can have several authentication methods (google + facebook for example).
    """
    @staticmethod
    def get_user(auth_id):
        res = db.engine.execute("""
            SELECT user_id from users_auth where auth_id = '{}'
            """.format(auth_id)).first()

        if not res:
            return None
        return User.get(res[0])

    @staticmethod
    def add_userauth(name=None, auth_id=None, auth_type=None,
                     email=None, fullprofile=None):
        # check if exists in the users table
        # if not, create first the unique user
        # then add a auth method for this user
        user = User.add_user(email=email, name=name)
        db.engine.execute(userauth_table.insert().values(
            user_id=user.id,
            auth_id=auth_id,
            auth_type=auth_type,
            fullprofile=fullprofile
        ))
        return user


class Checkins(object):
    @staticmethod
    def get(user_id, limit):
        res = db.engine.execute("""
            SELECT slot_id, way_name, long, lat, created_time::text as created_time
            FROM checkins
            WHERE user_id = {uid}
            ORDER BY created_time DESC
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
                {user_id}, {slot_id}, 'way_name',
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

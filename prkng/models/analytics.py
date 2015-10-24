from prkng.database import db, metadata

import datetime
import json
from sqlalchemy import Column, DateTime, Float, Integer, String, Table, text
from geoalchemy2 import Geometry


search_table = Table(
    'analytics_search',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer),
    Column('search_type', String),
    Column('created', DateTime, server_default=text('NOW()')),
    Column('query', String)
)

pos_table = Table(
    'analytics_pos',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer),
    Column('geom', Geometry('MULTIPOINT', 3857)),
    Column('centerpoint', Geometry('POINT', 3857)),
    Column('count', Integer),
    Column('created', DateTime),
    Column('search_type', String)
)

event_table = Table(
    'analytics_event',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer),
    Column('created', DateTime, server_default=text('NOW()')),
    Column('lat', Float, nullable=True),
    Column('long', Float, nullable=True),
    Column('event', String)
)


class Analytics(object):
    @staticmethod
    def add_search(user_id, query):
        db.engine.execute(search_table.insert().values(user_id=user_id, query=query))

    @staticmethod
    def add_pos(stype, user_id, lat, lng, radius):
        db.engine.execute(pos_table.insert().values(search_type=stype, user_id=user_id,
            lat=lat, long=lng, radius=radius))

    @staticmethod
    def add_pos_tobuf(stype, user_id, lat, lng, radius):
        db.redis.rpush('prkng:analytics:pos', json.dumps({"search_type": stype, "user_id": user_id,
            "created": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "lat": lat, "long": lng, "radius": radius}))

    @staticmethod
    def add_pos_bulk(pos):
        db.engine.execute(pos_table.insert().execute([x for x in pos]))

    @staticmethod
    def add_event(user_id, lat, lng, event):
        db.engine.execute(event_table.insert().values(user_id=user_id, lat=lat, long=lng, event=event))

    @staticmethod
    def add_event_tobuf(user_id, lat, lng, event):
        db.redis.rpush('prkng:analytics:event', json.dumps({"user_id": user_id, "lat": lat,
            "long": lng, "created": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event": event}))

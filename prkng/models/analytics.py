from prkng.database import db, metadata

import datetime
import json
from sqlalchemy import Column, DateTime, Float, Integer, String, Table, text


search_table = Table(
    'analytics_search',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer),
    Column('created', DateTime, server_default=text('NOW()')),
    Column('query', String)
)

pos_table = Table(
    'analytics_pos',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer),
    Column('created', DateTime, server_default=text('NOW()')),
    Column('lat', Float),
    Column('long', Float),
    Column('radius', Integer)
)


class Analytics(object):
    @staticmethod
    def add_search(user_id, query):
        db.engine.execute(search_table.insert().values(user_id=user_id, query=query))

    @staticmethod
    def add_pos(user_id, lat, lng, radius):
        db.engine.execute(pos_table.insert().values(user_id=user_id, lat=lat, long=lng, radius=radius))

    @staticmethod
    def add_pos_tobuf(user_id, lat, lng, radius):
        db.redis.rpush('prkng:analytics:pos', json.dumps({"user_id": user_id, "lat": lat, "long": lng,
            "radius": radius, "created": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))

    @staticmethod
    def add_pos_bulk(pos):
        db.engine.execute(pos_table.insert().execute([x for x in pos]))

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
    """
    This class handles the storage of raw Analytics data to the database.
    Presently we collect three types of analytics data here on the backend: map positions, search queries, and events.

    Map positions represent the centerpoints of the map as the user moves it (on finger release).
    These centerpoints are aggregated by a Task into MultiPoints, which are based on movements of the map that occur within 5 minutes of each other. They are given a weight for the amount of movements that have occurred.
    This data is used to observe popularity of certain neighbourhoods of a city, and notably generates the heatmap used in the Admin interface.

    Search queries are simply the raw search data entered by users into the search bar.

    Events are emitted by the client when other notable actions happen. Presently there are many types of events, such as when the user switches from the On-Street to the Off-Street tab, or when the user logs in to a Car2Go account for use with Carshare capability. Events are stored as a predetermined `event` string.
    """

    @staticmethod
    def add_search(user_id, query):
        """
        Adds a search query to the analytics database.

        :param user_id: user ID (int)
        :param query: query the user searched for (str)
        """
        db.engine.execute(search_table.insert().values(user_id=user_id, query=query))

    @staticmethod
    def add_pos(stype, user_id, lat, lng, radius):
        """
        Adds a map position directly to the database.

        :param stype: type of search, either 'lots' or 'slots' (str)
        :param user_id: user ID (int)
        :param lat: latitude of the centerpoint (int)
        :param lng: longitude of the centerpoint (int)
        :param radius: radius of the map in meters while searching (int)
        """
        db.engine.execute(pos_table.insert().values(search_type=stype, user_id=user_id,
            lat=lat, long=lng, radius=radius))

    @staticmethod
    def add_pos_tobuf(stype, user_id, lat, lng, radius):
        """
        Adds a map position to the processing buffer.

        :param stype: type of search, either 'lots' or 'slots' (str)
        :param user_id: user ID (int)
        :param lat: latitude of the centerpoint (int)
        :param lng: longitude of the centerpoint (int)
        :param radius: radius of the map in meters while searching (int)
        """
        db.redis.rpush('prkng:analytics:pos', json.dumps({"search_type": stype, "user_id": user_id,
            "created": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "lat": lat, "long": lng, "radius": radius}))

    @staticmethod
    def add_pos_bulk(pos):
        """
        Adds many map positions to the processing buffer.
        Position objects follow similar naming convention as in the other methods above.

        :param pos: list of position dicts
        """
        db.engine.execute(pos_table.insert().execute([x for x in pos]))

    @staticmethod
    def add_event(user_id, lat, lng, event):
        """
        Adds a user-generated event directly to the database.

        :param user_id: user ID (int)
        :param lat: latitude of the centerpoint (opt int)
        :param lng: longitude of the centerpoint (opt int)
        :param event: identifier for the event that occurred (str)
        """
        db.engine.execute(event_table.insert().values(user_id=user_id, lat=lat, long=lng, event=event))

    @staticmethod
    def add_event_tobuf(user_id, lat, lng, event):
        """
        Adds a user-generated event to the processing buffer.

        :param user_id: user ID (int)
        :param lat: latitude of the centerpoint (opt int)
        :param lng: longitude of the centerpoint (opt int)
        :param event: identifier for the event that occurred (str)
        """
        db.redis.rpush('prkng:analytics:event', json.dumps({"user_id": user_id, "lat": lat,
            "long": lng, "created": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event": event}))

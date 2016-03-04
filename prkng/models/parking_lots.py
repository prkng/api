from prkng.database import db, metadata

from sqlalchemy import Boolean, Column, Integer, String, Table
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry


lots_table = Table(
    'parking_lots',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('active', Boolean, default=True),
    Column('partner_id', String, nullable=True),
    Column('partner_name', String, nullable=True),
    Column('city', String),
    Column('name', String),
    Column('operator', String, nullable=True),
    Column('capacity', Integer, nullable=True),
    Column('available', Integer, nullable=True),
    Column('address', String),
    Column('description', String),
    Column('geom', Geometry('POINT', 3857)),
    Column('geojson', JSONB),
    Column('agenda', JSONB),
    Column('attrs', JSONB),
    Column('street_view', JSONB)
)


class ParkingLots(object):
    """
    A class to manage parking lots.

    Parking lots are represented by points on a map which depict the approximate location of a garage or lot where multiple vehicles can park off-street. Many details are associated with them, such as a special agenda which represents open hours and pricing windows.

    If the parking lot has a `partner_name` and `partner_id`, they are a shared representation of a lot for which we have details that come from an external source, such as Parking Panda.
    """

    properties = (
        'id',
        'geojson',
        'city',
        'name',
        'partner_name',
        'partner_id',
        'operator',
        'capacity',
        'available',
        'address',
        'agenda',
        'attrs',
        'street_view'
    )

    @staticmethod
    def get_all():
        """
        Retrieve all parking lots/garages.

        :returns: list of Parking Lot objects (dicts)
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
        """.format(properties=','.join(ParkingLots.properties))

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_within(x, y, radius):
        """
        Retrieve the nearest parking lots/garages.

        :param x: longitude (int)
        :param y: latitude (int)
        :param radius: radius in meters to search within (int)
        :returns: list of Parking Lot objects (int)
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE
            active = true
            AND ST_Dwithin(
                st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                geom,
                {radius}
            )
        """.format(
            properties=','.join(ParkingLots.properties),
            x=x,
            y=y,
            radius=radius
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_nearest(x, y, limit):
        """
        Retrieve the nearest X parking lots/garages to a given location.

        :param x: longitude (int)
        :param y: latitude (int)
        :param limit: number of nearest lots to return (int)
        :returns: list of Parking Lot objects (int)
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
        ORDER BY ST_Distance(geom, st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857))
        LIMIT {limit}
        """.format(
            properties=','.join(ParkingLots.properties),
            x=x,
            y=y,
            limit=limit
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_boundbox(nelat, nelng, swlat, swlng):
        """
        Retrieve all parking lots / garages inside a given boundbox.

        :param nelat: latitude of northeast corner (int)
        :param nelng: longitude of northeast corner (int)
        :param swlat: latitude of southwest corner (int)
        :param swlng: longitude of southwest corner (int)
        :returns: list of Parking Lot objects (dicts)
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
            AND ST_intersects(
                ST_Transform(
                    ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326),
                    3857
                ),
                parking_lots.geom
            )
        """.format(
            properties=','.join(ParkingLots.properties),
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_byid(lid):
        """
        Retrieve lot/garage information by its ID.

        :param lid: lot ID (int)
        :returns: Parking Lot object (dict)
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM parking_lots
            WHERE id = {sid}
            """.format(sid=lid, properties=','.join(ParkingLots.properties))).fetchall()

    @staticmethod
    def get_bypartnerid(pname, pid):
        """
        Retrieve lot/garage information by its partner name and ID.

        :param pname: partner name (str)
        :param pid: partner ID (str)
        :returns: Parking Lot object (dict)
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM parking_lots
            WHERE partner_name = '{pname}' AND partner_id = '{pid}'
            """.format(pname=pname, pid=pid, properties=','.join(ParkingLots.properties))).fetchall()

from prkng.database import db, metadata

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Table, text
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry


carshares_table = Table(
    'carshares',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('city', String),
    Column('slot_id', Integer),
    Column('lot_id', Integer),
    Column('company', String),
    Column('vin', String, unique=True, nullable=True),
    Column('partners_id', Integer),
    Column('name', String),
    Column('geom', Geometry('POINT', 3857)),
    Column('address', String),
    Column('fuel', Integer),
    Column('since', DateTime, server_default=text('NOW()')),
    Column('parked', Boolean),
    Column('electric', Boolean, default=False),
    Column('geojson', JSONB)
)

carshare_lots_table = Table(
    'carshare_lots',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('partners_id', Integer),
    Column('city', String),
    Column('company', String),
    Column('name', String),
    Column('geom', Geometry('POINT', 3857)),
    Column('capacity', Integer),
    Column('available', Integer),
    Column('geojson', JSONB)
)


class Carshares(object):
    properties = (
        'id',
        'geojson',
        'company',
        'name',
        'fuel',
        'electric'
    )
    lot_properties = (
        'id',
        'geojson',
        'company',
        'name',
        'capacity',
        'available'
    )

    @staticmethod
    def get(company, name):
        """
        Get a carshare by its company and car name.
        """
        res = db.engine.execute("""
            SELECT * FROM carshares WHERE company = '{}' AND name = '{}' LIMIT 1
        """.format(company, name)).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def get_within(city, x, y, radius, company=False):
        """
        Get all parked carshares in a city within a particular radius.
        """
        qry = """
            SELECT {properties} FROM carshares c
            WHERE c.city = '{city}' AND c.parked = true AND
                ST_Dwithin(
                    st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                    c.geom,
                    {radius}
                )
        """
        if company and "," in company:
            qry += "AND c.company = ANY(ARRAY[{}])".format(",".join(["'"+z+"'" for z in company.split(",")]))
        elif company:
            qry += "AND c.company = '{}'".format(company)
        return db.engine.execute(qry.format(properties=', '.join(Carshares.properties),
            city=city, x=x, y=y, radius=radius)).fetchall()

    @staticmethod
    def get_boundbox(nelat, nelng, swlat, swlng):
        """
        Retrieve all parked carshares inside a given boundbox.
        """

        res = db.engine.execute("""
            SELECT name FROM cities
            WHERE ST_Intersects(geom,
                ST_Transform(ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326), 3857)
            )
        """.format(nelat=nelat, nelng=nelng, swlat=swlat, swlng=swlng)).first()
        if not res:
            return False

        req = """
            SELECT {properties} FROM carshares c
            WHERE c.city = '{city}' AND
                ST_intersects(
                    ST_Transform(
                        ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326),
                        3857
                    ),
                    c.geom
                )
        """.format(
            properties=','.join(Carshares.properties),
            city=res[0],
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_lots_within(city, x, y, radius, company=False):
        """
        Get all carshare lots in a city within a particular radius.
        """
        qry = """
            SELECT {properties} FROM carshare_lots
            WHERE city = '{city}' AND
                ST_Dwithin(
                    st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                    geom,
                    {radius}
                )
        """
        if company and "," in company:
            qry += "AND company = ANY(ARRAY[{}])".format(",".join(["'"+z+"'" for z in company.split(",")]))
        elif company:
            qry += "AND company = '{}'".format(company)
        return db.engine.execute(qry.format(properties=', '.join(Carshares.lot_properties),
            city=city, x=x, y=y, radius=radius)).fetchall()

    @staticmethod
    def get_all(company, city):
        """
        Get all active carshare records for a city and company.
        """
        res = db.engine.execute("""
            SELECT
                c.id,
                c.city,
                c.name,
                c.vin,
                c.address,
                to_char(c.since, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS since,
                c.geojson,
                s.rules
            FROM carshares c
            JOIN slots s ON c.city = s.city AND c.slot_id = s.id
            WHERE  c.company = '{company}'
                AND c.city   = '{city}'
                AND c.parked = true
                AND c.lot_id IS NULL
        """.format(company=company, city=city)).fetchall()
        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

from prkng.database import db


class ParkingLots(object):
    properties = (
        'id',
        'geojson',
        'name',
        'operator',
        'address',
        'agenda',
        'attrs'
    )

    @staticmethod
    def get_within(x, y, radius):
        """
        Retrieve the nearest parking lots/garages within ``radius`` meters of a
        given location (x, y).
        """
        req = """
        SELECT 1 FROM cities
        WHERE ST_Intersects(geom, ST_Buffer(ST_Transform('SRID=4326;POINT({x} {y})'::geometry, 3857), 3))
        """.format(x=x, y=y)
        if not db.engine.execute(req).first():
            return False

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
            properties=','.join(Garages.properties),
            x=x,
            y=y,
            radius=radius
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_boundbox(nelat, nelng, swlat, swlng):
        """
        Retrieve all parking lots / garages inside a given boundbox.
        """

        req = """
        SELECT 1 FROM cities
        WHERE ST_Intersects(geom, ST_Transform(ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326), 3857))
        """.format(nelat=nelat, nelng=nelng, swlat=swlat, swlng=swlng)
        if not db.engine.execute(req).first():
            return False

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
            properties=','.join(Garages.properties),
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_byid(lid):
        """
        Retrieve lot/garage information by its ID
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM parking_lots
            WHERE id = {sid}
            """.format(sid=lid, properties=','.join(Garages.properties))).fetchall()

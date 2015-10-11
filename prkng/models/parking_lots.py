from prkng.database import db


class ParkingLots(object):
    properties = (
        'id',
        'geojson',
        'city',
        'name',
        'operator',
        'capacity',
        'address',
        'agenda',
        'attrs',
        'street_view'
    )

    @staticmethod
    def get_all():
        """
        Retrieve the nearest parking lots/garages within ``radius`` meters of a
        given location (x, y).
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
        """.format(properties=','.join(ParkingLots.properties))

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_within(x, y, radius):
        """
        Retrieve the nearest parking lots/garages within ``radius`` meters of a
        given location (x, y).
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
    def get_boundbox(nelat, nelng, swlat, swlng):
        """
        Retrieve all parking lots / garages inside a given boundbox.
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
        Retrieve lot/garage information by its ID
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM parking_lots
            WHERE id = {sid}
            """.format(sid=lid, properties=','.join(ParkingLots.properties))).fetchall()

from prkng.database import db
from prkng.processing.filters import on_restriction

import datetime


class Slots(object):
    @staticmethod
    def get_within(city, x, y, radius, duration, properties, checkin=None, permit=False):
        """
        Retrieve the nearest slots within ``radius`` meters of a
        given location (x, y).

        Apply restrictions before sending the response
        """
        checkin = checkin or datetime.datetime.now()
        duration = duration or 0.5

        req = """
        SELECT {properties} FROM slots
        WHERE city = '{city}' AND
            ST_Dwithin(
                st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                geom,
                {radius}
            )
        """.format(
            properties=','.join(properties),
            city=city,
            x=x,
            y=y,
            radius=radius
        )

        features = db.engine.execute(req).fetchall()

        return filter(
            lambda x: not on_restriction(x.rules, checkin, duration, permit),
            features
        )

    @staticmethod
    def get_boundbox(
            nelat, nelng, swlat, swlng, properties, checkin=None, duration=0.25, type=None,
            permit=False, invert=False):
        """
        Retrieve all slots inside a given boundbox.
        """

        req = """
        SELECT {properties} FROM slots
        WHERE
            ST_intersects(
                ST_Transform(
                    ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326),
                    3857
                ),
                slots.geom
            )
        """.format(
            properties=','.join(properties),
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng
        )

        slots = db.engine.execute(req).fetchall()
        if checkin and invert:
            slots = filter(lambda x: on_restriction(x.rules, checkin, float(duration), permit), slots)
        elif checkin:
            slots = filter(lambda x: not on_restriction(x.rules, checkin, float(duration), permit), slots)
        if type == 1:
            slots = filter(lambda x: "paid" in [y["restrict_typ"] for y in x.rules], slots)
        elif type == 2:
            slots = filter(lambda x: "permit" in [y["restrict_typ"] for y in x.rules], slots)
        elif type == 3:
            slots = filter(lambda x: any([y["time_max_parking"] for y in x.rules]), slots)

        return slots

    @staticmethod
    def get_byid(city, sid, properties):
        """
        Retrieve slot information by its ID
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM slots
            WHERE city = '{city}' AND id = {sid}
            """.format(city=city, sid=sid, properties=','.join(properties))).fetchall()

from prkng.database import db
from prkng.filters import assign_type, on_restriction

import datetime


class Slots(object):
    @staticmethod
    def get_within(city, x, y, radius, duration, properties, checkin=None, permit=False, carsharing=False):
        """
        Retrieve the nearest slots (geometry and ID) within ``radius`` meters of a
        given location (x, y).

        Apply restrictions before sending the response
        """
        checkin = checkin or datetime.datetime.now()
        duration = duration or 0.5
        paid = True

        req = "SELECT {properties} FROM slots s "

        if carsharing:
            req += "LEFT JOIN service_areas_carsharing c ON s.city = c.city"
            permit = 'all'
            duration = 24.0
            paid = city != "seattle"
        req += """
            WHERE s.city = '{city}' AND
                ST_Dwithin(
                    st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                    s.geom,
                    {radius}
                )
        """
        if carsharing:
            req += "AND (c.id IS NULL OR (c.id IS NOT NULL AND ST_Intersects(c.geom, s.geom)))"

        req = req.format(
            properties=','.join(properties),
            city=city,
            x=x,
            y=y,
            radius=radius
        )

        features = db.engine.execute(req).fetchall()
        features = filter(lambda x: not on_restriction(x.rules, checkin, duration, paid, permit),
            features
        )
        return map(lambda x: assign_type(dict(x), checkin), features)

    @staticmethod
    def get_boundbox(
            nelat, nelng, swlat, swlng, properties, checkin=None, duration=0.25, type=None,
            permit=False, invert=False):
        """
        Retrieve all slots inside a given boundbox.
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
            SELECT {properties} FROM slots
            WHERE city = '{city}' AND
                ST_intersects(
                    ST_Transform(
                        ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326),
                        3857
                    ),
                    slots.geom
                )
        """.format(
            properties=','.join(properties),
            city=res[0],
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng
        )

        slots = db.engine.execute(req).fetchall()
        if checkin and invert:
            slots = filter(lambda x: on_restriction(x.rules, checkin, float(duration), True, permit), slots)
        elif checkin:
            slots = filter(lambda x: not on_restriction(x.rules, checkin, float(duration), True, permit), slots)
        if type == 1:
            slots = filter(lambda x: "paid" in [z for y in x.rules for z in y["restrict_types"]], slots)
        elif type == 2:
            slots = filter(lambda x: "permit" in [z for y in x.rules for z in y["restrict_types"]], slots)
        elif type == 3:
            slots = filter(lambda x: any([y["time_max_parking"] for y in x.rules]), slots)

        return map(lambda x: assign_type(dict(x), checkin), slots)

    @staticmethod
    def get_byid(sid, properties):
        """
        Retrieve slot information by its ID
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM slots
            WHERE id = {sid}
            """.format(sid=sid, properties=','.join(properties))).fetchall()

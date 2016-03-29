from prkng.database import db
from prkng.filters import on_restriction, remove_not_applicable, add_temporary_restrictions

import datetime


class Slots(object):
    """
    An object allowing the management of Slots.

    Slots are lines that represent contiguous curb space for contiguous parking regulation. Put differently, they represent the linear parking spaces on a street that adhere to a particular regulation or set of regulations. They are the principal objects displayed in Prkng's client map view.
    """

    @staticmethod
    def get_within(city, x, y, radius, duration, properties, checkin=None, permit=False, carsharing=False):
        """
        Retrieve the nearest slots to a given location.
        Applies restrictions and filtering before sending the response.

        :param city: city name (str)
        :param x: longitude (int)
        :param y: latitude (int)
        :param radius: radius in meters to search within (int)
        :param duration: duration of the desired parking time (float)
        :param properties: properties to return on the Slot object (list)
        :param checkin: timestamp for the start of the desired parking time (ISO-8601 str)
        :param permit: comma-separated list of permits to exclude from restriction filtering (str)
        :param carsharing: True if carsharing restrictions should also be applied to the filter (bool)
        :returns: list of Slot objects (dicts)
        """
        checkin = checkin or datetime.datetime.now()
        duration = duration or 0.5
        paid = True

        req = "SELECT {properties}, t.rule AS temporary_rule FROM slots s "

        if carsharing:
            req += "LEFT JOIN service_areas_carsharing c ON s.city = c.city"
            permit = 'all'
            duration = 24.0
            paid = city == "seattle"
        req += """
            LEFT JOIN temporary_restrictions t ON t.city = s.city AND t.active = true AND s.id = ANY(t.slot_ids)
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
            properties=','.join(["s."+z for z in properties]),
            city=city,
            x=x,
            y=y,
            radius=radius
        )

        features = db.engine.execute(req).fetchall()
        features = map(lambda x: on_restriction(dict(x), checkin, duration, paid, permit), features)
        return filter(lambda x: x != False, features)

    @staticmethod
    def get_boundbox(
            nelat, nelng, swlat, swlng, properties, checkin=None, duration=0.25, type=None,
            permit=False, invert=False):
        """
        Retrieve all slots inside a given boundbox.
        Used only for Admin interface.

        :param nelat: latitude of northeast corner (int)
        :param nelng: longitude of northeast corner (int)
        :param swlat: latitude of southwest corner (int)
        :param swlng: longitude of southwest corner (int)
        :param properties: properties to return on the Slot object (list)
        :param checkin: timestamp for the start of the desired parking time (ISO-8601 str)
        :param duration: duration of the desired parking time (float)
        :param type: type of filtering to do (1 for paid only, 2 for permit only, 3 for time max only) (int)
        :param permit: comma-separated list of permits to exclude from restriction filtering (str)
        :param invert: True to instead return slots that would be restricted under these conditions (bool)
        :returns: list of Slot objects (dicts)
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
            SELECT {properties}, ARRAY[]::varchar[] AS restrict_types FROM slots
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
        slots = map(lambda x: dict(x), slots)
        if checkin and invert:
            slots = map(lambda x: on_restriction(x, checkin, float(duration), True, permit), slots)
        elif checkin:
            slots = map(lambda x: on_restriction(x, checkin, float(duration), True, permit), slots)
        slots = filter(lambda x: x != False, slots)
        
        if type == 1:
            slots = filter(lambda x: "paid" in [z for y in x["rules"] for z in y["restrict_types"]], slots)
        elif type == 2:
            slots = filter(lambda x: "permit" in [z for y in x["rules"] for z in y["restrict_types"]], slots)
        elif type == 3:
            slots = filter(lambda x: any([y["time_max_parking"] for y in x["rules"]]), slots)

        for x in slots:
            for y in x["rules"]:
                if "paid" in y["restrict_types"]:
                    x["restrict_types"] = ["paid"]

        return slots

    @staticmethod
    def get_byid(sid, properties, remove_na=False, checkin=False, permit=False):
        """
        Retrieve slot information by its ID.

        :param sid: slot ID (int)
        :param properties: properties to return on the Slot object (list)
        :param remote_na: True to remove restrictions that are not applicable (bool)
        :param checkin: timestamp for the start of the desired parking time (ISO-8601 str)
        :param permit: comma-separated list of permits to exclude from restriction filtering (str)
        :returns: Slot object (dict)
        """
        checkin = checkin or datetime.datetime.now()
        res = db.engine.execute("""
            SELECT {properties}, t.rule AS temporary_rule
            FROM slots s
            LEFT JOIN temporary_restrictions t ON t.active = true AND {sid} = ANY(t.slot_ids)
            WHERE s.id = {sid}
            """.format(sid=sid, properties=','.join(["s."+x for x in properties]))).fetchall()
        res = map(lambda x: add_temporary_restrictions(x), res)
        if remove_na:
            return map(lambda x: remove_not_applicable(x, checkin, permit), res)
        return res

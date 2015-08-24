from prkng.database import db

import aniso8601


class City(object):
    @staticmethod
    def get_all(returns="json"):
        return db.engine.execute("""
            SELECT
                gid AS id,
                name,
                name_disp,
                ST_As{}(ST_Transform(geom, 4326)) AS geom
            FROM cities
        """.format("GeoJSON" if returns == "json" else "KML")).fetchall()

    @staticmethod
    def get_assets():
        res = db.engine.execute("""
            SELECT
                version,
                kml_addr,
                geojson_addr,
                kml_mask_addr,
                geojson_mask_addr
            FROM city_assets
        """).fetchall()

        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def get_checkins(city, start, end):
        res = db.engine.execute("""
            SELECT
                c.id,
                c.user_id,
                s.id AS slot_id,
                c.way_name,
                to_char(c.created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created,
                u.name,
                u.email,
                u.gender,
                c.long,
                c.lat,
                c.active,
                a.auth_type AS user_type,
                s.rules
            FROM checkins c
            JOIN slots s ON s.id = c.slot_id
            JOIN users u ON c.user_id = u.id
            JOIN cities ct ON ST_intersects(s.geom, ct.geom)
            JOIN
                (SELECT auth_type, user_id, max(id) AS id
                    FROM users_auth GROUP BY auth_type, user_id) a
                ON c.user_id = a.user_id
            WHERE ct.name = '{}'
            {}
            """.format(city,
                ((" AND (c.created AT TIME ZONE 'UTC') >= '{}'".format(aniso8601.parse_datetime(start).strftime("%Y-%m-%d %H:%M:%S"))) if start else "") +
                ((" AND (c.created AT TIME ZONE 'UTC') <= '{}'".format(aniso8601.parse_datetime(end).strftime("%Y-%m-%d %H:%M:%S"))) if end else "")
            )).fetchall()

        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def get_reports(city):
        res = db.engine.execute("""
            SELECT
                r.id,
                to_char(r.created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created,
                r.slot_id,
                u.id AS user_id,
                u.name AS user_name,
                u.email AS user_email,
                s.way_name,
                s.rules,
                r.long,
                r.lat,
                r.image_url,
                r.notes,
                r.progress,
                ARRAY_REMOVE(ARRAY_AGG(c.id), NULL) AS corrections
            FROM reports r
            JOIN cities ct ON ST_intersects(ST_transform(ST_SetSRID(ST_MakePoint(r.long, r.lat), 4326), 3857), ct.geom)
            JOIN users u ON r.user_id = u.id
            LEFT JOIN slots s ON r.slot_id = s.id
            LEFT JOIN corrections c ON s.signposts = c.signposts
            WHERE ct.name = '{}'
            GROUP BY r.id, u.id, s.way_name, s.rules
            """.format(city)).fetchall()

        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def get_corrections(city):
        res = db.engine.execute("""
            SELECT
                c.*,
                s.id AS slot_id,
                s.way_name,
                s.button_location ->> 'lat' AS lat,
                s.button_location ->> 'long' AS long,
                c.code = ANY(ARRAY_AGG(codes->>'code')) AS active
            FROM corrections c,
                slots s,
                jsonb_array_elements(s.rules) codes
            WHERE c.city = '{}'
                AND c.signposts = s.signposts
            GROUP BY c.id, s.id
        """.format(city)).fetchall()

        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

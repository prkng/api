from prkng.database import db
from prkng.processing.common import process_corrected_rules, process_corrections

from sqlalchemy import text


class Corrections(object):
    @staticmethod
    def add(
            slot_id, code, city, description, initials, season_start, season_end,
            time_max_parking, agenda, special_days, restrict_typ):
        # get signposts by slot ID
        res = db.engine.execute("""
            SELECT way_name, signposts FROM slots WHERE city = '{city}' AND id = {id}
        """.format(city=city, id=slot_id)).first()
        if not res:
            return False

        # map correction to signposts and save
        res = db.engine.execute(
            """
            INSERT INTO corrections
                (initials, address, signposts, code, city, description, season_start, season_end,
                    time_max_parking, agenda, special_days, restrict_typ)
            SELECT '{initials}', '{address}', ARRAY{signposts}, '{code}', '{city}', '{description}',
                {season_start}, {season_end}, {time_max_parking}, '{agenda}'::jsonb,
                {special_days}, {restrict_typ}
            RETURNING *
            """.format(initials=initials, address=res[0], signposts=res[1], code=code, city=city,
                description=description, season_start="'"+season_start+"'" if season_start else "NULL",
                season_end="'"+season_end+"'" if season_end else "NULL",
                time_max_parking=time_max_parking or "NULL", agenda=agenda,
                special_days="'"+special_days+"'" if special_days else "NULL",
                restrict_typ="'"+restrict_typ+"'" if restrict_typ else "NULL")
        ).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def apply():
        # apply any pending corrections to existing slots
        db.engine.execute(text(process_corrected_rules).execution_options(autocommit=True))
        db.engine.execute(text(process_corrections).execution_options(autocommit=True))

    @staticmethod
    def get(id):
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
            WHERE c.id = {id}
              AND s.city = c.city
              AND s.signposts = c.signposts
            GROUP BY c.id, s.id
        """.format(id=id)).first()
        if not res:
            return False

        return {key: value for key, value in res.items()}

    @staticmethod
    def delete(id):
        db.engine.execute("""
            DELETE FROM corrections
            WHERE id = {}
        """.format(id))

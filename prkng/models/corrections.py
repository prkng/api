from prkng.database import db, metadata
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Table, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


corrections_table = Table(
    'corrections',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('city', String),
    Column('signposts', ARRAY(Integer)),
    Column('code', String),
    Column('description', String),
    Column('periods', ARRAY(ARRAY(String))),
    Column('time_max_parking', Integer),
    Column('agenda', JSONB),
    Column('special_days', String),
    Column('restrict_types', ARRAY(String)),
    Column('initials', String),
    Column('address', String),
    Column('created', DateTime, server_default=text('NOW()'), index=True)
)


class Corrections(object):
    @staticmethod
    def add(
            slot_id, code, city, description, initials, periods,
            time_max_parking, agenda, special_days, restrict_types):
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
                (initials, address, signposts, code, city, description, periods,
                    time_max_parking, agenda, special_days, restrict_types)
            SELECT '{initials}', '{address}', ARRAY{signposts}, '{code}', '{city}', '{description}',
                '{{{}}}'::varchar[][], {time_max_parking}, '{agenda}'::jsonb,
                {special_days}, '{{{restrict_types}}}'::varchar[]
            RETURNING *
            """.format(initials=initials, address=res[0], signposts=res[1], code=code, city=city,
                description=description, periods=periods if periods else "{}",
                time_max_parking=time_max_parking or "NULL", agenda=agenda,
                special_days="'"+special_days+"'" if special_days else "NULL",
                restrict_types=restrict_types if restrict_types else "")
        ).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def process_corrected_rules():
        db.engine.execute("""
            WITH s AS (
              -- get the rule if it already exists
              SELECT c.id, r.code, r.description FROM corrections c
                LEFT JOIN rules r
                   ON r.periods          = c.periods
                  AND r.time_max_parking = c.time_max_parking
                  AND r.agenda           = c.agenda
                  AND r.special_days     = c.special_days
                  AND r.restrict_types   = c.restrict_types
            ), i AS (
              -- if it doesn't exist, create it
              INSERT INTO rules
                (code, description, periods, time_max_parking,
                  agenda, special_days, restrict_types)
                SELECT c.code, c.description, c.periods, c.time_max_parking,
                  c.agenda, c.special_days, c.restrict_types
                  FROM corrections c, s
                  WHERE c.id = s.id AND s.code IS NULL
                  RETURNING code, description
            )
            -- finally update the original correction w/ proper code/desc if needed
            UPDATE corrections c
              SET code = s.code, description = s.description
              FROM s
              WHERE c.id = s.id
                AND c.code <> s.code
        """)

    @staticmethod
    def process_corrections():
        db.engine.execute("""
            WITH r AS (
              SELECT
                city,
                signposts,
                array_to_json(
                  array_agg(distinct
                  json_build_object(
                    'code', code,
                    'description', description,
                    'periods', periods,
                    'address', address,
                    'agenda', agenda,
                    'time_max_parking', time_max_parking,
                    'special_days', special_days,
                    'restrict_types', restrict_types,
                    'permit_no', NULL
                  )::jsonb
                ))::jsonb AS rules
              FROM corrections
              GROUP BY city, signposts
            )
            UPDATE slots s
              SET rules = r.rules
              FROM r
              WHERE s.city = r.city
                AND s.signposts = r.signposts
                AND s.rules <> r.rules
        """)

    @staticmethod
    def apply():
        # apply any pending corrections to existing slots
        Corrections.process_corrected_rules()
        Corrections.process_corrections()

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

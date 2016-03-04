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
    Column('season_start', String),
    Column('season_end', String),
    Column('time_max_parking', Integer),
    Column('agenda', JSONB),
    Column('special_days', String),
    Column('restrict_types', ARRAY(String)),
    Column('initials', String),
    Column('address', String),
    Column('created', DateTime, server_default=text('NOW()'), index=True)
)


class Corrections(object):
    """
    A class to retrieve and manage Correction objects.

    Corrections are manually-made adjustments to a city's parking data, based on reports received and actioned on by Prkng staff. They persist between data updates to allow our corrections to always be visible for certain slots/roads until they are removed.
    """

    @staticmethod
    def add(
            slot_id, code, city, description, initials, season_start, season_end,
            time_max_parking, agenda, special_days, restrict_types):
        """
        Add a correction to the database. Must apply before it will take effect.

        :param slot_id: the slot ID to add the correction for (int)
        :param code: the new or existing rule code to use (str)
        :param description: if adding a new rule, use this description for it (str)
        :param initials: initials of the Prkng staff member making this correction (str)
        :param season_start: month-day of the start of the season (e.g. '04-01'), or empty string (str)
        :param season_end: month-day of the end of the season (e.g. '11-30'), or empty string (str)
        :param time_max_parking: time in minutes for max parking restrictions, or None (int)
        :param agenda: Agenda object for the rule (dict)
        :param special_days: special days for which this rule is to be in effect (str)
        :param restrict_types: array of applicable restrict types (strs)
        :returns: Correction object (dict)
        """
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
                    time_max_parking, agenda, special_days, restrict_types)
            SELECT '{initials}', '{address}', ARRAY{signposts}, '{code}', '{city}', '{description}',
                {season_start}, {season_end}, {time_max_parking}, '{agenda}'::jsonb,
                {special_days}, '{{{restrict_types}}}'::varchar[]
            RETURNING *
            """.format(initials=initials, address=res[0], signposts=res[1], code=code, city=city,
                description=description, season_start="'"+season_start+"'" if season_start else "NULL",
                season_end="'"+season_end+"'" if season_end else "NULL",
                time_max_parking=time_max_parking or "NULL", agenda=agenda,
                special_days="'"+special_days+"'" if special_days else "NULL",
                restrict_types=restrict_types if restrict_types else "")
        ).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def process_corrected_rules():
        """
        Process the corrected rules in-system. Namely, checks to see if any rules exist with the same properties. If one does, it is used in the place of the rule code/description given to that rule already. If it doesn't, it is added to the rules table as a new rule.
        """
        db.engine.execute("""
            WITH s AS (
              -- get the rule if it already exists
              SELECT c.id, r.code, r.description FROM corrections c
                LEFT JOIN rules r
                   ON r.season_start     = c.season_start
                  AND r.season_end       = c.season_end
                  AND r.time_max_parking = c.time_max_parking
                  AND r.agenda           = c.agenda
                  AND r.special_days     = c.special_days
                  AND r.restrict_types   = c.restrict_types
            ), i AS (
              -- if it doesn't exist, create it
              INSERT INTO rules
                (code, description, season_start, season_end, time_max_parking,
                  agenda, special_days, restrict_types)
                SELECT c.code, c.description, c.season_start, c.season_end, c.time_max_parking,
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
        """
        Process corrections and set the changed rules as being applicable for their given slots.
        """
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
                    'season_start', season_start,
                    'season_end', season_end,
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
        """
        Shortcut to run `process_corrected_rules` and `process_corrections`.
        """
        # apply any pending corrections to existing slots
        Corrections.process_corrected_rules()
        Corrections.process_corrections()

    @staticmethod
    def get(id):
        """
        Get a correction by its ID.

        :param id: correction ID (int)
        :returns: Correction object (dict)
        """
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
        """
        Delete a correction from the database.
        Corrections need to be applied again in order to complete the removal process.

        :param id: correction ID (int)
        """
        db.engine.execute("""
            DELETE FROM corrections
            WHERE id = {}
        """.format(id))

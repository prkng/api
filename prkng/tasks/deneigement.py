# -*- coding: utf-8 -*-

from prkng import create_app, notifications
from prkng.database import PostgresWrapper

import aniso8601
from babel.dates import format_datetime
import datetime
import json
import os
import pytz
from redis import Redis
from rq import Queue
from suds.client import Client


def deneigement_notifications():
    q = Queue('medium', connection=Redis(db=1))
    q.enqueue(push_deneigement_scheduled)
    q.enqueue(push_deneigement_8hr)


def update_deneigement():
    """
    Task to check with Montreal Planif-Neige API and note snow-clearing operations
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    logfile = os.path.join(os.path.expanduser('~'), 'log', 'deneigement.log')
    if not CONFIG["DEBUG"]:
        logfile = '/home/parkng/log/deneigement.log'

    with open(logfile, 'a') as f:
        f.write("Snow removal API check: {} === ".format(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')))
    client = Client("https://servicesenligne2.ville.montreal.qc.ca/api/infoneige/InfoneigeWebService?WSDL")
    planification_request = client.factory.create('getPlanificationsForDate')
    planification_request.fromDate = (datetime.datetime.utcnow().replace(tzinfo=pytz.utc) - datetime.timedelta(minutes=30)).astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%dT%H:%M:%S')
    planification_request.tokenString = CONFIG["PLANIFNEIGE_API_KEY"]
    response = client.service.GetPlanificationsForDate(planification_request)
    if response['responseStatus'] == 8:
        # No new data
        with open(logfile, 'a') as f:
            f.write(" > All's well. (0)\n")
    elif response['responseStatus'] != 0:
        # An error occurred
        raise Exception("Info-Neige call failed: code {}, message: {}".format(response['responseStatus'],
            response['responseDesc'].encode('utf-8')))
    db.query("""
        CREATE TABLE IF NOT EXISTS temporary_restrictions (
            id serial primary key,
            city varchar,
            partner_id integer,
            r15id varchar,
            modified timestamp default NOW(),
            start timestamp,
            finish timestamp,
            type varchar,
            meta varchar,
            rule jsonb,
            active boolean
        )
    """)
    values, record = [], "({},'{}'::timestamp,'{}'::timestamp,{},'{}'::jsonb,{})"
    for x in response['planifications']['planification']:
        if x['etatDeneig'] in [2, 3] and hasattr(x, 'dateDebutPlanif'):
            debut, fin = x['dateDebutPlanif'], x['dateFinPlanif']
            if hasattr(x, 'dateDebutReplanif'):
                debut, fin = x['dateDebutReplanif'], x['dateFinReplanif']
            agenda = {str(z): [] for z in range(1,8)}
            debutJour, finJour = debut.isoweekday(), fin.isoweekday()
            debutHeure = float(debut.hour) + (float(debut.minute) / 60.0)
            finHeure = float(fin.hour) + (float(fin.minute) / 60.0)
            if debutJour == finJour:
                agenda[str(debutJour)] = [[debutHeure, finHeure]]
            else:
                agenda[str(debutJour)] = [[debutHeure, 24.0]]
                agenda[str(finJour)] = [[0.0, finHeure]]
                if (fin.day - debut.day) > 1:
                    if debutJour > finJour:
                        for z in range(debutJour, 8):
                            agenda[str(z)] = [[0.0,24.0]]
                        for z in range(1, finJour + 1):
                            agenda[str(z)] = [[0.0,24.0]]
                    else:
                        for z in range(debutJour + 1, finJour + 1):
                            agenda[str(z)] = [[0.0,24.0]]
            rule = {"code": "MTL-NEIGE", "description": "DÉNEIGEMENT PRÉVU DANS CE SECTEUR",
                "season_start": None, "season_end": None, "agenda": agenda, "time_max_parking": None,
                "special_days": None, "restrict_types": ["snow"], "permit_no": None}
            values.append(record.format(x['coteRueId'], debut.strftime('%Y-%m-%d %H:%M:%S'),
                fin.strftime('%Y-%m-%d %H:%M:%S'), 'true', json.dumps(rule), x['etatDeneig']))
        elif x['etatDeneig'] in [0, 1, 4, 10]:
            values.append(record.format(x['coteRueId'], datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'false', '{}', x['etatDeneig']))
    with open(logfile, 'a') as f:
        f.write(" > All's well. ({})\n".format(len(values)))

    if values:
        # update temporary restrictions item when we are already tracking the blockface
        db.query("""
            UPDATE temporary_restrictions d SET start = x.start, finish = x.finish,
                active = x.active, rule = x.rule, modified = NOW(), meta = x.state::text
            FROM (VALUES {}) AS x(geobase_id, start, finish, active, rule, state)
            WHERE d.city = 'montreal' AND d.type = 'snow' AND x.geobase_id::text = d.partner_id
              AND (x.start != d.start OR x.finish != d.finish OR x.active != d.active
                OR x.state::text != d.meta)
        """.format(",".join(values)))

        # insert temporary restrictions for newly-mentioned blockfaces, and link with current slot IDs
        db.query("""
            INSERT INTO temporary_restrictions (city, start, finish,
                    rule, type, active, meta, partner_id, r15id)
                SELECT DISTINCT 'montreal', x.start, x.finish, x.rule, 'snow',
                    x.active, x.state::text, x.geobase_id,
                    (CASE WHEN x.geobase_id = r.lrid THEN (r.r14id || '0')
                     ELSE (r.r14id || '1') END)
                FROM (VALUES {}) AS x(geobase_id, start, finish, active, rule, state)
                JOIN roads r ON s.city = 'montreal' AND (r.lrid = x.geobase_id OR r.rrid = x.geobase_id)
                WHERE (SELECT 1 FROM temporary_restrictions l WHERE l.type = 'snow'
                            AND l.partner_id = x.geobase_id::text LIMIT 1) IS NULL
        """.format(",".join(values)))


def push_deneigement_scheduled():
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    # grab the appropriate checkins to send pushes to by slot ID
    start = datetime.datetime.now()
    finish = start - datetime.timedelta(minutes=5)
    res = db.query("""
        SELECT DISTINCT x.start, u.lang, u.sns_id
        FROM temporary_restrictions x
        JOIN checkins c ON c.slot_id = ANY(x.slot_ids)
        JOIN users u ON c.user_id = u.id
        WHERE (x.meta = '2' OR x.meta = '3') AND x.active = true AND x.type = 'snow'
            AND x.modified > '{}' AND x.modified < '{}'
            AND x.start > (NOW() AT TIME ZONE 'US/Eastern' + INTERVAL '6 HOURS')
            AND c.active = true AND c.checkout_time IS NULL
            AND u.push_on_temp = true AND u.sns_id IS NOT NULL
            AND c.checkin_time > (NOW() - INTERVAL '14 DAYS')
    """.format(finish.strftime('%Y-%m-%d %H:%M:%S'), start.strftime('%Y-%m-%d %H:%M:%S')))

    # group device IDs by start time, then send messages
    lang_en, lang_fr = filter(lambda x: x[1] == 'en', res), filter(lambda x: x[1] == 'fr', res)
    data = {"en": {x: [] for x in set([z[0].isoformat() for z in lang_en])},
        "fr": {x: [] for x in set([z[0].isoformat() for z in lang_fr])}}
    for x in lang_en:
        data["en"][x[0].isoformat()].append(x[2])
    for x in lang_fr:
        data["fr"][x[0].isoformat()].append(x[2])
    for x in data["en"].keys():
        dt = format_datetime(aniso8601.parse_datetime(x), u"h:mm a 'on' EEEE d MMM")
        notifications.schedule_notifications(data["en"][x],
            "❄️ Snow removal scheduled! Move your car before {}".format(dt))
    for x in data["fr"].keys():
        dt = format_datetime(aniso8601.parse_datetime(x), u"H'h'mm', 'EEEE 'le 'd MMM", locale='fr_FR')
        notifications.schedule_notifications(data["fr"][x],
            "❄️ Déneigement annoncé ! Déplacez votre véhicule avant {}".format(dt))


def push_deneigement_8hr():
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    start = (datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern')) + datetime.timedelta(hours=8))
    finish = start - datetime.timedelta(minutes=5)
    res = db.query("""
        SELECT DISTINCT x.start, u.lang, u.sns_id
        FROM temporary_restrictions x
        JOIN checkins c ON c.slot_id = ANY(x.slot_ids)
        JOIN users u ON c.user_id = u.id
        WHERE (x.meta = '2' OR x.meta = '3') AND x.active = true AND x.type = 'snow'
            AND x.start > '{}' AND x.start < '{}'
            AND c.active = true AND c.checkout_time IS NULL
            AND u.push_on_temp = true AND u.sns_id IS NOT NULL
            AND c.checkin_time > (NOW() - INTERVAL '14 DAYS')
    """.format(finish.strftime('%Y-%m-%d %H:%M:%S'), start.strftime('%Y-%m-%d %H:%M:%S')))

    # group device IDs by start time, then send messages
    lang_en, lang_fr = filter(lambda x: x[1] == 'en', res), filter(lambda x: x[1] == 'fr', res)
    data = {"en": {x: [] for x in set([z[0].isoformat() for z in lang_en])},
        "fr": {x: [] for x in set([z[0].isoformat() for z in lang_fr])}}
    for x in lang_en:
        data["en"][x[0].isoformat()].append(x[2])
    for x in lang_fr:
        data["fr"][x[0].isoformat()].append(x[2])
    for x in data["en"].keys():
        dt = format_datetime(aniso8601.parse_datetime(x), u"h:mm a")
        notifications.schedule_notifications(data["en"][x],
            u"❄️ Attention, snow removal starts in 8 hours, at {}!".format(dt))
    for x in data["fr"].keys():
        dt = format_datetime(aniso8601.parse_datetime(x), u"H'h'mm", locale='fr_FR')
        notifications.schedule_notifications(data["fr"][x],
            u"❄️ Attention, le déneigement commence dans 8h, à {} !".format(dt))

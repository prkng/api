# -*- coding: utf-8 -*-

import datetime

from redis import Redis
from rq_scheduler import Scheduler

from carsharing import *
from deneigement import *
from general import *


scheduler = Scheduler('scheduled_jobs', connection=Redis(db=1))


def init_tasks(debug=True):
    now = datetime.datetime.now()
    stop_tasks()

    # Carsharing
    scheduler.schedule(scheduled_time=now, func=update_car2go, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_automobile, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_communauto, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_zipcar, interval=86400, result_ttl=172800, repeat=None)

    # Parking lots
    scheduler.schedule(scheduled_time=now, func=update_parkingpanda, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_seattle_lots, interval=120, result_ttl=240, repeat=None)

    # Analytics
    scheduler.schedule(scheduled_time=now, func=update_analytics, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_free_spaces, interval=300, result_ttl=600, repeat=None)

    # Notifications
    scheduler.schedule(scheduled_time=now, func=hello_amazon, interval=300, result_ttl=600, repeat=None)
    scheduler.schedule(scheduled_time=now, func=send_notifications, interval=300, result_ttl=600, repeat=None)

    # Déneigement
    scheduler.schedule(scheduled_time=now, func=push_deneigement_scheduled, interval=300, result_ttl=600, repeat=None)
    scheduler.schedule(scheduled_time=now, func=push_deneigement_8hr, interval=300, result_ttl=600, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_deneigement, interval=1800, result_ttl=3600, repeat=None)

def stop_tasks():
    for x in scheduler.get_jobs():
        scheduler.cancel(x)

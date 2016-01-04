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

    # Every 2 min
    scheduler.schedule(scheduled_time=now, func=update_lots, interval=120, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_carshares, interval=120, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_analytics, interval=120, result_ttl=240, repeat=None)

    # Every 5 min
    scheduler.schedule(scheduled_time=now, func=update_free_spaces, interval=300, result_ttl=600, repeat=None)
    scheduler.schedule(scheduled_time=now, func=process_notifications, interval=300, repeat=None)
    scheduler.schedule(scheduled_time=now, func=deneigement_notifications, interval=300, repeat=None)

    # Every 30 min
    scheduler.schedule(scheduled_time=now, func=update_deneigement, interval=1800, result_ttl=3600, repeat=None)

    # Every day
    if not debug:
        scheduler.schedule(scheduled_time=now, func=run_backup, interval=86400, result_ttl=172800, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_zipcar, interval=86400, result_ttl=172800, repeat=None)


def stop_tasks():
    for x in scheduler.get_jobs():
        scheduler.cancel(x)

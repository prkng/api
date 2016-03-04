from prkng.database import db
from prkng.utils import random_string

import redis


def schedule_notifications(device_ids, message):
    """
    Schedule push notifications for devices via Redis/Task.

    :param device_ids: list of Amazon SNS keys to send message to (str)
    :param message: message to send to said users (str/unicode)
    :returns: None
    """
    pid = random_string(16)
    if not db.redis:
        db.redis = redis.Redis(db=1)
    db.redis.hset('prkng:push', pid, message)
    db.redis.rpush('prkng:push:'+pid, *device_ids)

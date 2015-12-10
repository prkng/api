from prkng.database import db
from prkng.utils import random_string

import apns
import json
import os
import time


def send_apple_notification(device_id, text):
    """
    Send push notification to iPhone users via Apple Push Notification Service (APNs)
    """
    ourdir = os.path.join(os.path.dirname(os.environ["PRKNG_SETTINGS"]), '{}')
    conn = apns.APNs(cert_file=ourdir.format('push_cert.pem'),
        key_file=ourdir.format('push_key.pem'))
    payload = apns.Payload(alert=text, sound="default")
    conn.gateway_server.send_notification(device_id, payload)


def get_apple_notification_failures():
    """
    Check for failed notification delivery attempts due to unregistered iOS device IDs.
    """
    ourdir = os.path.join(os.path.dirname(os.environ["PRKNG_SETTINGS"]), '{}')
    conn = apns.APNs(cert_file=ourdir.format('push_cert.pem'),
        key_file=ourdir.format('push_key.pem'))
    return conn.feedback_server.items()


def send_google_notification(token, text):
    """
    Send push notification to Android users via Google Cloud Messaging (GCM)
    """
    pass


def schedule_notifications(device_ids, message):
    """
    Schedule push notifications for devices via Redis/Task
    """
    pid = random_string(16)
    db.redis.hset('prkng:push', pid, message)
    db.redis.rpush('prkng:push:'+pid, *device_ids)

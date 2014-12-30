# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os


class Defaults(object):
    DEBUG = False
    TESTING = False
    LOG_LEVEL = 'info'
    # secret key is used for session handling (client side using cookies)
    SECRET_KEY = os.urandom(24)
    # database connection
    PG_HOST = 'localhost'
    PG_DATABASE = 'prkng'
    PG_PORT = '5432'
    PG_USERNAME = ''
    PG_PASSWORD = ''

    DOWNLOAD_DIRECTORY = '/tmp'


class Testing(Defaults):
    TESTING = True

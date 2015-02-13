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

    PG_TEST_HOST = 'localhost'
    PG_TEST_DATABASE = 'prkng_test'
    PG_TEST_PORT = '5432'
    PG_TEST_USERNAME = ''
    PG_TEST_PASSWORD = ''

    DOWNLOAD_DIRECTORY = '/tmp'

    OAUTH_CREDENTIALS = {
        'facebook': {
            'id': '1043720578978201',
            'secret': '21221bccfc052c95cb37a40d200bad35'
        },
        'google': {
            'id': '809052690526-8r6c2frl23212mlnkvf18094a278kld5.apps.googleusercontent.com',
            'secret': 'poPuwF4CdjJr7IM89Z3JjjT7'
        }
    }


class Testing(Defaults):
    TESTING = True

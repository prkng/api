# -*- coding: utf-8 -*-
from os.path import join, abspath, exists, dirname

from flask import Flask

from logger import Logger, set_level


__version__ = '1.0.2.dev'


def create_app(env='Defaults'):
    """
    Creates application.

    The configuration is loaded in the following steps:

        1. load Defaults config or that passed as ``env`` argument
        2. override with config file defined in environnement variable PRKNG_SETTINGS
        3. override with prkng.cfg if exists locally (in the root directory, dev purpose)

    :returns: flask application instance
    """
    app = Flask(__name__)
    app.config.from_object('prkng.settings.{env}'.format(env=env))
    app.config.from_envvar('PRKNG_SETTINGS', silent=True)

    custom_settings = join(join(dirname(abspath(__file__)), '..'), 'prkng.cfg')

    if exists(custom_settings):
        Logger.info("'prkng.cfg' found. Using it")
        app.config.from_pyfile(custom_settings)

    set_level(app.config['LOG_LEVEL'])

    return app

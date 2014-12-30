# -*- coding: utf-8 -*-
"""
Flask application callable
"""
from prkng import create_app
from api import init_api
from database import init_db
from logger import Logger

app = create_app()
init_api(app)
init_db(app)

Logger.debug(app.config)

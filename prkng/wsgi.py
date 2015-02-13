# -*- coding: utf-8 -*-
"""
Flask application callable
"""
from prkng import create_app
from api import init_api
from database import init_db
from models import init_model
from login import init_login
from logger import Logger

app = create_app()
init_db(app)
init_model(app)
init_api(app)
init_login(app)

Logger.debug(app.config)

# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

This module contains the WSGI application used by Flask development server
and any production WSGI deployments
"""
from prkng import create_app
from api import init_api
from database import init_db
from models import init_model
from login import init_login
from admin import init_admin
from logger import Logger

app = create_app()
init_db(app)
init_model(app)
init_api(app)
init_login(app)
init_admin(app)

Logger.debug(app.config)

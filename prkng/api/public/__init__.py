from prkng.models import User

from flask import g, request
from flask.ext.restplus import Api
from functools import wraps


HEADER_API_KEY = 'X-API-KEY'
authorizations = {
  'apikey': {
      'type': 'apiKey',
      'in': 'header',
      'name': 'X-API-KEY'
  }
}


class PrkngApi(Api):
    """
    Subclass Api and adds a ``secure`` decorator
    """
    def __init__(self, **kwargs):
        super(PrkngApi, self).__init__(**kwargs)

    def secure(self, func):
        '''Enforce authentication'''
        @wraps(func)
        def wrapper(*args, **kwargs):
            apikey = request.headers.get(HEADER_API_KEY)
            if not apikey:
                return 'Invalid API Key', 401

            g.user = User.get_byapikey(apikey)
            if not g.user:
                return 'Invalid API Key', 401

            return func(*args, **kwargs)

        return wrapper

# api instance (Blueprint)
api = PrkngApi(
    version='1.0',
    title='Prkng Public API',
    description='On-street parking information API',
    authorizations=authorizations
)

def init_api(app):
    """
    Initialize extensions into flask application
    """
    if not app.config["DEBUG"]:
        api._doc = False
    api.init_app(app)

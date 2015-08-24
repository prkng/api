from prkng.models import User

from flask import g, current_app, request
from flask.ext.restplus import Api
from functools import wraps
from itsdangerous import TimedJSONWebSignatureSerializer, SignatureExpired, BadSignature
import time


HEADER_API_KEY = 'X-API-KEY'


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
    default='v0'
)

def init_api(app):
    """
    Initialize extensions into flask application
    """
    api.ui = app.config["DEBUG"]
    api.init_app(app)


def auth_required():
    def wrapper(func):
        @wraps(func)
        def decorator(*args, **kwargs):
            v = verify()
            if v:
                return v
            return func(*args, **kwargs)
        return decorator
    return wrapper


def create_token(user):
    iat = time.time()
    payload = {
        "iss": user,
        "iat": iat,
        "exp": iat + 21600
    }
    tjwss = TimedJSONWebSignatureSerializer(secret_key=current_app.config["SECRET_KEY"],
        expires_in=21600, algorithm_name="HS256")
    return tjwss.dumps(payload).decode("utf-8")


def verify():
    token = request.headers.get("Authorization", None)
    if not token:
        return "Authorization required", 401

    token = token.split()
    if token[0] != "Bearer" or len(token) > 2:
        return "Malformed token", 400
    token = token[1]

    try:
        tjwss = TimedJSONWebSignatureSerializer(secret_key=current_app.config["SECRET_KEY"],
            expires_in=21600, algorithm_name="HS256")
        payload = tjwss.loads(token)
    except SignatureExpired:
        return "Token expired", 401
    except BadSignature:
        return "Malformed token signature", 401

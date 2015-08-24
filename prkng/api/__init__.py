from flask import current_app, request
from functools import wraps
from itsdangerous import TimedJSONWebSignatureSerializer, SignatureExpired, BadSignature
import time


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

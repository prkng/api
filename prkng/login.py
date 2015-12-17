# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from prkng.models import User, UserAuth

from flask.ext.login import LoginManager, login_user
from flask import current_app
from passlib.hash import pbkdf2_sha256
import requests


# login Manager
lm = LoginManager()


def init_login(app):
    """
    Initialize login manager extension into flask application
    """
    lm.init_app(app)


@lm.user_loader
def load_user(id):
    return User.get(int(id))


def email_register(
        email=None, password=None, name=None, gender=None, birthyear=None,
        image_url=None):
    """
    Signup with an email and a password
    """
    email=email.lower()
    user = User.get_byemail(email)
    if user:
        return "User already exists", 404

    # primary user doesn't exists, creating it
    user = User.add_user(
        name=name,
        email=email,
        gender=gender,
        image_url=image_url
    )

    # add an authentification method
    auth_id = 'email${}'.format(user.id)
    UserAuth.add_userauth(
        user_id=user.id,
        name=name,
        auth_id=auth_id,
        email=email,
        auth_type='email',
        password=pbkdf2_sha256.encrypt(password, rounds=200, salt_size=16),
        fullprofile={'birthyear': birthyear}
    )

    # login user in the current session
    login_user(user, True)

    resp = {
        'auth_id': auth_id,
    }
    resp.update(user.json)

    return resp, 201


def email_update(
        user, email=None, password=None, name=None, gender=None, birthyear=None,
        image_url=None):
    """
    Update user profile with new information
    """
    user.update_profile(name, email, gender, image_url)
    auth_id = 'email${}'.format(user.id)
    ua = UserAuth.exists(auth_id)
    if ua and password:
        UserAuth.update_password(auth_id, password)
    if ua:
        UserAuth.update(auth_id, birthyear)

    resp = {
        'auth_id': auth_id,
    }
    resp.update(user.json)

    return resp, 200


def email_signin(email, password):
    """
    Signin with an email and a password
    """
    user = User.get_byemail(email)

    if not user:
        return "Account doesn't exists, please register", 401

    # check if authentication method by email exists for this user
    auth_id = 'email${}'.format(user.id)
    user_auth = UserAuth.exists(auth_id)
    if not user_auth:
        return "Existing user with google or facebook account, not email", 401

    # check password validity
    if not pbkdf2_sha256.verify(password, user_auth.password):
        return "Incorrect password", 401

    user.update_apikey(User.generate_apikey(user.email))

    resp = {
        'auth_id': auth_id,
    }
    resp.update(user.json)

    return resp, 200


def facebook_signin(access_token):
    """
    Authorize user given its access_token.
    Add it to the db if not already present
    """
    # verify access token has been requested with the correct app id
    resp = requests.get(
        "https://graph.facebook.com/app/",
        params={'access_token': access_token}
    )
    data = resp.json()

    if resp.status_code != 200:
        return data, resp.status_code

    if data['id'] != current_app.config['OAUTH_CREDENTIALS']['facebook']['id']:
        return "Authentication failed.", 401

    # get user profile
    resp = requests.get(
        "https://graph.facebook.com/me",
        params={'access_token': access_token}
    )
    me = resp.json()

    if resp.status_code != 200:
        return me, resp.status_code

    if 'email' not in me:
        return 'Email information not provided, cannot register user', 401

    # fetch current profile pic
    resp = requests.get(
        "https://graph.facebook.com/me/picture",
        params={'access_token': access_token, 'redirect': False, 'type': 'normal'}
    )
    pic = resp.json()["data"].get('url', '')

    # check if user exists with its email as unique identifier
    user = User.get_byemail(me['email'])
    if not user:
        # primary user doesn't exists, creating it
        user = User.add_user(
            name=me['name'],
            email=me['email'],
            gender=me.get('gender', None),
            image_url=pic)
    else:
        # if already exists just update with a new apikey and profile pic
        user.update_apikey(User.generate_apikey(user.email))
        user.update_profile(image_url=pic)
    # known facebook account ?
    auth_id = 'facebook${}'.format(me['id'])
    user_auth = UserAuth.exists(auth_id)

    if not user_auth:
        # add user auth informations
        UserAuth.add_userauth(
            user_id=user.id,
            name=user.name,
            auth_id=auth_id,
            email=user.email,
            auth_type='facebook',
            fullprofile=me
        )

    # login user (powered by flask-login)
    login_user(user, True)

    resp = {
        'auth_id': auth_id,
    }
    resp.update(user.json)

    return resp, 200


def google_signin(access_token, name, email, picture):
    """
    Authorize user given its access_token.
    Add it to the db if not already present

    """
    # verify access token has been requested with the correct app id
    if access_token.startswith("eyJh"):
        # Google Sign-In (1.3.1+)
        resp = requests.get(
            "https://www.googleapis.com/oauth2/v3/tokeninfo",
            params={'id_token': access_token}
        )
        data = resp.json()
        if resp.status_code != 200:
            return data, resp.status_code

        if data['aud'] not in [current_app.config['OAUTH_CREDENTIALS']['google']['ios_id'],
                current_app.config['OAUTH_CREDENTIALS']['google']['android_id']]:
            return "Authentication failed.", 401

        id = data['sub']
        me = {"name": name, "email": email}
    else:
        # Google OAuth 2.0 (1.3 and below)
        resp = requests.get(
            "https://www.googleapis.com/oauth2/v1/tokeninfo",
            params={'access_token': access_token}
        )
        data = resp.json()
        if resp.status_code != 200:
            return data, resp.status_code

        if data['audience'] != current_app.config['OAUTH_CREDENTIALS']['google']['id']:
            return "Authentication failed.", 401

        # get user profile
        resp = requests.get(
            "https://www.googleapis.com/oauth2/v1/userinfo",
            params={'access_token': access_token}
        )

        me = resp.json()
        if resp.status_code != 200:
            return me, resp.status_code

        if 'email' not in me:
            return 'Email information not provided, cannot register user', 401

        id, email, name, picture = me['id'], me['email'], me['name'], me.get('picture', '')

    auth_id = 'google${}'.format(id)

    # known google account ?
    user_auth = UserAuth.exists(auth_id)

    # check if user exists with its email as unique identifier
    user = User.get_byemail(email)
    if not user:
        # primary user doesn't exists, creating it
        user = User.add_user(
            name=name,
            email=email,
            gender=None,
            image_url=picture)

    if not user_auth:
        # add user auth informations
        UserAuth.add_userauth(
            user_id=user.id,
            name=user.name,
            auth_id=auth_id,
            email=user.email,
            auth_type='google',
            fullprofile=me
        )
    else:
        # if already exists just update with a new apikey and profile details
        user.update_apikey(User.generate_apikey(user.email))
        user.update_profile(name=name, email=email, image_url=picture)

    # login user (powered by flask-login)
    login_user(user, True)

    resp = {
        'auth_id': auth_id,
    }
    resp.update(user.json)

    return resp, 200

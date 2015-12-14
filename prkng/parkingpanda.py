from flask import current_app

import requests


BASE_URL = "http://dev.parkingpanda.com/api/v2"


def login(username, password):
    """
    Login to Parking Panda API with a given username and password.
    Returns the user's ID and 'API password' for use with future requests.
    """
    data = requests.get(BASE_URL + "/users", params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        auth=requests.auth.HTTPBasicAuth(username, password))
    if data.json()["success"] == False:
        return False
    data = data.json()["data"]
    return (data["id"], data["apiPassword"])

def create_user(email, password, first_name, last_name, phone,
        inv_code="", sms_notif=False, welcome_mail=False):
    """
    Create a user for use with the Parking Panda API.
    Returns the user's ID and 'API password' for use with future requests.
    """
    data = requests.post(BASE_URL + "/users", params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        json={"email": email, "password": password, "firstName": first_name, "lastName": last_name,
        "phone": phone, "invitationCodeForSignup": inv_code, "receiveSMSNotifications": sms_notif,
        "dontSendWelcomeEmail": welcome_mail == False},
        auth=requests.auth.HTTPBasicAuth("admin", "admin"))
    if data.json()["success"] == False:
        return False
    data = data.json()["data"]
    return (data["id"], data["apiPassword"])

def get_credit_cards(user_id, email, api_password):
    """
    Obtain a user's saved credit cards (with some details masked for security).
    Returns an array of card information objects.
    """
    data = requests.get(BASE_URL + "/users/" + str(user_id) + "/credit-cards",
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return data.json()["data"]

def add_credit_card(user_id, email, api_password, card_number, card_name, card_expiry,
        card_cvv, card_postal, default=True):
    """
    Add a credit card to the user's store of saved cards.
    Returns a Parking Panda card information object (with some details masked for security).
    """
    data = requests.post(BASE_URL + "/users/" + str(user_id) + "/credit-cards",
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        json={"cardholderName": card_name, "creditCardNumber": card_number,
        "cvv": card_cvv, "makeDefault": default, "billingPostal": card_postal,
        "expirationDate": card_expiry},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return data.json()["data"]

def update_credit_card(user_id, email, api_password, card_token, card_number, card_name, card_expiry,
        card_cvv, card_postal, default=True):
    """
    Updates a credit card in the user's store of saved cards.
    Returns a Parking Panda card information object (with some details masked for security).
    """
    data = requests.put(BASE_URL + "/users/" + str(user_id) + "/credit-cards/" + str(card_token),
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        json={"cardholderName": card_name, "creditCardNumber": card_number,
        "cvv": card_cvv, "makeDefault": default, "billingPostal": card_postal,
        "expirationDate": card_expiry},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return data.json()["data"]

def delete_credit_card(user_id, email, api_password, card_token):
    """
    Deletes a credit card from the user's store of saved cards.
    Returns True if the card was successfully deleted.
    """
    data = requests.delete(BASE_URL + "/users/" + str(user_id) + "/credit-cards/" + str(card_token),
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return True

def create_reservation(user_id, email, api_password, loc_id, start, end, card_token,
        veh_desc=None, park_option="hourly", code=None):
    """
    Create a reservation at a Parking Panda lot, and purchase with provided details.
    Start and End times must be datetime objects.
    Returns reservation information objects.
    """
    data = requests.post(BASE_URL + "/users/" + str(user_id) + "/transactions",
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        json={"idPackagePlan": None, "code": code, "dontUseCredits": False,
        "eventId": None, "parkingOption": park_option, "paymentMethodToken": card_token,
        "upsellIds": None, "vehicleDescription": veh_desc, "idLocation": loc_id,
        "endDateAndTime": end.isoformat(), "startDateAndTime": start.isoformat()},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return data.json()["data"]

def get_reservations(user_id, email, api_password, filt=None):
    """
    Get a list of ALL reservations this user has made.
    Returns reservation information objects.
    """
    data = requests.get(BASE_URL + "/users/" + str(user_id) + "/transactions" + (("/"+filt) if filt else ""),
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return data.json()["data"]

def get_past_reservations(user_id, email, api_password):
    """
    Get a list of PAST reservations this user has made.
    Returns reservation information objects.
    """
    return get_reservations(user_id, email, api_password, "past")

def get_upcoming_reservations(user_id, email, api_password):
    """
    Get a list of UPCOMING reservations this user has made.
    Returns reservation information objects.
    """
    return get_reservations(user_id, email, api_password, "upcoming")

def get_reservation(user_id, email, api_password, confirmation):
    """
    Get a particular reservation for this user by its confirmation ID.
    Returns reservation information objects.
    """
    return get_reservations(user_id, email, api_password, confirmation)

def open_gate(user_id, email, api_password, res_id, direction, gate_id=1):
    """
    Sends an 'open gate' command for a particular user and reservation.
    Direction: 1 for in, 2 for out
    Returns True if the call was successful.
    """
    data = requests.post(BASE_URL + "/gates/vend-request",
        params={"apikey": current_app.config["PARKINGPANDA_PUBLIC_KEY"]},
        json={"idUser": user_id, "idTransaction": res_id, "idGate": gate_id,
        "direction": direction},
        auth=requests.auth.HTTPBasicAuth(email, api_password))
    if data.json()["success"] == False:
        return False
    return data.json()["data"]["isSuccessful"]

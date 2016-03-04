# -*- coding: utf-8 -*-
"""
:author: Jacob Cook <jacob@prk.ng>

Utility functions for occasional use
"""

from __future__ import print_function

import aniso8601
import hashlib
import random


def timestamp(x):
    """
    Parse out milliseconds and timezones from an ISO-8601 timestamp.

    :param x: ISO-8601 timestamp (str)
    :returns: ISO-8601 timestamp (str)
    """
    return aniso8601.parse_datetime(x).isoformat(str('T'))

def can_be_int(data):
    """
    Simply tells you if an item (string, etc) could potentially be an integer.

    :param data: string
    :returns: True if param can be an integer (bool)
    """
    try:
        int(data)
        return True
    except ValueError:
        return False

def random_string(length=40):
    """
    Create a randomish alphanumeric string.

    :param length: length of the string to generate, default 40 (int)
    :returns: randomish string (str)
    """
    return hashlib.sha1(str(random.random())).hexdigest()[0:length]

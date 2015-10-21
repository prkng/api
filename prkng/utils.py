# -*- coding: utf-8 -*-
from __future__ import print_function

import aniso8601
import hashlib
import random


def timestamp(x):
    return aniso8601.parse_datetime(x).isoformat(str('T'))

def can_be_int(data):
    """
    Simply tells you if an item (string, etc) could potentially be an integer.
    """
    try:
        int(data)
        return True
    except ValueError:
        return False

def random_string(length=40):
    """Create a random alphanumeric string."""
    return hashlib.sha1(str(random.random())).hexdigest()[0:length]

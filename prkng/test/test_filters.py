# -*- coding: utf-8 -*-
from collections import namedtuple

import pytest

from ..filters import on_restriction, period_matching


def test_on_restrictions_with_period():
    rule_view = [
        {
            "periods": [["04-01","12-01"]],
            "time_max_parking": None,
            'agenda': {
                '1': [[9.5, 10.5]], '3': [],
                '2': [], '5': [],
                '4': [], '7': [],
                '6': []}
        },
        {
            "periods": [],
            "time_max_parking": None,
            'agenda': {
                '1': [[9.0, 23.0]], '3': [[9.0, 23.0]],
                '2': [[9.0, 23.0]], '5': [[9.0, 23.0]],
                '4': [[9.0, 23.0]], '7': [[9.0, 23.0]],
                '6': [[9.0, 23.0]]}
        }
    ]
    assert on_restriction(rule_view, '2015-04-07T09:30', 1) == True
    assert on_restriction(rule_view, '2015-02-09T08:00', 1) == False


def test_on_restrictions_period_maxparking():
    rule_view = [
        {
            "periods": [["04-01","12-01"]],
            "time_max_parking": 120,
            'agenda': {
                '1': [[0.0, 24.0]], '3': [[0.0, 24.0]],
                '2': [[0.0, 24.0]], '5': [[0.0, 24.0]],
                '4': [[0.0, 24.0]], '7': [[0.0, 24.0]],
                '6': [[0.0, 24.0]]}
        }
    ]
    assert on_restriction(rule_view, '2015-04-07T09:30', 3) == True
    assert on_restriction(rule_view, '2015-02-09T08:00', 2) == False


def test_on_restrictions_inverted_period():
    rule_view = [
        {
            "periods": [["12-01","04-01"]],
            "time_max_parking": None,
            'agenda': {
                '1': [[10.0, 18.0]], '3': [[10.0, 18.0]],
                '2': [], '5': [],
                '4': [], '7': [],
                '6': []}
        }
    ]
    assert on_restriction(rule_view, '2015-04-07T09:30', 3) == False
    assert on_restriction(rule_view, '2015-02-09T08:30', 2) == True


def test_on_restrictions_largeparkingtime():
    rule_view = [
        {
            "periods": [],
            "time_max_parking": None,
            'agenda': {
                '1': [[18.0, 20.0]], '3': [],
                '2': [], '5': [],
                '4': [], '7': [],
                '6': []}
        }
    ]
    assert on_restriction(rule_view, '2015-02-09T21:30', 168) == False  # monday


def test_on_restrictions_multiplerangeaday():
    rule_view = [
        {
            "periods": [],
            "time_max_parking": None,
            'agenda': {
                '1': [[5, 10], [18.0, 20.0]], '3': [],
                '2': [], '5': [],
                '4': [], '7': [],
                '6': []}
        }
    ]
    assert on_restriction(rule_view, '2015-02-09T06:30', 3) == True  # monday
    assert on_restriction(rule_view, '2015-02-09T10:00', 3) == False  # monday


def test_on_restrictions_flexible():
    rule_view = [
        {
            'restrict_types': [],
            'code': 'AV-AB',
            'description': 'A 08h-09h30 LUN. AU VEN.',
            'time_max_parking': None,
            'periods': [],
            'agenda': {'1': [[8.0, 9.5]],
                       '3': [[8.0, 9.5]],
                       '2': [[8.0, 9.5]],
                       '5': [[8.0, 9.5]],
                       '4': [[8.0, 9.5]],
                       '7': [], '6': []},
            'special_days': None
        },
        {
            'restrict_types': ['maintenance'],
            'code': 'EU-TF+F',
            'description': 'P ENTRETIEN (ORANGE) 07h-19h ou 19h-07h (flexible)',
            'time_max_parking': None,
            'periods': [],
            'agenda': {'1': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]],
                       '3': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]],
                       '2': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]],
                       '5': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]],
                       '4': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]],
                       '7': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]],
                       '6': [[7.0, 19.0], [19.0, 24.0], [0, 7.0]]},
            'special_days': None
        }]

    assert on_restriction(rule_view, '2015-04-27T09:30', 1) == True
    assert on_restriction(rule_view, '2015-04-28T08:30', 1) == True


def test_on_restrictions_permit():
    rule_view = [
        {
            'restrict_types': [],
            'code': 'AV-AB',
            'description': 'A 08h-09h30 LUN. AU VEN.',
            'time_max_parking': None,
            'periods': [],
            'agenda': {'1': [[8.0, 9.5]],
                       '3': [[8.0, 9.5]],
                       '2': [[8.0, 9.5]],
                       '5': [[8.0, 9.5]],
                       '4': [[8.0, 9.5]],
                       '7': [], '6': []},
            'special_days': None
        },
        {
            'restrict_types': ['permit'],
            'permit_no': 151,
            'code': 'R-PF',
            'description': 'P RESERVE S3R 09h-23h',
            'time_max_parking': None,
            'periods': [],
            'agenda': {'1': [[9.0, 23.0]],
                       '2': [[9.0, 23.0]],
                       '3': [[9.0, 23.0]],
                       '4': [[9.0, 23.0]],
                       '5': [[9.0, 23.0]],
                       '6': [[9.0, 23.0]],
                       '7': [[9.0, 23.0]]},
            'special_days': None
        }
    ]

    assert on_restriction(rule_view, '2015-02-09T12:00', 1, 151) == False
    assert on_restriction(rule_view, '2015-02-09T08:15', 1, 151) == True
    assert on_restriction(rule_view, '2015-02-09T12:00', 1) == True


def test_period_matching():
    assert period_matching(1, 1, 1, 4, 1, 4) == True
    assert period_matching(1, 1, 1, 4, 1, 1) == True
    assert period_matching(1, 1, 1, 4, 10, 4) == False
    assert period_matching(1, 12, 1, 4, 1, 4) == True
    assert period_matching(1, 12, 1, 4, 2, 4) == False
    assert period_matching(1, 4, 1, 12, 1, 2) == False
    assert period_matching(1, 3, 1, 1, 1, 2) == False

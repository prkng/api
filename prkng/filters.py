# -*- coding: utf-8 -*-
from itertools import chain
from datetime import timedelta, time, datetime
from aniso8601 import parse_datetime, parse_time


def on_restriction(rules, checkin, duration):
    """
    Returns True if restrictions are consistent with the checkin
    and duration given in argument. False otherwise

    :param rules: list of rules (dict)
    :param checkin: checkin time
    :param duration: duration in hour. Float accepted
    """
    checkin = parse_datetime(checkin)
    duration = timedelta(hours=duration)
    checkin_end = checkin + duration  # datetime

    month = checkin.date().month  # month as number
    isodow = checkin.isoweekday()  # 1->7
    year = checkin.year  # 2015
    day = checkin.strftime('%d')  # 07

    # analyze each rule and stop iteration on conflict
    for rule in rules:

        # first test season day/month
        start_day, start_month = ('-' or rule['season_start']).split('-')
        end_day, end_month = ('-' or rule['season_end']).split('-')
        season_match = season_matching(
            start_day,
            start_month,
            end_day,
            end_month,
            day,
            month
        )

        if not season_match:
            # not concerned, going to the next rule
            continue

        max_time_ok = True
        time_range_ok = True

        # analyze time_max_parking
        time_max_parking = timedelta(minutes=rule['time_max_parking'] or 3679200)
        if duration > time_max_parking:
            max_time_ok &= False

        # analyze time ranges
        # extract range time for each day and test overlapping with checkin + duration
        # start at current day and slice over days
        iterto = chain(range(1, 8)[isodow-1:], range(1, 8)[:isodow-1])

        for absoluteday, numday in enumerate(iterto):
            tsranges = rule['agenda'][str(numday)]
            for start, stop in filter(bool, tsranges):

                start_time = datetime(
                    year, month, int(day)+absoluteday,
                    hour=int(start), minute=int(start % 1 * 60))

                stop_time = datetime(
                    year, month, int(day)+absoluteday,
                    hour=int(stop-1), minute=int(stop % 1 * 60)) + timedelta(hours=1)
                    #  hack to avoid ValueError: hour must be in 0..23

                if max(start_time, checkin) < min(stop_time, checkin_end):
                    # overlapping !
                    time_range_ok &= False

            if not max_time_ok or not time_range_ok:
                # max_time exceed or time range overlapping or both
                return True

    return False


def season_matching(start_day, start_month, end_day, end_month, day, month):
    if not start_month:
        # no season restriction so matching ok
        return True

    if start_month < end_month and not (month >= start_month and month <= end_month):
        # month out of range
        return False
    if start_month > end_month and (month < start_month and month > end_month):
        # month out of range
        return False

    if month == start_month:
        if day < start_day:
            return False

    if month == end_month:
        if day > end_day:
            return False

    return True

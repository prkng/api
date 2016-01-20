# -*- coding: utf-8 -*-
from itertools import chain
from datetime import timedelta, datetime
from aniso8601 import parse_datetime


def on_restriction(slot, checkin, duration, paid=True, permit=False):
    """
    Process rules for display to client. Returns rule(s) if restrictions are compatible with the checkin
    and duration given in argument. False otherwise.

    :param rules: list of rules (dict)
    :param checkin: checkin time
    :param duration: duration in hour. Float accepted
    :param paid: set to False to not return any paid slots.
    :param permit: return permit slots matching this name/number (str), 'all', or False for none
    """
    checkin = parse_datetime(checkin)
    duration = timedelta(hours=duration)
    checkin_end = checkin + duration  # datetime

    month = checkin.date().month  # month as number
    isodow = checkin.isoweekday()  # 1->7
    year = checkin.year  # 2015
    day = checkin.strftime('%d')  # 07

    slot['restrict_types'] = []

    # add any applicable temporary restrictions into the main rules list
    if slot['temporary_rule']:
        slot["rules"].append(slot["temporary_rule"])

    # analyze each rule: leave it alone if it is not currently restricted, return False if it is
    for rule in slot["rules"]:
        if "paid" in rule['restrict_types'] and not paid:
            # don't show me paid slots
            return False
        elif any(x in ['angled'] for x in rule['restrict_types']):
            # not concerned, going to the next rule
            continue

        # first test season day/month
        start_month, start_day = "", ""
        if rule['season_start']:
            start_month, start_day = [int(x) for x in rule['season_start'].split('-')]
        end_month, end_day = "", ""
        if rule['season_end']:
            end_month, end_day = [int(x) for x in rule['season_end'].split('-')]
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

        # extract time range for each day and test overlapping with checkin + duration
        # start at current day and slice over days
        iterto = chain(range(1, 8)[isodow-1:], range(1, 8)[:isodow-1])

        for absoluteday, numday in enumerate(iterto):
            tsranges = rule['agenda'][str(numday)]
            for start, stop in filter(bool, tsranges):

                try:
                    start_time = datetime(year, month, int(day), hour=int(start), minute=int(start % 1 * 60)) \
                        + timedelta(days=absoluteday)
                    #  hack to avoid ValueError: hour must be in 0..23
                    stop_time = datetime(year, month, int(day), hour=int(stop-1), minute=int(stop % 1 * 60)) \
                        + timedelta(days=absoluteday, hours=1)
                except TypeError:
                    raise Exception("Data integrity error on {}, please review rules".format(rule['code']))
                except Exception, e:
                    raise Exception("Exception occurred on {} :  {}".format(rule['code'], str(e)))

                if "paid" in rule["restrict_types"] and checkin >= start_time and checkin <= stop_time:
                    slot["restrict_types"] = ["paid"]

                if "permit" in rule['restrict_types'] and (permit == 'all' or str(rule.get('permit_no')) in str(permit).split(",")):
                    # this is a permit rule and we like permits
                    continue
                elif "permit" not in rule['restrict_types'] and "paid" in rule["restrict_types"]:
                    continue

                if (max(start_time, checkin) < min(stop_time, checkin_end) and rule['time_max_parking'] == None):
                    # overlapping !
                    time_range_ok &= False

                # FIXME when/if we do custom duration values
                # make sure permit/paid are still taken into account
                if (max(start_time, checkin) < min(stop_time, checkin_end) and rule['time_max_parking'] <> None):
                    # overlapping BUT a time_max_parking is allowed!
                    #if checkin_end time is after the stop time
                    if duration > time_max_parking and (checkin_end > stop_time):
                        #extract duration inside
                        duration_in_checkin_range = (stop_time - checkin)
                        if (duration_in_checkin_range > time_max_parking):
                            max_time_ok &= False

                    #if checkin time is before the start time
                    if duration > time_max_parking and (checkin < start_time):
                        #extract duration inside
                        duration_in_checkin_range = (checkin_end - start_time)
                        if (duration_in_checkin_range > time_max_parking):
                            max_time_ok &= False

                    if (checkin >= start_time and checkin_end <= stop_time and duration > time_max_parking):
                        # parking time is totally inside range BUT duration too long
                        max_time_ok &= False

                    if "permit" in rule["restrict_types"]:
                        # everything OK but it's a permit rule, and we haven't skipped it yet, so...
                        max_time_ok &= False

            if not max_time_ok or not time_range_ok:
                # max_time exceed or time range overlapping or both
                return False

    return slot


def add_temporary_restrictions(slot):
    if slot['temporary_rule']:
        slot["rules"].append(slot["temporary_rule"])
    return slot


def remove_not_applicable(slot, checkin, permit=False):
    checkin = parse_datetime(checkin)
    month = checkin.date().month  # month as number
    day = checkin.strftime('%d')  # 07

    for rule in slot.rules:
        # first test season day/month
        start_month, start_day = "", ""
        if rule['season_start']:
            start_month, start_day = [int(x) for x in rule['season_start'].split('-')]
        end_month, end_day = "", ""
        if rule['season_end']:
            end_month, end_day = [int(x) for x in rule['season_end'].split('-')]
        season_match = season_matching(
            start_day,
            start_month,
            end_day,
            end_month,
            day,
            month
        )

        if not season_match:
            slot.rules.remove(rule)
        elif "permit" in rule['restrict_types'] and (permit == 'all' or str(rule.get('permit_no')) in str(permit).split(",")):
            slot.rules.remove(rule)
    return slot


def season_matching(start_day, start_month, end_day, end_month,
                    day, month):
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

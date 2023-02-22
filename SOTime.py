from datetime import datetime, timedelta


def round_time(dt=None, date_delta=timedelta(minutes=1), to="average"):
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    round_to = date_delta.total_seconds()
    if dt is None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == "up":
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (
                (seconds + dt.microsecond / 1000000 + round_to) // round_to * round_to
            )
        elif to == "down":
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + timedelta(0, rounding - seconds, -dt.microsecond)

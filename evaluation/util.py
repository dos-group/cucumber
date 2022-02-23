from datetime import timedelta, datetime

from config import START_DATE, TIME_STEP


def timedelta_to_simtimedelta(td: timedelta) -> float:
    return td.total_seconds() / (60 * TIME_STEP)


def simtime_to_datetime(st: float) -> datetime:
    return START_DATE + timedelta(minutes=TIME_STEP * st)


def datetime_to_simtime(dt: datetime) -> float:
    return timedelta_to_simtimedelta(dt - START_DATE)


def round_down_dt(dt: datetime):
    """Rounds datetime down to last time step."""
    return dt - timedelta(minutes=dt.minute % TIME_STEP, seconds=dt.second, microseconds=dt.microsecond)


def round_up_dt(dt: datetime):
    """Rounds datetime up to next time step."""
    return round_down_dt(dt) + timedelta(minutes=TIME_STEP)

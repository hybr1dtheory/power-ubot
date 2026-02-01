"""Module for additional functions (data processing, transforming, etc)"""
__all__ = ["split_by_day", "parse_message", "timedelta_to_str", "BaseRow"]
from datetime import datetime, timedelta
import re
from dataclasses import dataclass


PATTERNS = {
    "ON": {
        "time": re.compile(r"(?P<time>\d{2}:\d{2}) Світло з'явилося"),
        "duration": re.compile(r"🕓 Його не було (?:(?P<h>\d{1,2})год )?(?P<m>\d{1,2})хв"),
    },
    "OFF": {
        "time": re.compile(r"(?P<time>\d{2}:\d{2}) Світло зникло"),
    },
}


@dataclass(frozen=True)
class BaseRow:
    channel: str
    oblast: str
    raion: str
    start: datetime
    end: datetime


def parse_message(text: str) -> dict | None:
    """Function uses regular expressions to filter the necessary events. 
    Therefore, if the post template changes, you will need to change the patterns"""
    for event_type, rules in PATTERNS.items():
        m_time = rules["time"].search(text)
        if not m_time:
            continue
        result = {
            "type": event_type,
            "time": m_time.group("time")
        }
        if event_type == "ON":
            m_dur = rules["duration"].search(text)
            if not m_dur:
                return None

            h = int(m_dur.group("h") or 0)
            m = int(m_dur.group("m") or 0)
            result["duration"] = timedelta(hours=h, minutes=m)

        return result

    return None



def timedelta_to_str(td: timedelta) -> str:
    """Converts timedelta to str in Power Query format D.HH:MM:SS"""
    total_seconds = int(td.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{days}.{hours:02}:{minutes:02}:{seconds:02}"


def split_by_day(row: BaseRow):
    """Generator that yields daily segments for a single outage event.
    Returned values are dicts with keys StartDatetime, EndDatetime, Duration, Raion, Channel"""
    end_dt = row.end
    start_dt = row.start
    first_day_start = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    last_day_start = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    days_count = (last_day_start - first_day_start).days + 1
    for i in range(days_count):
        day = first_day_start + timedelta(days=i)
        next_day = day + timedelta(days=1)
        seg_start = max(start_dt, day)
        seg_end = min(end_dt, next_day)
        if seg_end > seg_start:
            yield {
                "StartDatetime": seg_start,
                "EndDatetime": seg_end,
                "Duration": timedelta_to_str(seg_end - seg_start),
                "Oblast": row.oblast,
                "Raion": row.raion,
                "Channel": row.channel,
            }

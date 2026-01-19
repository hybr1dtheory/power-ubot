"""Module for additional functions (data processing, transforming, etc)"""
from datetime import datetime, timedelta


def timedelta_to_str(td) -> str:
    """Converts pandas timedelta to str in Power Query format D.HH:MM:SS"""
    total_seconds = int(td.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{days}.{hours:02}:{minutes:02}:{seconds:02}"


def split_by_day(row: dict):
    """Generator that yields daily segments for a single outage event.
    Required keys in row:
      - EndDatetime (datetime)
      - Duration (timedelta)
      - Oblast, Raion, Channel (text)"""
    end_dt = row["EndDatetime"]
    start_dt = end_dt - row["Duration"]
    day_start = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_limit = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    days_count = (day_end_limit - day_start).days + 1
    for i in range(days_count):
        day = day_start + timedelta(days=i)
        next_day = day + timedelta(days=1)
        seg_start = max(start_dt, day)
        seg_end = min(end_dt, next_day)
        if seg_end > seg_start:
            yield {
                "StartDatetime": seg_start,
                "EndDatetime": seg_end,
                "Duration": timedelta_to_str(seg_end - seg_start),
                "Oblast": row["Oblast"],
                "Raion": row["Raion"],
                "Channel": row["Channel"],
            }

from datetime import datetime
from helpers import split_by_day, BaseRow


def test_split_by_day_cross_midnight():
    row = BaseRow(
        "X", "Y", "Z",
        datetime(2026, 1, 1, 22, 0),
        datetime(2026, 1, 2, 2, 0),
    )
    segments = list(split_by_day(row))

    assert len(segments) == 2
    assert segments[0]["StartDatetime"] == datetime(2026, 1, 1, 22, 0)
    assert segments[0]["EndDatetime"] == datetime(2026, 1, 2, 0, 0)
    assert segments[1]["StartDatetime"] == datetime(2026, 1, 2, 0, 0)
    assert segments[1]["EndDatetime"] == datetime(2026, 1, 2, 2, 0)


def test_split_by_day_full_day():
    row = row = BaseRow(
        "X", "Y", "Z",
        datetime(2026, 1, 1, 0, 0),
        datetime(2026, 1, 2, 0, 0),
    )
    segments = list(split_by_day(row))

    assert len(segments) == 1
    assert segments[0]["Duration"] == "1.00:00:00"

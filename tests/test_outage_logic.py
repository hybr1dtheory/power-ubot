from datetime import datetime, timedelta
from helpers import BaseRow, split_by_day, timedelta_to_str


def test_outage_duration_matches_off_on_interval():
    start = datetime(2026, 1, 20, 10, 0)
    end = datetime(2026, 1, 20, 12, 30)

    row = BaseRow(
        channel="test",
        oblast="X",
        raion="Y",
        start=start,
        end=end
    )
    rows = list(split_by_day(row))

    assert len(rows) == 1
    assert rows[0]["Duration"] == timedelta_to_str(timedelta(hours=2, minutes=30))

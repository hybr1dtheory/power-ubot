from datetime import datetime
import json


def set_last_date(dt: datetime):
    with open("lastdate.json", 'w') as fw:
        lst = {"lastdate": f"{dt.date()}"}
        json.dump(lst, fw)


lst: dict
with open("lastdate.json", 'r') as fr:
    lst = json.load(fr)
START_DATE = datetime.fromisoformat(lst["lastdate"])
FINISH_DATE = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

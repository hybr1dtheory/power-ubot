"""This module runs a loop iterating through the channel list 
and collects power-off and power-on event data. 
The bot does not handle real-time events, but only parses the channels history, 
processes and saves it to the powerdata.csv file, and then finishes its work"""
from pyrogram import Client
from helpers import datetime, timedelta, timedelta_to_str, split_by_day
from time import sleep
import re
import pandas as pd
from channels import CHANNELLIST
from datecfg import START_DATE, FINISH_DATE, set_last_date


app = Client("ubot")


async def get_events_from_chat(
        app_obj: Client, chatname: str, obl: str, rai: str,
        start_date: datetime, finish_date: datetime
    ) -> list[dict]:
    """Async function to get all ON events from channel in a specified period.
    Function uses regular expressions to filter the necessary events. 
    Therefore, if the post template changes, you will need to change the patterns"""
    pattern1 = r"\d\d:\d\d Світло з'явилося"
    pattern2 = r"🕓 Його не було (\d{1,2}год )?\d{1,2}хв"
    chat_data = []
    async for message in app_obj.get_chat_history(chatname):
        if message.date > finish_date:
            continue
        if message.date < start_date:
            break
        if not message.text:
            continue
        m1 = re.search(pattern1, message.text)
        m2 = re.search(pattern2, message.text)
        if m1 and m2:
            event = m1.group(0).split()
            durat = m2.group(0)
            hh = re.search(r"\d{1,2}год", durat)
            mm = re.search(r"\d{1,2}хв", durat)
            hrs = hh.group(0).replace("год", "") if hh else "0"
            mins = mm.group(0).replace("хв", "") if mm else "0"
            d = message.date.date()
            dur = timedelta(hours=int(hrs), minutes=int(mins))
            dt = datetime.fromisoformat(f"{d}T{event[0]}")
            row = {
                "EndDatetime": dt, "Oblast": obl, "Raion": rai,
                "Channel": chatname, "Duration": dur
            }
            chat_data.extend(split_by_day(row))
    return chat_data


async def main():
    data = []
    async with app:
        print("starting...")
        for chat in CHANNELLIST:
            print(f"Parsing from {chat}")
            chat_data = await get_events_from_chat(
                app, chat, 
                CHANNELLIST[chat]["oblast"],
                CHANNELLIST[chat]["raion"],
                START_DATE, FINISH_DATE
            )
            data.extend(chat_data)
            # wait 5 seconds after each request to prevent blocking of requests.
            # Error [420 FLOOD_WAIT_X]
            sleep(5)
    # Transforming data into a convenient format
    df = pd.DataFrame.from_records(data)
    df.sort_values(["StartDatetime"], inplace=True)
    # Writing data to a file in append mode and without headers
    with open("newpowerdata.csv", 'a', encoding="utf-8") as f:
        df.to_csv(f, header=False, index=False, encoding="utf-8-sig")
    set_last_date(FINISH_DATE)
    
    print("FINISH")


app.run(main())

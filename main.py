"""This module runs a loop iterating through the channel list 
and collects power-off and power-on event data. 
The bot does not handle real-time events, but only parses the channels history, 
processes and saves it to the powerdata.csv file, and then finishes its work"""
from pyrogram import Client
from asyncio import sleep
import pandas as pd
import logging
from channels import save_channels_data, channels
from datecfg import START_DATE, FINISH_DATE, set_last_date, datetime, timedelta
from helpers import timedelta_to_str, split_by_day, parse_message, BaseRow


app = Client("ubot")
logger = logging.getLogger(__name__)
logging.basicConfig(filename='logs.txt', encoding='utf-8', level=logging.INFO)


async def get_events_from_chat(
        app_obj: Client, chatname: str, chat_id: int, obl: str,
        rai: str, start_date: datetime, finish_date: datetime
    ) -> list[dict]:
    """Async function to get all ON events from channel in a specified period."""
    period_start = start_date
    period_end = finish_date - timedelta(microseconds=1)
    # Getting chat_id
    if not chat_id:
        chat = await app_obj.get_chat(chatname)
        chat_id = chat.id
        channels[chatname]["chat_id"] = chat_id
    # Getting all on/off events from chat history
    events: list[tuple[str, datetime, timedelta | None]] = []
    async for message in app_obj.get_chat_history(chat_id):
        if message.date > finish_date:
            continue
        if message.date < start_date:
            break
        if not message.text:
            continue
        # parsing text to find events
        parsed = parse_message(message.text)
        if not parsed:
            continue
        d = message.date.date()
        event_dt = datetime.fromisoformat(f"{d}T{parsed['time']}")
        if parsed["type"] == "ON":
            logger.info(
                f"\t\tON event detected at {event_dt}. Outage duration {parsed["duration"]}"
                f"\n\t\tMessage: {message.text}"
            )
            events.append(("ON", event_dt, parsed["duration"]))
        elif parsed["type"] == "OFF":
            logger.info(
                f"\t\tOFF event detected at {event_dt}."
                f"\n\t\tMessage: {message.text}"
            )
            events.append(("OFF", event_dt, None))
    # Important! Pyrogram gets messages in reverse order
    events.sort(key=lambda e: e[1])
    # Build intervals
    rows: list[BaseRow] = []
    open_off: datetime | None = None
    for ev_type, ev_time, ev_dur in events:
        if ev_type == "OFF":
            open_off = ev_time
        elif ev_type == "ON" and open_off:
            outage_start = open_off
            outage_end = ev_time
            # trim the segment if its start/end falls outside the period
            start_dt = max(outage_start, period_start)
            end_dt = min(outage_end, period_end)
            if end_dt > start_dt:
                rows.append(BaseRow(chatname, obl, rai, start_dt, end_dt))
            open_off = None
    # Tail of the period
    if open_off:
        start_dt = max(open_off, period_start)
        if period_end > start_dt:
            rows.append(BaseRow(chatname, obl, rai, start_dt, period_end))
    # Save current state
    if events:
        last_type, last_time, _ = events[-1]
        channels[chatname]["last_event_type"] = last_type.lower()
        channels[chatname]["last_event_time"] = last_time.isoformat()
    # split by day
    result = []
    for row in rows:
        result.extend(split_by_day(row))

    return result



async def main():
    logger.info(f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Started")
    data = []
    async with app:
        for ch_name, ch_data in channels.items():
            logger.info(f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Parsing from {ch_name}")
            print(f"Parsing from {ch_name}")
            chat_data = await get_events_from_chat(
                app, ch_name,
                ch_data.get("chat_id", None),
                ch_data["oblast"],
                ch_data["raion"],
                START_DATE, FINISH_DATE
            )
            data.extend(chat_data)
            logger.info(f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} End parsing for {ch_name}")
            # wait 5 seconds after each request to prevent blocking of requests.
            # Error [420 FLOOD_WAIT_X]
            await sleep(5)
    # Transforming data into a convenient format
    df = pd.DataFrame.from_records(data)
    df.sort_values(["StartDatetime"], inplace=True)
    # Writing data to a file in append mode and without headers
    with open("powerdata.csv", 'a', encoding="utf-8") as f:
        df.to_csv(f, header=False, index=False, encoding="utf-8-sig")
    set_last_date(FINISH_DATE)
    save_channels_data(channels)
    
    logger.info(f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Finished")


app.run(main())

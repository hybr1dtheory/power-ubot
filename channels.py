"""The list of channels to parse data"""
import json


def save_channels_data(chnls: dict):
    if all([isinstance(v["last_event_time"], (str, type(None))) for c, v in chnls.items()]):
        with open("channels.json", 'w') as fw:
            json.dump(chnls, fw, sort_keys=True, indent=4)
    else:
        for c, v in chnls.items():
            v["last_event_time"] = str(v["last_event_time"])
        with open("channels.json", 'w') as fw:
            json.dump(chnls, fw, sort_keys=True, indent=4)


def reset_open_outages(channels: dict):
    for name, data in channels.items():
        data["last_event_time"] = None
        data["last_event_type"] = None


channels: dict
with open("channels.json", 'r') as fr:
    channels = json.load(fr)

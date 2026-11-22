import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

# api settings from .env (defaults for local run)
API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
DAYS_BACK = int(os.getenv("USGS_DAYS_BACK", "30"))
MIN_MAGNITUDE = float(os.getenv("USGS_MIN_MAGNITUDE", "2.5"))
RAW_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "raw_data.csv"


# send request and retry up to 3 times on errors
def fetch_data(params, retries=3):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            print(f"Attempt {attempt}: status code {response.status_code}")
        except requests.RequestException as e:
            print(f"Attempt {attempt}: request error: {e}")
        time.sleep(2)
    return None


# geojson feature to flat dict
def parse_feature(feature):
    props = feature["properties"]
    lon, lat, depth = feature["geometry"]["coordinates"]
    return {
        "id": feature["id"],
        "mag": props.get("mag"),
        "mag_type": props.get("magType"),
        "place": props.get("place"),
        "time": props.get("time"),
        "updated": props.get("updated"),
        "type": props.get("type"),
        "status": props.get("status"),
        "alert": props.get("alert"),
        "tsunami": props.get("tsunami"),
        "sig": props.get("sig"),
        "net": props.get("net"),
        "latitude": lat,
        "longitude": lon,
        "depth": depth,
    }


def collect_records():
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DAYS_BACK)
    params = {
        "format": "geojson",
        "starttime": start.strftime("%Y-%m-%d"),
        "endtime": end.strftime("%Y-%m-%d"),
        "minmagnitude": MIN_MAGNITUDE,
        "orderby": "time",
    }
    print(f"Request: {API_URL} {params}")

    data = fetch_data(params)
    if data is None or "features" not in data:
        print("Could not get data from USGS API")
        return []

    records = []
    for feature in data["features"]:
        try:
            records.append(parse_feature(feature))
        except (KeyError, TypeError, ValueError) as e:
            print(f"Skipped bad feature: {e}")

    # save original data before cleaning
    RAW_DATA_PATH.parent.mkdir(exist_ok=True)
    pd.DataFrame(records).to_csv(RAW_DATA_PATH, index=False)
    print(f"Collected: {len(records)} records, saved to {RAW_DATA_PATH}")
    return records


if __name__ == "__main__":
    collect_records()
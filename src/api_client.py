"""Collect earthquake records from the USGS Earthquake Catalog API.

Endpoint: https://earthquake.usgs.gov/fdsnws/event/1/query
Docs:     https://earthquake.usgs.gov/fdsnws/event/1/

The original API response is saved unchanged to data/raw_response.json,
and the flattened records are saved to data/raw_data.csv before any cleaning.
"""
import csv
import json
import logging
import time

import requests

from src import config

logger = logging.getLogger(__name__)

RECORD_FIELDS = [
    "event_id", "magnitude", "mag_type", "place", "time_ms", "updated_ms",
    "latitude", "longitude", "depth_km", "event_type", "status", "tsunami",
    "significance", "alert", "network", "felt", "cdi", "mmi",
    "gap", "rms", "nst", "dmin", "title", "url",
]


class ApiError(Exception):
    """Raised when the API data cannot be collected."""


def build_params(start_time, end_time, min_magnitude, limit):
    """Query parameters for the USGS API."""
    return {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "minmagnitude": min_magnitude,
        "orderby": "time-asc",
        "limit": limit,
    }


def request_data(params):
    """Send the GET request and return the parsed JSON response.

    Retries on timeouts, connection errors and server errors (5xx, 429).
    Client errors (4xx) mean wrong parameters, so they are not retried.
    """
    for attempt in range(1, config.API_MAX_RETRIES + 1):
        try:
            response = requests.get(config.API_URL, params=params, timeout=config.API_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as error:
            logger.warning("Attempt %d/%d failed: %s", attempt, config.API_MAX_RETRIES, error)
        else:
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as error:
                    raise ApiError("API returned invalid JSON") from error
            if response.status_code == 429 or response.status_code >= 500:
                logger.warning("Attempt %d/%d: HTTP %d", attempt, config.API_MAX_RETRIES, response.status_code)
            else:
                raise ApiError(f"HTTP {response.status_code}: {response.text[:200]}")

        if attempt < config.API_MAX_RETRIES:
            time.sleep(2 ** attempt)

    raise ApiError(f"No successful response after {config.API_MAX_RETRIES} attempts")


def parse_feature(feature):
    """Convert one GeoJSON feature into a flat record. Values are kept as received."""
    props = feature.get("properties") or {}
    coordinates = (feature.get("geometry") or {}).get("coordinates") or []
    longitude, latitude, depth = (list(coordinates) + [None, None, None])[:3]

    return {
        "event_id": feature.get("id"),
        "magnitude": props.get("mag"),
        "mag_type": props.get("magType"),
        "place": props.get("place"),
        "time_ms": props.get("time"),
        "updated_ms": props.get("updated"),
        "latitude": latitude,
        "longitude": longitude,
        "depth_km": depth,
        "event_type": props.get("type"),
        "status": props.get("status"),
        "tsunami": props.get("tsunami"),
        "significance": props.get("sig"),
        "alert": props.get("alert"),
        "network": props.get("net"),
        "felt": props.get("felt"),
        "cdi": props.get("cdi"),
        "mmi": props.get("mmi"),
        "gap": props.get("gap"),
        "rms": props.get("rms"),
        "nst": props.get("nst"),
        "dmin": props.get("dmin"),
        "title": props.get("title"),
        "url": props.get("url"),
    }


def save_raw_json(data, path=config.RAW_JSON_PATH):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def save_raw_csv(records, path=config.RAW_CSV_PATH):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=RECORD_FIELDS)
        writer.writeheader()
        writer.writerows(records)


def collect(start_time=config.API_START_TIME, end_time=config.API_END_TIME,
            min_magnitude=config.API_MIN_MAGNITUDE, limit=config.API_LIMIT):
    """Collect earthquake records, save the raw data and return the records."""
    params = build_params(start_time, end_time, min_magnitude, limit)
    logger.info("Requesting %s with %s", config.API_URL, params)

    data = request_data(params)
    features = data.get("features")
    if not isinstance(features, list):
        raise ApiError("Response has no 'features' list")
    if not features:
        raise ApiError("API returned 0 records, change the date range or magnitude")

    records = [parse_feature(feature) for feature in features]

    save_raw_json(data)
    save_raw_csv(records)
    logger.info("Collected %d records, saved to %s", len(records), config.RAW_CSV_PATH)
    return records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    collected = collect()
    print(f"Collected records: {len(collected)}")

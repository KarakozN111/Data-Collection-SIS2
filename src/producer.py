import json
import math
import os
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer, KafkaException

# kafka settings from .env (defaults for local run)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")
RAW_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "raw_data.csv"


# dict to json string, NaN to null
def to_json(record):
    clean = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in record.items()}
    return json.dumps(clean, default=str)


def send_records(records):
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    stats = {"total": len(records), "published": 0, "failed": 0}

    # check that broker is running
    try:
        producer.list_topics(timeout=5)
    except KafkaException as e:
        print(f"Kafka is not available at {BOOTSTRAP_SERVERS}: {e}")
        stats["failed"] = len(records)
        return stats

    # delivery report for each message
    def on_delivery(err, msg):
        if err:
            stats["failed"] += 1
            print(f"Delivery failed: {err}")
        else:
            stats["published"] += 1

    for record in records:
        try:
            value = to_json(record)
        except (TypeError, ValueError) as e:
            stats["failed"] += 1
            print(f"JSON error: {e}")
            continue

        # send if queue is full wait and retry
        while True:
            try:
                producer.produce(TOPIC, key=str(record.get("id")), value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)

    # wait until all messages are delivered
    stats["failed"] += producer.flush(30)
    print(f"Records: {stats['total']}, published: {stats['published']}, failed: {stats['failed']}")
    return stats


# place for raw data from api_client
def load_records():
    if RAW_DATA_PATH.exists():
        return pd.read_csv(RAW_DATA_PATH).to_dict(orient="records")
    print("raw_data.csv not found, using test records")
    return [
        {"id": "test1", "mag": 4.5, "depth": 10.0, "place": "Test A", "time": 1763856000000,
         "type": "earthquake", "alert": None, "tsunami": 0},
        {"id": "test2", "mag": 2.1, "depth": 5.3, "place": "Test B", "time": 1763859600000,
         "type": "earthquake", "alert": "green", "tsunami": 0},
        {"id": "test3", "mag": 6.0, "depth": 35.0, "place": "Test C", "time": 1763863200000,
         "type": "earthquake", "alert": None, "tsunami": 1},
    ]


if __name__ == "__main__":
    send_records(load_records())
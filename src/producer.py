import json
import math

import pandas as pd
from confluent_kafka import Producer, KafkaException

from src import config


# dict to json string, NaN to null
def to_json(record):
    clean = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in record.items()}
    return json.dumps(clean, default=str)


def send_records(records):
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    stats = {"total": len(records), "published": 0, "failed": 0}

    # check that broker is running
    try:
        producer.list_topics(timeout=5)
    except KafkaException as e:
        raise RuntimeError(f"Kafka is not available at {config.KAFKA_BOOTSTRAP_SERVERS}: {e}")

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

        # send, if queue is full wait and retry
        while True:
            try:
                producer.produce(config.KAFKA_TOPIC, key=str(record.get("event_id")),
                                 value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)

    # wait until all messages are delivered
    stats["failed"] += producer.flush(30)
    print(f"Records: {stats['total']}, published: {stats['published']}, failed: {stats['failed']}")
    return stats["published"]


# raw data from api_client, test records if file not ready
def load_records():
    if config.RAW_CSV_PATH.exists():
        return pd.read_csv(config.RAW_CSV_PATH).to_dict(orient="records")
    print("raw_data.csv not found, using test records")
    return [
        {"event_id": "test1", "magnitude": 4.5, "depth_km": 10.0, "place": "Test A",
         "time_ms": 1763856000000, "event_type": "earthquake", "alert": None, "tsunami": 0},
        {"event_id": "test2", "magnitude": 2.1, "depth_km": 5.3, "place": "Test B",
         "time_ms": 1763859600000, "event_type": "earthquake", "alert": "green", "tsunami": 0},
        {"event_id": "test3", "magnitude": 6.0, "depth_km": 35.0, "place": "Test C",
         "time_ms": 1763863200000, "event_type": "earthquake", "alert": None, "tsunami": 1},
    ]


if __name__ == "__main__":
    try:
        send_records(load_records())
    except RuntimeError as e:
        print(e)
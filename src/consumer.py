import json
import os
import time

from confluent_kafka import Consumer, KafkaException

# kafka settings from .env (defaults for local run)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "earthquake-consumer")
IDLE_TIMEOUT = 10  # stop if no new messages for 10 seconds


def consume_records(expected_count=None):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    records = []
    stats = {"consumed": 0, "errors": 0}

    # check that broker is running
    try:
        consumer.list_topics(timeout=5)
    except KafkaException as e:
        print(f"Kafka is not available at {BOOTSTRAP_SERVERS}: {e}")
        consumer.close()
        return records, stats

    consumer.subscribe([TOPIC])
    last_message_time = time.time()

    try:
        while True:
            # stop condition: all expected messages read or no new messages
            if expected_count is not None and stats["consumed"] >= expected_count:
                break
            if time.time() - last_message_time > IDLE_TIMEOUT:
                print(f"No new messages for {IDLE_TIMEOUT} seconds, stopping")
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            last_message_time = time.time()

            if msg.error():
                stats["errors"] += 1
                print(f"Kafka error: {msg.error()}")
                continue

            # json string to dict
            try:
                records.append(json.loads(msg.value().decode("utf-8")))
                stats["consumed"] += 1
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                stats["errors"] += 1
                print(f"JSON error: {e}")
    finally:
        consumer.close()

    print(f"Consumed: {stats['consumed']}, errors: {stats['errors']}")
    return records, stats


if __name__ == "__main__":
    records, stats = consume_records()
    for record in records[:3]:
        print(record)
import json
import time

from confluent_kafka import Consumer, KafkaException

from src import config

GROUP_ID = "earthquake-consumer"
IDLE_TIMEOUT = 10  # stop if no new messages for 10 seconds


def consume_records(expected_count=None):
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    records = []
    errors = 0

    # check that broker is running
    try:
        consumer.list_topics(timeout=5)
    except KafkaException as e:
        consumer.close()
        raise RuntimeError(f"Kafka is not available at {config.KAFKA_BOOTSTRAP_SERVERS}: {e}")

    consumer.subscribe([config.KAFKA_TOPIC])
    last_message_time = time.time()

    try:
        while True:
            # stop condition: all expected messages read or no new messages
            if expected_count is not None and len(records) >= expected_count:
                break
            if time.time() - last_message_time > IDLE_TIMEOUT:
                print(f"No new messages for {IDLE_TIMEOUT} seconds, stopping")
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            last_message_time = time.time()

            if msg.error():
                errors += 1
                print(f"Kafka error: {msg.error()}")
                continue

            # json string to dict
            try:
                records.append(json.loads(msg.value().decode("utf-8")))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                errors += 1
                print(f"JSON error: {e}")
    finally:
        consumer.close()

    print(f"Consumed: {len(records)}, errors: {errors}")
    return records


if __name__ == "__main__":
    try:
        for record in consume_records()[:3]:
            print(record)
    except RuntimeError as e:
        print(e)
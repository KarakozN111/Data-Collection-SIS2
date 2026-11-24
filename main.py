"""Run the complete pipeline:

USGS API -> Kafka Producer -> Kafka Topic -> Kafka Consumer -> Pandera validation
-> cleaning and transformation -> SQLite3 -> EDA and figures

Before running, start Kafka (docker compose up -d) and create the topic (see README.md).

Usage:
    python main.py
    python main.py --start 2026-09-01 --end 2026-10-01 --min-magnitude 3
"""
import argparse
import importlib
import json
import logging
import sys

import pandas as pd

from src import api_client, config

logger = logging.getLogger("pipeline")


def load_module(name):
    """Import a pipeline module and give a clear message if it is not ready yet."""
    try:
        return importlib.import_module(f"src.{name}")
    except ModuleNotFoundError as error:
        logger.error("Module src/%s.py cannot be loaded: %s", name, error)
        sys.exit(1)


def run_stage(title, function, *args):
    """Run one stage and stop the pipeline with a clear message if it fails."""
    logger.info("=== %s ===", title)
    try:
        return function(*args)
    except Exception as error:
        logger.error("Stage '%s' failed: %s", title, error)
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Earthquake data pipeline")
    parser.add_argument("--start", default=config.API_START_TIME, help="start date, YYYY-MM-DD")
    parser.add_argument("--end", default=config.API_END_TIME, help="end date, YYYY-MM-DD")
    parser.add_argument("--min-magnitude", type=float, default=config.API_MIN_MAGNITUDE)

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    summary = {}

    # 1. API collection
    records = run_stage("1. API data collection", api_client.collect,
                        args.start, args.end, args.min_magnitude)
    summary["collected_from_api"] = len(records)

    # 2. Kafka streaming: records go through the real Kafka topic
    producer = load_module("producer")
    consumer = load_module("consumer")
    summary["published_to_kafka"] = run_stage("2. Kafka Producer", producer.send_records, records)
    consumed = run_stage("3. Kafka Consumer", consumer.consume_records, len(records))
    summary["consumed_from_kafka"] = len(consumed)

    # 3. Validation
    validator = load_module("validator")
    valid_df, invalid_df = run_stage("4. Pandera validation", validator.validate, pd.DataFrame(consumed))
    summary["valid_records"] = len(valid_df)
    summary["invalid_records"] = len(invalid_df)

    # 4. Cleaning and transformation
    cleaner = load_module("cleaner")
    clean_df = run_stage("5. Cleaning and transformation", cleaner.clean, valid_df)
    summary["after_cleaning"] = len(clean_df)

    # 5. SQLite loading, run twice to check idempotency
    loader = load_module("loader")
    summary["stored_first_load"] = run_stage("6. SQLite loading (run 1)", loader.load_to_db, clean_df)
    summary["stored_second_load"] = run_stage("6. SQLite loading (run 2)", loader.load_to_db, clean_df)

    # 6. Analysis
    analysis = load_module("analysis")
    run_stage("7. EDA and visualization", analysis.run_analysis)

    print_summary(summary)
    with open(config.SUMMARY_PATH, "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)


def print_summary(summary):
    labels = {
        "collected_from_api": "Collected from API",
        "published_to_kafka": "Published to Kafka",
        "consumed_from_kafka": "Consumed from Kafka",
        "valid_records": "Valid records",
        "invalid_records": "Invalid records",
        "after_cleaning": "Records after cleaning",
        "stored_first_load": "Stored in SQLite (load 1)",
        "stored_second_load": "Stored in SQLite (load 2)",
    }
    print("\n===== Pipeline summary =====")
    for key, label in labels.items():
        print(f"{label:<28}{summary[key]}")

    if summary["stored_first_load"] == summary["stored_second_load"]:
        print("Idempotency check: OK, no duplicates after repeated loading")
    else:
        print("Idempotency check: FAILED, row count changed after repeated loading")


if __name__ == "__main__":
    main()

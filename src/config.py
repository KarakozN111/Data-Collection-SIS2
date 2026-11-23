"""Shared project settings.

Values are read from environment variables (or a local .env file) with safe defaults,
so the project runs without any private configuration.
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent

API_URL = os.getenv("API_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
API_START_TIME = os.getenv("API_START_TIME", "2026-10-01")
API_END_TIME = os.getenv("API_END_TIME", "2026-11-01")
API_MIN_MAGNITUDE = float(os.getenv("API_MIN_MAGNITUDE", "2.5"))
API_LIMIT = int(os.getenv("API_LIMIT", "20000"))
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))

# Apache Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")

# Files
DATA_DIR = PROJECT_ROOT / "data"
RAW_JSON_PATH = DATA_DIR / "raw_response.json"
RAW_CSV_PATH = DATA_DIR / "raw_data.csv"
DB_PATH = DATA_DIR / "output.db"
SUMMARY_PATH = DATA_DIR / "pipeline_summary.json"
FIGURES_DIR = PROJECT_ROOT / "figures"

"""Stage 6: store cleaned earthquake records in SQLite3.

The table uses event_id (the USGS event id) as PRIMARY KEY, so one earthquake
can be stored only once. Records are written with an UPSERT
(INSERT ... ON CONFLICT(event_id) DO UPDATE ... WHERE newer), which makes the
loading stage idempotent:
  - a new event_id is inserted;
  - an existing event_id is updated only if USGS sent a newer version of the event
    (updated_time is later than the stored one);
  - the same data loaded again changes nothing.

Run separately:
    python -m src.loader            # validate + clean raw_data.csv, load twice, check
    python -m src.loader --verify   # only run sql/verification_queries.sql
"""
import argparse
import hashlib
import logging
import sqlite3

import pandas as pd

from src import config

logger = logging.getLogger(__name__)

TABLE_NAME = "earthquakes"
SQL_VERIFICATION_PATH = config.PROJECT_ROOT / "sql" / "verification_queries.sql"

# Column name -> SQLite type and constraints.
# The order here is the order of columns in the table.
COLUMNS = {
    "event_id":     "TEXT PRIMARY KEY",
    "event_time":   "TEXT NOT NULL",          # ISO 8601, UTC, e.g. 2026-10-01 05:21:59
    "updated_time": "TEXT",
    "magnitude":    "REAL NOT NULL CHECK (magnitude BETWEEN -2 AND 10)",
    "mag_type":     "TEXT",
    "place":        "TEXT",
    "region":       "TEXT",
    "latitude":     "REAL NOT NULL CHECK (latitude BETWEEN -90 AND 90)",
    "longitude":    "REAL NOT NULL CHECK (longitude BETWEEN -180 AND 180)",
    "depth_km":     "REAL NOT NULL",
    "event_type":   "TEXT",
    "status":       "TEXT",
    "tsunami":      "INTEGER CHECK (tsunami IN (0, 1))",
    "significance": "INTEGER",
    "alert":        "TEXT",
    "network":      "TEXT",
    "felt":         "INTEGER",
    "cdi":          "REAL",
    "mmi":          "REAL",
    "gap":          "REAL",
    "rms":          "REAL",
    "nst":          "INTEGER",
    "dmin":         "REAL",
    "title":        "TEXT",
    "url":          "TEXT",
}
INTEGER_COLUMNS = [name for name, sql_type in COLUMNS.items() if sql_type.startswith("INTEGER")]
REQUIRED_COLUMNS = ["event_id", "event_time", "magnitude", "latitude", "longitude", "depth_km"]


def create_table(connection):
    """Create the table and indexes if they do not exist yet."""
    columns_sql = ",\n    ".join(f"{name} {sql_type}" for name, sql_type in COLUMNS.items())
    connection.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    {columns_sql}\n)")
    # Indexes for the columns used most often in WHERE / GROUP BY / ORDER BY
    connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_time ON {TABLE_NAME}(event_time)")
    connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_region ON {TABLE_NAME}(region)")
    connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_mag ON {TABLE_NAME}(magnitude)")


def ms_to_text(series):
    """Milliseconds since 1970 (UTC) -> 'YYYY-MM-DD HH:MM:SS' text."""
    return pd.to_datetime(series, unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")


def datetime_to_text(series):
    """Any datetime column (with or without time zone) -> 'YYYY-MM-DD HH:MM:SS' text in UTC."""
    times = pd.to_datetime(series, utc=True, errors="coerce")
    return times.dt.strftime("%Y-%m-%d %H:%M:%S")


def prepare_for_db(df):
    """Bring the cleaned DataFrame to the table format.

    cleaner.py is expected to give event_time / updated_time as datetimes and a region
    column. If it still has only time_ms / updated_ms or no region, they are created here,
    so the loader does not break. Columns that are not in the table are not stored.
    """
    data = df.copy()

    if "event_time" in data.columns:
        data["event_time"] = datetime_to_text(data["event_time"])
    elif "time_ms" in data.columns:
        data["event_time"] = ms_to_text(data["time_ms"])

    if "updated_time" in data.columns:
        data["updated_time"] = datetime_to_text(data["updated_time"])
    elif "updated_ms" in data.columns:
        data["updated_time"] = ms_to_text(data["updated_ms"])

    if "region" not in data.columns and "place" in data.columns:
        # "20 km ESE of Anza, CA" -> "CA", "North Atlantic Ocean" -> "North Atlantic Ocean"
        data["region"] = data["place"].astype("string").str.split(",").str[-1].str.strip()

    missing = [name for name in REQUIRED_COLUMNS if name not in data.columns]
    if missing:
        raise ValueError(f"Required columns are missing in the cleaned data: {missing}")

    # Keep only table columns; optional columns that cleaner did not create become NULL
    data = data.reindex(columns=list(COLUMNS))

    # SQLite does not know pandas Int64 / NaN: use Python int and None
    for name in INTEGER_COLUMNS:
        data[name] = pd.to_numeric(data[name], errors="coerce").round().astype("Int64")
    data = data.astype(object).where(data.notna(), None)
    return data


def build_upsert_sql():
    names = list(COLUMNS)
    placeholders = ", ".join("?" for _ in names)
    update_part = ",\n        ".join(f"{name} = excluded.{name}" for name in names if name != "event_id")
    # The WHERE makes repeated loading of the same data a no-op:
    # a row is rewritten only if USGS updated the event after the stored version.
    return (
        f"INSERT INTO {TABLE_NAME} ({', '.join(names)})\n"
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT(event_id) DO UPDATE SET\n        {update_part}\n"
        f"WHERE excluded.updated_time > {TABLE_NAME}.updated_time"
    )


def count_rows(connection):
    return connection.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]


def table_checksum(connection):
    """MD5 of all rows ordered by event_id. Equal checksums = identical table content."""
    digest = hashlib.md5()
    for row in connection.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY event_id"):
        digest.update(repr(row).encode("utf-8"))
    return digest.hexdigest()


def load_to_db(df, db_path=None):
    """Insert cleaned records into SQLite. Returns the number of rows in the table after loading."""
    db_path = db_path or config.DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)

    data = prepare_for_db(df)
    duplicates_in_input = int(data["event_id"].duplicated().sum())
    if duplicates_in_input:
        logger.warning("%d duplicate event_id values in the input, only one copy is kept",
                       duplicates_in_input)
        data = data.drop_duplicates(subset="event_id", keep="last")
    rows = list(data.itertuples(index=False, name=None))

    try:
        with sqlite3.connect(db_path) as connection:   # commits on success, rolls back on error
            create_table(connection)
            rows_before = count_rows(connection)
            changes_before = connection.total_changes
            connection.executemany(build_upsert_sql(), rows)
            changed = connection.total_changes - changes_before
            rows_after = count_rows(connection)
            duplicate_ids = connection.execute(
                f"SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM {TABLE_NAME}").fetchone()[0]
    except sqlite3.Error as error:
        raise RuntimeError(f"SQLite error while loading {db_path}: {error}") from error

    inserted = rows_after - rows_before
    updated = changed - inserted
    logger.info("Processed records: %d | in table before: %d | inserted: %d | updated: %d | "
                "unchanged: %d | in table after: %d | duplicate ids: %d",
                len(rows), rows_before, inserted, updated, len(rows) - changed,
                rows_after, duplicate_ids)
    return rows_after


def idempotency_test(df, db_path=None):
    """Load the same data twice and prove that the table did not change."""
    db_path = db_path or config.DB_PATH
    first_count = load_to_db(df, db_path)
    with sqlite3.connect(db_path) as connection:
        first_checksum = table_checksum(connection)

    second_count = load_to_db(df, db_path)
    with sqlite3.connect(db_path) as connection:
        second_checksum = table_checksum(connection)

    print("\n===== Idempotency test =====")
    print(f"Processed records       {len(df)}")
    print(f"Rows after load 1       {first_count}")
    print(f"Rows after load 2       {second_count}")
    print(f"Checksum after load 1   {first_checksum}")
    print(f"Checksum after load 2   {second_checksum}")
    passed = first_count == second_count and first_checksum == second_checksum
    print("Result: OK, no duplicates and no changes" if passed else "Result: FAILED")
    return passed


def run_verification_queries(db_path=None, sql_path=SQL_VERIFICATION_PATH):
    """Run every query from sql/verification_queries.sql and print the results.

    Works without the sqlite3 command line tool (it is often missing on Windows).
    """
    db_path = db_path or config.DB_PATH
    if not db_path.exists():
        raise FileNotFoundError(f"{db_path} not found, run the loader first")
    script = sql_path.read_text(encoding="utf-8")

    # Split the file into statements; the "-- " comment line above a query is its title
    statements, title, buffer = [], None, []
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            if not buffer:
                title = stripped.lstrip("- ").strip()
            continue
        if stripped.startswith("."):        # sqlite3 CLI commands like .headers on
            continue
        if stripped:
            buffer.append(line)
        if stripped.endswith(";"):
            statements.append((title, "\n".join(buffer)))
            title, buffer = None, []

    with sqlite3.connect(db_path) as connection:
        for number, (title, statement) in enumerate(statements, start=1):
            print(f"\n--- Query {number}: {title or ''}")
            result = pd.read_sql_query(statement, connection)
            print(result.to_string(index=False))


def load_clean_data_from_raw():
    """For running this file alone: raw_data.csv -> validator -> cleaner."""
    from src import cleaner, validator

    raw_df = pd.read_csv(config.RAW_CSV_PATH)
    valid_df, invalid_df = validator.validate(raw_df)
    clean_df = cleaner.clean(valid_df)
    print(f"Raw: {len(raw_df)}, valid: {len(valid_df)}, invalid: {len(invalid_df)}, "
          f"after cleaning: {len(clean_df)}")
    return clean_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Load cleaned data into SQLite3")
    parser.add_argument("--verify", action="store_true",
                        help="only run sql/verification_queries.sql on data/output.db")
    args = parser.parse_args()

    if not args.verify:
        idempotency_test(load_clean_data_from_raw())
    run_verification_queries()

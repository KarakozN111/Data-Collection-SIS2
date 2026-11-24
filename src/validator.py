"""Validate consumed earthquake records with Pandera (Member 3).

The schema is applied BEFORE cleaning, to the records exactly as they came out of Kafka.

Handling strategy for invalid data:
    * the whole record is moved to a quarantine table (invalid_df) together with
      the list of its validation errors and saved to data/invalid_records.csv;
    * only fully valid records continue to the cleaning stage.
Missing values in optional fields (alert, felt, cdi, mmi ...) are allowed here
and handled later in cleaner.py.

Because the USGS data is almost always valid, four deliberately invalid TEST
records are added before validation. They have event_id "TEST_INVALID_xx" and
is_test_record = True, so they are easy to find and never reach the final dataset.
"""
import logging
import time

import pandas as pd

try:  
    import pandera.pandas as pa
except ImportError:  
    import pandera as pa
from pandera.errors import SchemaErrors

from src import config

logger = logging.getLogger(__name__)

VALID_PATH = config.DATA_DIR / "valid_records.csv"
INVALID_PATH = config.DATA_DIR / "invalid_records.csv"

# Allowed categorical values (from the USGS API documentation)
EVENT_TYPES = [
    "earthquake", "quarry blast", "explosion", "chemical explosion", "mining explosion",
    "nuclear explosion", "landslide", "rock burst", "ice quake", "volcanic eruption",
    "sonic boom", "other event", "experimental explosion",
]
STATUSES = ["automatic", "reviewed", "deleted"]
ALERT_LEVELS = ["green", "yellow", "orange", "red"]

# Earliest acceptable event time: 2000-01-01 00:00 UTC in milliseconds
MIN_TIME_MS = int(pd.Timestamp("2000-01-01", tz="UTC").timestamp() * 1000)


def build_schema():
    """Pandera schema for one consumed record.

    coerce=True converts every column to the declared type. If a value cannot be
    converted (for example magnitude = "abc"), Pandera reports it as an error.
    """
    now_ms = int(time.time() * 1000)  # events cannot be in the future

    return pa.DataFrameSchema(
        columns={
            # Rule 1: required unique id, not empty
            "event_id": pa.Column(str, pa.Check.str_length(min_value=1), nullable=False),
            # Rule 2: magnitude is a number in a physically possible range
            "magnitude": pa.Column(float, pa.Check.in_range(-1.0, 10.0), nullable=False),
            "mag_type": pa.Column(str, pa.Check.str_matches(r"^[A-Za-z_]+$"), nullable=False),
            "place": pa.Column(str, nullable=True),
            # Rule 3: date/time checks - after 2000 and not in the future
            "time_ms": pa.Column("int64", pa.Check.in_range(MIN_TIME_MS, now_ms), nullable=False),
            "updated_ms": pa.Column("int64", pa.Check.in_range(MIN_TIME_MS, now_ms), nullable=False),
            # Rule 4: coordinates and depth inside real-world boundaries
            "latitude": pa.Column(float, pa.Check.in_range(-90.0, 90.0), nullable=False),
            "longitude": pa.Column(float, pa.Check.in_range(-180.0, 180.0), nullable=False),
            "depth_km": pa.Column(float, pa.Check.in_range(-10.0, 800.0), nullable=False),
            # Rule 5: allowed categorical values
            "event_type": pa.Column(str, pa.Check.isin(EVENT_TYPES), nullable=False),
            "status": pa.Column(str, pa.Check.isin(STATUSES), nullable=False),
            "alert": pa.Column(str, pa.Check.isin(ALERT_LEVELS), nullable=True),
            "network": pa.Column(str, pa.Check.str_matches(r"^[a-z]{2}$"), nullable=False),
            "tsunami": pa.Column("int64", pa.Check.isin([0, 1]), nullable=False),
            # Rule 6: non-negative measurement fields (missing values allowed)
            "significance": pa.Column("int64", pa.Check.ge(0), nullable=False),
            "felt": pa.Column(float, pa.Check.ge(0), nullable=True),
            "cdi": pa.Column(float, pa.Check.in_range(0.0, 12.0), nullable=True),
            "mmi": pa.Column(float, pa.Check.in_range(0.0, 12.0), nullable=True),
            "gap": pa.Column(float, pa.Check.in_range(0.0, 360.0), nullable=True),
            "rms": pa.Column(float, pa.Check.ge(0), nullable=True),
            "nst": pa.Column(float, pa.Check.ge(0), nullable=True),
            "dmin": pa.Column(float, pa.Check.ge(0), nullable=True),
            "title": pa.Column(str, nullable=True),
            "url": pa.Column(str, pa.Check.str_startswith("https://"), nullable=True),
        },
        checks=[
            # Rule 7 (custom, uses two columns): a record cannot be updated before the event happened
            pa.Check(
                lambda df: df["updated_ms"] >= df["time_ms"],
                name="updated_after_event_time",
                error="updated_ms must be >= time_ms",
            ),
        ],
        coerce=True,
        strict=False,  # extra columns (is_test_record) are allowed
    )


def make_test_records(template):
    """Create 4 deliberately invalid records based on one real record."""
    tests = []
    for number in range(1, 5):
        record = dict(template)
        record["event_id"] = f"TEST_INVALID_{number:02d}"
        record["title"] = f"TEST RECORD {number}"
        record["is_test_record"] = True
        tests.append(record)

    # 1: numbers out of range
    tests[0]["magnitude"] = 15.2
    tests[0]["latitude"] = 123.4
    # 2: categories that are not allowed
    tests[1]["event_type"] = "meteor strike"
    tests[1]["status"] = "pending"
    tests[1]["alert"] = "purple"
    # 3: date in the future (2099) and update time before the event time
    tests[2]["time_ms"] = int(pd.Timestamp("2099-01-01", tz="UTC").timestamp() * 1000)
    # 4: wrong data type, missing required value, wrong URL
    tests[3]["magnitude"] = "abc"
    tests[3]["depth_km"] = None
    tests[3]["url"] = "ftp://wrong-url"
    return tests


def collect_errors(failure_cases):
    """Turn Pandera failure cases into {row index: "error1; error2"}."""
    # Rows with index = None are column-level problems (for example a missing column)
    row_failures = failure_cases.dropna(subset=["index"])
    errors = {}
    for _, failure in row_failures.iterrows():
        column = failure["column"] if pd.notna(failure["column"]) else "row"
        text = f"{column}: {failure['check']} (value={failure['failure_case']})"
        errors.setdefault(int(failure["index"]), []).append(text)
    return {index: "; ".join(texts) for index, texts in errors.items()}


def validate(df, add_test_records=True):
    """Validate the consumed records.

    Returns (valid_df, invalid_df). invalid_df has an extra 'validation_errors' column.
    """
    df = df.copy().reset_index(drop=True)
    df["is_test_record"] = False

    if add_test_records and len(df) > 0:
        tests = pd.DataFrame(make_test_records(df.iloc[0].to_dict()))
        df = pd.concat([df, tests], ignore_index=True)
        logger.info("Added %d deliberately invalid test records", len(tests))

    schema = build_schema()
    try:
        checked = schema.validate(df, lazy=True)  # lazy=True: collect ALL errors, not only the first
        errors_by_row = {}
    except SchemaErrors as error:
        failure_cases = error.failure_cases
        errors_by_row = collect_errors(failure_cases)
        print_error_summary(failure_cases)
        checked = None

        # A missing column is a structure problem, not a problem of single rows -> stop
        missing_columns = failure_cases[failure_cases["check"].astype(str).str.contains("column_in_dataframe")]
        if not missing_columns.empty:
            raise ValueError(f"Required columns are missing: {list(missing_columns['failure_case'])}") from error

    invalid_mask = df.index.isin(list(errors_by_row))
    invalid_df = df[invalid_mask].copy()
    invalid_df["validation_errors"] = invalid_df.index.map(errors_by_row)

    if checked is None:
        # validate again only the good rows to get them with correct types
        checked = schema.validate(df[~invalid_mask])
    valid_df = checked[~checked["is_test_record"].astype(bool)].drop(columns="is_test_record")

    # Duplicated ids are not rejected here, cleaner.py removes them
    duplicates = valid_df["event_id"].duplicated().sum()

    test_count = int(invalid_df["is_test_record"].sum())
    print("\n----- Pandera validation -----")
    print(f"Records checked        {len(df)} ({len(df) - int(df['is_test_record'].sum())} real + "
          f"{int(df['is_test_record'].sum())} test)")
    print(f"Valid records          {len(valid_df)}")
    print(f"Invalid records        {len(invalid_df)} ({len(invalid_df) - test_count} real + {test_count} test)")
    print(f"Duplicated event_id    {duplicates} (handled in cleaner.py)")
    if not invalid_df.empty:
        print("\nInvalid records and their errors:")
        for _, row in invalid_df.iterrows():
            print(f"  {row['event_id']}: {row['validation_errors']}")

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    valid_df.to_csv(VALID_PATH, index=False)
    invalid_df.to_csv(INVALID_PATH, index=False)
    logger.info("Saved %s and %s", VALID_PATH.name, INVALID_PATH.name)

    return valid_df.reset_index(drop=True), invalid_df.reset_index(drop=True)


def print_error_summary(failure_cases):
    """How many failures each rule produced."""
    summary = (failure_cases.fillna({"column": "row"})
               .groupby(["column", "check"]).size()
               .reset_index(name="failures"))
    print("\nFailures by validation rule:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raw_df = pd.read_csv(config.RAW_CSV_PATH)
    validate(raw_df)

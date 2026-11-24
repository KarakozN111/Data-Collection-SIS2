"""Clean and transform validated earthquake records with pandas and NumPy (Member 3).

Input:  valid records from validator.py
Output: one clean DataFrame for loader.py (one row = one unique event)

Additional results of the transformation stage are saved to data/transformations/:
    missing_values.csv       missing values before and after cleaning
    region_summary.csv       GroupBy + aggregation by region
    network_summary.csv      GroupBy + aggregation by seismic network
    pivot_region_depth.csv   pivot_table: number of events per region and depth class
    strong_shallow.csv       filter with several conditions
"""
import logging

import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)

CLEAN_PATH = config.DATA_DIR / "clean_data.csv"
OUTPUT_DIR = config.DATA_DIR / "transformations"

TEXT_COLUMNS = ["mag_type", "event_type", "status", "alert", "network"]
NUMERIC_COLUMNS = ["magnitude", "latitude", "longitude", "depth_km", "felt", "cdi",
                   "mmi", "gap", "rms", "nst", "dmin", "significance", "tsunami"]

# USGS writes some US states as abbreviations ("20 km ESE of Anza, CA")
STATE_NAMES = {
    "CA": "California", "NV": "Nevada", "OR": "Oregon", "WA": "Washington",
    "ID": "Idaho", "MT": "Montana", "WY": "Wyoming", "UT": "Utah", "AZ": "Arizona",
    "NM": "New Mexico", "CO": "Colorado", "TX": "Texas", "OK": "Oklahoma",
    "AK": "Alaska", "HI": "Hawaii", "B.C.": "British Columbia", "MX": "Mexico",
}


def handle_missing(df, report):
    """Fill missing values only where the meaning of the gap is known."""
    before = df.isna().sum()

    # place: 1 empty value -> "Unknown" so that the region can still be built
    df["place"] = df["place"].fillna("Unknown location")
    # alert: empty means "PAGER did not issue an alert", not an unknown value
    df["alert"] = df["alert"].fillna("none")
    # felt: empty means nobody sent a "Did you feel it?" report -> 0 reports
    df["felt"] = df["felt"].fillna(0)
    # gap and dmin: very few gaps -> median of the same network (the stations are similar)
    for column in ["gap", "dmin", "rms", "nst"]:
        df[column] = df[column].fillna(df.groupby("network")[column].transform("median"))
        df[column] = df[column].fillna(df[column].median())
    # cdi and mmi are NOT filled: they exist only for felt / strong events,
    # inventing intensity for other events would create fake data. They stay NULL.

    after = df.isna().sum()
    missing = pd.DataFrame({"missing_before": before, "missing_after": after})
    missing = missing[missing["missing_before"] > 0]
    missing.to_csv(OUTPUT_DIR / "missing_values.csv", index_label="column")
    print("\nMissing values (before -> after):")
    print(missing.to_string())
    report["missing_values_filled"] = int(before.sum() - after.sum())
    return df


def remove_duplicates(df, report):
    """Remove full copies and repeated event ids (keep the most recently updated version)."""
    start = len(df)
    df = df.drop_duplicates()
    full_copies = start - len(df)

    df = df.sort_values("updated_ms").drop_duplicates(subset="event_id", keep="last")
    same_id = start - full_copies - len(df)

    report["duplicates_removed"] = full_copies + same_id
    print(f"\nDuplicates: {full_copies} full copies, {same_id} repeated event_id -> removed")
    return df


def convert_types(df):
    """Set correct types for every column."""
    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    for column in ["tsunami", "significance", "felt", "nst"]:
        df[column] = df[column].round().astype("int64")

    # milliseconds since 1970 -> real UTC datetime
    df["event_time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df["updated_time"] = pd.to_datetime(df["updated_ms"], unit="ms", utc=True)
    return df.drop(columns=["time_ms", "updated_ms"])


def normalize_text(df):
    """Same spelling for the same value, and a separate 'region' column."""
    for column in TEXT_COLUMNS:
        df[column] = df[column].astype(str).str.strip().str.lower()
    df["event_type"] = df["event_type"].str.replace(r"\s+", "_", regex=True)  # "quarry blast" -> "quarry_blast"

    df["place"] = df["place"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["title"] = df["title"].astype(str).str.strip()

    # "45 km SW of Ovalle, Chile" -> "Chile";  "Japan region" -> "Japan"
    region = df["place"].str.split(",").str[-1].str.strip()
    region = region.str.replace(r"\s+region$", "", regex=True)
    region = region.replace(STATE_NAMES)
    df["region"] = region.str[0].str.upper() + region.str[1:]  # "south of ..." -> "South of ..."
    return df


def add_numpy_features(df):
    """New numeric columns calculated with NumPy (vectorized, no loops)."""
    magnitude = df["magnitude"].to_numpy()
    depth = df["depth_km"].to_numpy()

    # Released energy (Gutenberg-Richter): log10(E) = 1.5*M + 4.8, E in joules
    df["energy_joules"] = np.power(10.0, 1.5 * magnitude + 4.8)
    df["log10_energy"] = np.round(np.log10(df["energy_joules"].to_numpy()), 2)

    # Depth class used in seismology: shallow < 70 km, intermediate 70-300 km, deep > 300 km
    df["depth_class"] = np.select(
        [depth < 70, depth <= 300], ["shallow", "intermediate"], default="deep")

    # Magnitude class: 2.5-3.9 minor, 4.0-4.9 light, 5.0-5.9 moderate, 6+ strong
    df["magnitude_class"] = np.select(
        [magnitude < 4, magnitude < 5, magnitude < 6], ["minor", "light", "moderate"], default="strong")

    # How unusual the magnitude is compared to the whole month (z-score)
    df["magnitude_zscore"] = np.round((magnitude - np.mean(magnitude)) / np.std(magnitude), 3)

    df["hemisphere"] = np.where(df["latitude"].to_numpy() >= 0, "north", "south")
    df["is_strong"] = np.where(magnitude >= 5.0, 1, 0)
    return df


def make_transformations(df, report):
    """Filtering, GroupBy and reshaping on the clean data."""
    # Filter with several conditions: strong, shallow, real earthquakes
    # (shallow strong events are the most dangerous ones for people)
    strong_shallow = df[(df["magnitude"] >= 5.0)
                        & (df["depth_class"] == "shallow")
                        & (df["event_type"] == "earthquake")]
    strong_shallow.to_csv(OUTPUT_DIR / "strong_shallow.csv", index=False)
    report["strong_shallow_events"] = len(strong_shallow)
    print(f"\nFilter (magnitude >= 5 AND depth < 70 km AND earthquake): {len(strong_shallow)} events")

    # GroupBy by region with several aggregations
    region_summary = (df.groupby("region")
                      .agg(events=("event_id", "count"),
                           mean_magnitude=("magnitude", "mean"),
                           max_magnitude=("magnitude", "max"),
                           mean_depth_km=("depth_km", "mean"),
                           strong_events=("is_strong", "sum"),
                           total_energy_joules=("energy_joules", "sum"))
                      .round(2)
                      .sort_values("events", ascending=False))
    region_summary.to_csv(OUTPUT_DIR / "region_summary.csv")
    print("\nTop 10 regions by number of events:")
    print(region_summary.head(10).to_string())

    # GroupBy by seismic network
    network_summary = (df.groupby("network")
                       .agg(events=("event_id", "count"),
                            mean_magnitude=("magnitude", "mean"),
                            reviewed_share=("status", lambda s: (s == "reviewed").mean()))
                       .round(3)
                       .sort_values("events", ascending=False))
    network_summary.to_csv(OUTPUT_DIR / "network_summary.csv")

    # Reshaping: pivot_table, top-10 regions x depth class
    top_regions = region_summary.head(10).index
    pivot = pd.pivot_table(df[df["region"].isin(top_regions)],
                           index="region", columns="depth_class", values="event_id",
                           aggfunc="count", fill_value=0)
    pivot = pivot.reindex(columns=["shallow", "intermediate", "deep"], fill_value=0)
    pivot.to_csv(OUTPUT_DIR / "pivot_region_depth.csv")
    print("\nPivot table: events per region and depth class:")
    print(pivot.to_string())


def clean(df):
    """Full cleaning stage. Returns the clean DataFrame for loader.py."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = {"input_records": len(df)}
    df = df.copy()
    if "is_test_record" in df.columns:  # test records must never reach the final data
        df = df[~df["is_test_record"].astype(bool)].drop(columns="is_test_record")

    df = remove_duplicates(df, report)
    df = handle_missing(df, report)
    df = convert_types(df)
    df = normalize_text(df)
    df = add_numpy_features(df)
    df = df.sort_values("event_time").reset_index(drop=True)

    make_transformations(df, report)

    df.to_csv(CLEAN_PATH, index=False)
    report["output_records"] = len(df)
    print(f"\nCleaning: {report['input_records']} records in -> {report['output_records']} records out "
          f"({report['duplicates_removed']} duplicates removed, "
          f"{report['missing_values_filled']} missing values filled)")
    logger.info("Saved clean data to %s", CLEAN_PATH)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Use the output of validator.py if it exists, otherwise validate the raw data first
    from src import validator
    if validator.VALID_PATH.exists():
        valid_df = pd.read_csv(validator.VALID_PATH)
    else:
        valid_df, _ = validator.validate(pd.read_csv(config.RAW_CSV_PATH))
    clean(valid_df)

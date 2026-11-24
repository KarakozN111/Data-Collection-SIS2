"""Stage 7: exploratory data analysis (EDA) of the earthquakes stored in SQLite3.

Reads data/output.db (not the CSV), so the analysis uses exactly the data that the
pipeline stored. Prints statistics, saves figures to figures/ and the key numbers
to data/eda_summary.json.

Research questions:
  Q1. How are magnitudes distributed - how rare are strong earthquakes?
  Q2. Where do the earthquakes happen and at what depth?
  Q3. Are deeper earthquakes stronger?
  Q4. Which regions are the most active, and at which depths?
  Q5. How does seismic activity change from day to day?

Run separately:  python -m src.analysis
"""
import json
import logging
import sqlite3

import matplotlib

matplotlib.use("Agg")  # save figures to files, no window needed
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src import config

logger = logging.getLogger(__name__)

TABLE_NAME = "earthquakes"

# Colors: one fixed color per depth class, used in every figure
DEPTH_LABELS = ["shallow (<70 km)", "intermediate (70-300 km)", "deep (300+ km)"]
DEPTH_BINS = [-np.inf, 70, 300, np.inf]
DEPTH_COLORS = {"shallow (<70 km)": "#eb6834",
                "intermediate (70-300 km)": "#2a78d6",
                "deep (300+ km)": "#1baf7a"}
MAIN_COLOR = "#2a78d6"
TEXT_COLOR = "#52514e"

MAG_LABELS = ["2.5-3.9", "4.0-4.9", "5.0-5.9", "6.0-6.9", "7.0+"]
MAG_BINS = [-np.inf, 4, 5, 6, 7, np.inf]


# ---------- data ----------

def read_data(db_path=None):
    db_path = db_path or config.DB_PATH
    if not db_path.exists():
        raise FileNotFoundError(f"{db_path} not found, run the loader first")
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", connection)
    if df.empty:
        raise ValueError("The earthquakes table is empty")

    # SQLite stores time as text -> back to datetime
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["event_date"] = df["event_time"].dt.date
    df["depth_class"] = pd.cut(df["depth_km"], bins=DEPTH_BINS, labels=DEPTH_LABELS, right=False)
    df["magnitude_class"] = pd.cut(df["magnitude"], bins=MAG_BINS, labels=MAG_LABELS, right=False)
    return df


# ---------- statistics ----------

def describe_dataset(df):
    print("\n===== 1. Dataset dimensions and types =====")
    print(f"Rows: {df.shape[0]}, columns: {df.shape[1]}")
    print(df.dtypes.to_string())

    print("\n===== 2. Missing values (only columns with gaps) =====")
    missing = df.isna().sum()
    missing = missing[missing > 0]
    print((pd.DataFrame({"missing": missing, "percent": (missing / len(df) * 100).round(1)})
           .to_string() if not missing.empty else "No missing values"))

    numeric_cols = ["magnitude", "depth_km", "significance", "nst", "gap", "rms"]
    print("\n===== 3. Descriptive statistics (numerical) =====")
    stats = df[numeric_cols].describe().T
    stats["skew"] = df[numeric_cols].skew()
    print(stats.round(2).to_string())

    print("\n===== 4. Categorical statistics =====")
    for column in ["event_type", "mag_type", "status", "network", "alert"]:
        counts = df[column].fillna("none").value_counts()
        print(f"\n{column}: {df[column].nunique()} unique values")
        print(counts.head(8).to_string())

    print("\n===== 5. Correlation between numerical columns =====")
    correlation = df[numeric_cols].corr().round(2)
    print(correlation.to_string())
    return stats, correlation


def grouped_analysis(df):
    print("\n===== 6. Grouped analysis =====")
    by_magnitude = (df.groupby("magnitude_class", observed=False)
                      .agg(events=("event_id", "count"),
                           avg_depth_km=("depth_km", "mean"),
                           tsunami_flags=("tsunami", "sum")).round(1))
    by_magnitude["share_percent"] = (by_magnitude["events"] / len(df) * 100).round(1)
    print("\nBy magnitude class:\n" + by_magnitude.to_string())

    by_depth = (df.groupby("depth_class", observed=False)
                  .agg(events=("event_id", "count"),
                       avg_magnitude=("magnitude", "mean"),
                       median_magnitude=("magnitude", "median"),
                       max_magnitude=("magnitude", "max")).round(2))
    print("\nBy depth class:\n" + by_depth.to_string())

    by_region = (df.groupby("region")
                   .agg(events=("event_id", "count"),
                        avg_magnitude=("magnitude", "mean"),
                        max_magnitude=("magnitude", "max"),
                        avg_depth_km=("depth_km", "mean"))
                   .sort_values("events", ascending=False).round(2))
    print("\nTop 10 regions:\n" + by_region.head(10).to_string())

    by_day = (df.groupby("event_date")
                .agg(events=("event_id", "count"), max_magnitude=("magnitude", "max")))
    print("\nEvents per day: mean {:.1f}, min {}, max {} ({})".format(
        by_day["events"].mean(), by_day["events"].min(),
        by_day["events"].max(), by_day["events"].idxmax()))

    by_mag_type = (df.groupby("mag_type")
                     .agg(events=("event_id", "count"), avg_magnitude=("magnitude", "mean"))
                     .sort_values("events", ascending=False).round(2))
    print("\nBy magnitude type:\n" + by_mag_type.to_string())
    return by_magnitude, by_depth, by_region, by_day


# ---------- figures ----------

def style_axes(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel(xlabel, color=TEXT_COLOR)
    ax.set_ylabel(ylabel, color=TEXT_COLOR)
    ax.tick_params(colors=TEXT_COLOR)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color("#c3c2b7")
    ax.grid(axis="y", color="#e1e0d9", linewidth=0.8)
    ax.set_axisbelow(True)


def save(fig, name):
    path = config.FIGURES_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", path.relative_to(config.PROJECT_ROOT))
    return path


def plot_magnitude_distribution(df):
    """Q1: histogram of magnitudes with a log scale on the y axis."""
    fig, ax = plt.subplots(figsize=(9, 5))
    bins = np.arange(2.5, df["magnitude"].max() + 0.2, 0.1)
    ax.hist(df["magnitude"], bins=bins, color=MAIN_COLOR, edgecolor="white", linewidth=0.8)
    ax.set_yscale("log")
    mean, median = df["magnitude"].mean(), df["magnitude"].median()
    ax.axvline(mean, color="#0b0b0b", linestyle="--", linewidth=1.5, label=f"mean = {mean:.2f}")
    ax.axvline(median, color="#0b0b0b", linestyle=":", linewidth=1.5, label=f"median = {median:.2f}")
    style_axes(ax, "Magnitude distribution of earthquakes, October 2026",
               "Magnitude", "Number of events (log scale)")
    ax.legend(frameon=False)
    return save(fig, "01_magnitude_distribution.png")


def plot_epicenter_map(df):
    """Q2: location of epicenters, color = depth class, size = magnitude."""
    fig, ax = plt.subplots(figsize=(12, 6))
    for label in DEPTH_LABELS:
        part = df[df["depth_class"] == label]
        ax.scatter(part["longitude"], part["latitude"],
                   s=(part["magnitude"] - 2) ** 2.5 * 3, color=DEPTH_COLORS[label],
                   alpha=0.6, edgecolor="white", linewidth=0.4, label=f"{label}: {len(part)}")
    ax.set_xlim(-180, 180)
    ax.set_ylim(-90, 90)
    ax.set_xticks(range(-180, 181, 60))
    ax.set_yticks(range(-90, 91, 30))
    ax.axhline(0, color="#c3c2b7", linewidth=0.8)
    style_axes(ax, "Epicenters of earthquakes M2.5+ (marker size = magnitude)",
               "Longitude, degrees", "Latitude, degrees")
    ax.grid(color="#e1e0d9", linewidth=0.8)
    ax.legend(title="Depth class", loc="lower left", frameon=True, markerscale=0.8)
    return save(fig, "02_epicenter_map.png")


def plot_depth_vs_magnitude(df):
    """Q3: depth against magnitude, depth axis goes down like in the Earth."""
    fig, ax = plt.subplots(figsize=(9, 6))
    for label in DEPTH_LABELS:
        part = df[df["depth_class"] == label]
        ax.scatter(part["magnitude"], part["depth_km"], s=14, alpha=0.55,
                   color=DEPTH_COLORS[label], edgecolor="none", label=label)
    ax.invert_yaxis()
    correlation = df["magnitude"].corr(df["depth_km"])
    style_axes(ax, f"Depth vs magnitude (Pearson r = {correlation:.2f})",
               "Magnitude", "Depth, km (down = deeper)")
    ax.grid(color="#e1e0d9", linewidth=0.8)
    ax.legend(title="Depth class", frameon=False)
    return save(fig, "03_depth_vs_magnitude.png")


def plot_top_regions(by_region):
    """Q4: the ten regions with the most events."""
    top = by_region.head(10).iloc[::-1]          # biggest on top
    fig, ax = plt.subplots(figsize=(9, 5.5))
    bars = ax.barh(top.index, top["events"], color=MAIN_COLOR, height=0.7)
    for bar, (events, avg_mag) in zip(bars, top[["events", "avg_magnitude"]].values):
        ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
                f"{int(events)}  (avg M {avg_mag:.1f})", va="center", fontsize=9, color=TEXT_COLOR)
    style_axes(ax, "Top 10 regions by number of earthquakes", "Number of events", "")
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color="#e1e0d9", linewidth=0.8)
    ax.set_xlim(0, top["events"].max() * 1.25)
    return save(fig, "04_top_regions.png")


def plot_region_depth_heatmap(df, by_region):
    """Q4: pivot_table of event counts, top regions x depth class."""
    top_regions = by_region.head(10).index
    pivot = pd.pivot_table(df[df["region"].isin(top_regions)], index="region",
                           columns="depth_class", values="event_id",
                           aggfunc="count", fill_value=0, observed=False)
    pivot = pivot.loc[top_regions]
    print("\nPivot table: events by region and depth class\n" + pivot.to_string())
    fig, ax = plt.subplots(figsize=(9, 6))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="Blues", linewidths=2, linecolor="white",
                cbar_kws={"label": "Number of events"}, ax=ax)
    ax.set_title("Earthquakes by region and depth class", loc="left", fontsize=13, fontweight="bold")
    ax.set_xlabel("Depth class", color=TEXT_COLOR)
    ax.set_ylabel("Region", color=TEXT_COLOR)
    return save(fig, "05_region_depth_heatmap.png"), pivot


def plot_daily_activity(by_day):
    """Q5: number of events per day; days with an M6+ event are marked."""
    fig, ax = plt.subplots(figsize=(11, 5))
    days = pd.to_datetime(by_day.index)
    strong = by_day["max_magnitude"] >= 6
    ax.bar(days[~strong.values], by_day.loc[~strong, "events"], color="#86b6ef",
           width=0.8, label="no M6+ event that day")
    ax.bar(days[strong.values], by_day.loc[strong, "events"], color=MAIN_COLOR,
           width=0.8, label="day with an M6+ event")
    mean = by_day["events"].mean()
    ax.axhline(mean, color="#0b0b0b", linestyle="--", linewidth=1.2,
               label=f"daily mean = {mean:.0f}")
    peak_day = by_day["events"].idxmax()
    ax.annotate(f"{by_day['events'].max()} events, max M {by_day.loc[peak_day, 'max_magnitude']:.1f}",
                xy=(pd.to_datetime(peak_day), by_day["events"].max()),
                xytext=(0, 6), textcoords="offset points", ha="center", fontsize=9, color=TEXT_COLOR)
    ax.set_ylim(0, by_day["events"].max() * 1.3)
    style_axes(ax, "Daily number of earthquakes M2.5+, October 2026", "Date (UTC)", "Number of events")
    ax.legend(frameon=False, loc="upper left", ncol=3)
    fig.autofmt_xdate()
    return save(fig, "06_daily_activity.png")


# ---------- main function ----------

def run_analysis(db_path=None):
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    df = read_data(db_path)
    stats, correlation = describe_dataset(df)
    by_magnitude, by_depth, by_region, by_day = grouped_analysis(df)

    sns.set_theme(style="white")
    plt.rcParams.update({"font.size": 10})
    figures = [
        plot_magnitude_distribution(df),
        plot_epicenter_map(df),
        plot_depth_vs_magnitude(df),
        plot_top_regions(by_region),
    ]
    heatmap_path, pivot = plot_region_depth_heatmap(df, by_region)
    figures += [heatmap_path, plot_daily_activity(by_day)]

    # Key numbers for the report, so every number there can be reproduced
    strongest = df.loc[df["magnitude"].idxmax()]
    summary = {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "period": [str(df["event_time"].min()), str(df["event_time"].max())],
        "magnitude": {k: round(float(v), 2) for k, v in stats.loc["magnitude"].items()},
        "depth_km": {k: round(float(v), 2) for k, v in stats.loc["depth_km"].items()},
        "corr_magnitude_depth": float(correlation.loc["magnitude", "depth_km"]),
        "corr_magnitude_significance": float(correlation.loc["magnitude", "significance"]),
        "by_magnitude_class": by_magnitude.reset_index().astype({"magnitude_class": str}).to_dict("records"),
        "by_depth_class": by_depth.reset_index().astype({"depth_class": str}).to_dict("records"),
        "top_regions": by_region.head(10).reset_index().to_dict("records"),
        "regions_total": int(df["region"].nunique()),
        "events_per_day": {"mean": round(float(by_day["events"].mean()), 1),
                           "min": int(by_day["events"].min()),
                           "max": int(by_day["events"].max()),
                           "max_day": str(by_day["events"].idxmax())},
        "strongest_event": {"magnitude": float(strongest["magnitude"]), "place": strongest["place"],
                            "time": str(strongest["event_time"]), "depth_km": float(strongest["depth_km"])},
        "tsunami_flags": int(df["tsunami"].sum()),
        "alerts": df["alert"].fillna("none").value_counts().to_dict(),
        "figures": [str(path.relative_to(config.PROJECT_ROOT)) for path in figures],
    }
    summary_path = config.DATA_DIR / "eda_summary.json"
    with open(summary_path, "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, default=str, ensure_ascii=False)
    print(f"\nSaved {len(figures)} figures to {config.FIGURES_DIR.name}/ and key numbers to {summary_path.name}")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        run_analysis()
    except (FileNotFoundError, ValueError) as error:
        print(error)

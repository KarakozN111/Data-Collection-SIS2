# Module interfaces

`main.py` calls one function from each module. Every member implements the function in their file with exactly this name, input and output, so the modules connect without changing `main.py`.

| File | Function | Input | Output | Owner |
|---|---|---|---|---|
| `src/api_client.py` | `collect(start_time, end_time, min_magnitude)` | API parameters | `list[dict]` records, also saves `data/raw_data.csv` | Member 1 |
| `src/producer.py` | `send_records(records)` | `list[dict]` | `int` number of published messages | Member 2 |
| `src/consumer.py` | `consume_records(expected_count)` | `int` | `list[dict]` consumed records | Member 2 |
| `src/validator.py` | `validate(df)` | `pd.DataFrame` | `(valid_df, invalid_df)` | Member 3 |
| `src/cleaner.py` | `clean(df)` | `pd.DataFrame` (valid records) | `pd.DataFrame` clean data | Member 3 |
| `src/loader.py` | `load_to_db(df)` | `pd.DataFrame` | `int` number of rows in the table after loading | Member 4 |
| `src/analysis.py` | `run_analysis()` | reads `data/output.db` | saves figures to `figures/` | Member 4 |

## Shared settings

All paths and settings come from `src/config.py`. Do not write paths like `C:/Users/...`.

```python
from src import config

config.KAFKA_BOOTSTRAP_SERVERS   # "localhost:9092"
config.KAFKA_TOPIC               # "earthquakes"
config.RAW_CSV_PATH              # data/raw_data.csv
config.DB_PATH                   # data/output.db
config.FIGURES_DIR               # figures/
```

## Record format

Each record collected from the API (one Kafka message) is a dictionary with these keys. Values are kept exactly as the API returned them.

| Key | Type | Example | Meaning |
|---|---|---|---|
| `event_id` | str | `us7000qabc` | Unique USGS event id |
| `magnitude` | float | `4.6` | Earthquake magnitude |
| `mag_type` | str | `mb` | Magnitude type (mb, ml, mww, md...) |
| `place` | str | `45 km SW of Ovalle, Chile` | Text description of the location |
| `time_ms` | int | `1759363200000` | Event time, milliseconds since 1970-01-01 UTC |
| `updated_ms` | int | `1759449600000` | Last update time, milliseconds since 1970-01-01 UTC |
| `latitude` | float | `-30.9` | Latitude, -90 to 90 |
| `longitude` | float | `-71.5` | Longitude, -180 to 180 |
| `depth_km` | float | `35.0` | Depth in km |
| `event_type` | str | `earthquake` | Event type (earthquake, quarry blast, explosion...) |
| `status` | str | `reviewed` | `automatic` or `reviewed` |
| `tsunami` | int | `0` | 1 if a tsunami warning was issued |
| `significance` | int | `340` | USGS significance score, 0 to 1000+ |
| `alert` | str or null | `green` | PAGER alert level: green, yellow, orange, red |
| `network` | str | `us` | Seismic network that reported the event |
| `felt` | int or null | `12` | Number of "felt" reports from people |
| `cdi` | float or null | `3.4` | Community intensity |
| `mmi` | float or null | `4.1` | Modified Mercalli intensity |
| `gap` | float or null | `45` | Azimuthal gap in degrees |
| `rms` | float or null | `0.8` | Travel time residual |
| `nst` | int or null | `56` | Number of stations used |
| `dmin` | float or null | `1.2` | Distance to the nearest station in degrees |
| `title` | str | `M 4.6 - 45 km SW of Ovalle, Chile` | Event title |
| `url` | str | `https://earthquake.usgs.gov/...` | Event page |

## Columns expected by loader.py

`loader.py` stores these columns in the table `earthquakes` (see `COLUMNS` in `src/loader.py`):
all record keys above except `time_ms` / `updated_ms`, plus

| Column | Type in `clean_df` | Meaning |
|---|---|---|
| `event_time` | datetime (UTC) | Event time, made from `time_ms` |
| `updated_time` | datetime (UTC) | Last update, made from `updated_ms` |
| `region` | str | Last part of `place` after the comma, e.g. `Alaska` |

If `cleaner.py` does not create them, the loader builds them from `time_ms`, `updated_ms` and `place`.
Other extra columns of `clean_df` are not stored.


## Running one stage on its own

Every module can also be started separately from the project root, for example `python -m src.validator`.
Stages after Kafka read `data/raw_data.csv` when they are started on their own:

```python
import pandas as pd
from src import config
df = pd.read_csv(config.RAW_CSV_PATH)
```

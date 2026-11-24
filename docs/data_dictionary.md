# Data dictionary

Final clean dataset produced by `src/cleaner.py` (`clean(df)`), saved to `data/clean_data.csv` and loaded into SQLite by `src/loader.py`.
One row = one unique earthquake event. The raw record format (before cleaning) is described in `docs/interfaces.md`.

Source: USGS Earthquake Catalog API, https://earthquake.usgs.gov/fdsnws/event/1/

## Original columns (from the API, cleaned)

| Column | Type | Group | Meaning | Example | Cleaning applied |
|---|---|---|---|---|---|
| `event_id` | text | identifier | Unique USGS event id, primary key | `ci41341279` | Duplicates removed (latest `updated_time` kept) |
| `magnitude` | float | numerical | Earthquake magnitude | `3.08` | Validated range -1..10 |
| `mag_type` | text | categorical | Magnitude type: `mb`, `ml`, `mww`, `md`, `mwr`... | `ml` | Trimmed, lower case |
| `place` | text | text | Location description from USGS | `20 km ESE of Anza, CA` | Extra spaces removed; 1 missing value -> `Unknown location` |
| `latitude` | float | numerical | Latitude in degrees, -90..90 | `33.5023` | Validated range |
| `longitude` | float | numerical | Longitude in degrees, -180..180 | `-116.4633` | Validated range |
| `depth_km` | float | numerical | Depth in km (negative = above sea level) | `5.13` | Validated range -10..800 |
| `event_type` | text | categorical | `earthquake`, `explosion`, `landslide`, `quarry_blast`... | `earthquake` | Lower case, spaces -> `_` |
| `status` | text | categorical | `reviewed` (checked by a seismologist) or `automatic` | `reviewed` | Lower case |
| `tsunami` | integer | categorical (0/1) | 1 = tsunami information was issued | `0` | Validated values 0/1 |
| `significance` | integer | numerical | USGS significance score (magnitude, felt reports, damage) | `149` | Must be >= 0 |
| `alert` | text | categorical | PAGER alert level: `green`, `yellow`, `orange`, `red`, `none` | `none` | Missing -> `none` (no alert was issued) |
| `network` | text | categorical | Seismic network that reported the event (`us`, `ak`, `ci`...) | `ci` | Lower case |
| `felt` | integer | numerical | Number of "Did you feel it?" reports | `8` | Missing -> 0 (no reports) |
| `cdi` | float | numerical | Community intensity from felt reports, 0..12 | `4.1` | Missing values kept (NULL): exists only for felt events |
| `mmi` | float | numerical | Modified Mercalli intensity (ShakeMap), 0..12 | NULL | Missing values kept (NULL): exists only for strong events |
| `gap` | float | numerical | Azimuthal gap between stations, degrees 0..360 | `21.0` | Missing -> median of the same network |
| `rms` | float | numerical | Travel time residual, seconds | `0.17` | Missing -> median of the same network |
| `nst` | integer | numerical | Number of stations used | `115` | Missing -> median of the same network |
| `dmin` | float | numerical | Distance to the nearest station, degrees | `0.1163` | Missing -> median of the same network |
| `title` | text | text | Event title | `M 3.1 - 20 km ESE of Anza, CA` | Trimmed |
| `url` | text | text | Event page on the USGS site | `https://earthquake.usgs.gov/...` | Must start with `https://` |
| `event_time` | datetime (UTC) | date/time | Time of the earthquake | `2026-10-01 00:01:59.630+00:00` | Converted from `time_ms` (ms since 1970) |
| `updated_time` | datetime (UTC) | date/time | Last update of the record by USGS | `2026-10-21 00:56:54.040+00:00` | Converted from `updated_ms`; must be >= `event_time` |

## New columns (created in `cleaner.py`)

| Column | Type | Group | Meaning | Example | How it is calculated |
|---|---|---|---|---|---|
| `region` | text | categorical | Country / region of the event | `California` | Text after the last comma in `place`; `" region"` suffix removed; US state codes replaced (`CA` -> `California`) |
| `energy_joules` | float | numerical | Released seismic energy, J | `2.63e9` | NumPy: `10 ** (1.5 * magnitude + 4.8)` (Gutenberg-Richter) |
| `log10_energy` | float | numerical | log10 of the energy, easier to compare | `9.42` | NumPy: `np.log10(energy_joules)` |
| `depth_class` | text | categorical | `shallow` (< 70 km), `intermediate` (70-300 km), `deep` (> 300 km) | `shallow` | NumPy: `np.select` |
| `magnitude_class` | text | categorical | `minor` (< 4), `light` (4-4.9), `moderate` (5-5.9), `strong` (>= 6) | `minor` | NumPy: `np.select` |
| `magnitude_zscore` | float | numerical | How many standard deviations the magnitude is from the mean of the period | `-0.968` | NumPy: `(m - np.mean(m)) / np.std(m)` |
| `hemisphere` | text | categorical | `north` or `south` | `north` | NumPy: `np.where(latitude >= 0, ...)` |
| `is_strong` | integer | categorical (0/1) | 1 if magnitude >= 5 | `0` | NumPy: `np.where(magnitude >= 5, 1, 0)` |

## Removed columns

| Column | Reason |
|---|---|
| `time_ms`, `updated_ms` | Replaced by `event_time` and `updated_time` (same information, readable format) |
| `is_test_record` | Helper flag of the validation stage; test records never reach the clean dataset |

## Files of the validation and cleaning stage

| File | Content |
|---|---|
| `data/valid_records.csv` | Records that passed Pandera validation |
| `data/invalid_records.csv` | Rejected records (incl. 4 `TEST_INVALID_xx` test records) with the column `validation_errors` |
| `data/clean_data.csv` | Final clean dataset described above |
| `data/transformations/missing_values.csv` | Missing values per column before and after cleaning |
| `data/transformations/region_summary.csv` | GroupBy by region: count, mean/max magnitude, mean depth, strong events, total energy |
| `data/transformations/network_summary.csv` | GroupBy by network: count, mean magnitude, share of reviewed events |
| `data/transformations/pivot_region_depth.csv` | `pivot_table`: top-10 regions x depth class |
| `data/transformations/strong_shallow.csv` | Filter: magnitude >= 5 AND depth < 70 km AND event_type = earthquake |

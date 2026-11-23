# Analysis of Seismic Activity Based on Earthquake Data

SIS2 project for the Data Collection & Preparation course: an end-to-end data pipeline with Apache Kafka and Pandera.

## Project overview

The pipeline collects real earthquake records from the USGS Earthquake Catalog API, streams them through Apache Kafka, validates them with Pandera, cleans and transforms them with pandas and NumPy, stores them in SQLite3 and analyzes them with Matplotlib and Seaborn.

```
USGS API -> api_client.py -> data/raw_data.csv (original data)
         -> producer.py   -> Kafka topic "earthquakes" -> consumer.py
         -> validator.py  (Pandera)
         -> cleaner.py    (pandas, NumPy)
         -> loader.py     -> data/output.db (SQLite3)
         -> analysis.py   -> figures/*.png
```

## Group members

| Member | Responsibility |
|---|---|
| Member 1 | API data collection, `main.py`, configuration, README, report assembly |
| Member 2 | Kafka Broker, Topic, Producer and Consumer |
| Member 3 | Pandera validation, data cleaning and transformation, data dictionary |
| Member 4 | SQLite3 storage and idempotency, EDA and visualizations |

## Data source

- **API:** USGS Earthquake Catalog (FDSN Event Web Service), no API key required
- **Endpoint:** `https://earthquake.usgs.gov/fdsnws/event/1/query`
- **Documentation:** https://earthquake.usgs.gov/fdsnws/event/1/

| Parameter | Value | Meaning |
|---|---|---|
| `format` | `geojson` | Response format |
| `starttime` | `2026-10-01` | Start of the period (UTC) |
| `endtime` | `2026-11-01` | End of the period (UTC) |
| `minmagnitude` | `2.5` | Minimum magnitude |
| `orderby` | `time-asc` | Oldest events first |
| `limit` | `20000` | Maximum number of events (API limit) |

Example request:
```
https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-10-01&endtime=2026-11-01&minmagnitude=2.5&orderby=time-asc
```

Each record has numerical fields (magnitude, depth, coordinates, significance), categorical fields (event type, magnitude type, status, alert level, network) and date/time fields (event time, update time). The full list of fields is in `docs/interfaces.md` and `docs/data_dictionary.md`.

## Requirements

- Python 3.10 or newer
- Docker Desktop (for the Kafka Broker)
- Internet connection (for the API request)

## Installation

```bash
git clone <repository-url>
cd seismic-pipeline

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Optional: copy `.env.example` to `.env` to change the date range, magnitude or Kafka address. Without `.env` the default values from `src/config.py` are used.

## Kafka setup

Start the Kafka Broker:
```bash
docker compose up -d
docker ps                          # the kafka container must be "Up"
```

Create the topic:
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic earthquakes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

<!-- TODO (Member 2): check the container name and the kafka-topics.sh path against docker-compose.yml -->

Stop Kafka after work:
```bash
docker compose down
```

## Running the pipeline

### Full pipeline
```bash
python main.py
```

With other API parameters:
```bash
python main.py --start 2026-09-01 --end 2026-10-01 --min-magnitude 3
```

### Each stage separately

Run the commands in this order from the project root:

```bash
python -m src.api_client          # 1. collect data -> data/raw_data.csv
python -m src.producer            # 2. send records to Kafka
python -m src.consumer            # 3. read records from Kafka
python -m src.validator           # 4. Pandera validation
python -m src.cleaner             # 5. cleaning and transformation
python -m src.loader              # 6. load into data/output.db
python -m src.analysis            # 7. EDA and figures
```

<!-- TODO (Members 2-4): check that each module can be started on its own with these commands -->

The Producer and the Consumer can also be run in two terminals: start the Consumer first, then the Producer in the second terminal.

## Expected outputs

| File | Created by | Content |
|---|---|---|
| `data/raw_response.json` | `api_client.py` | Original API response, unchanged |
| `data/raw_data.csv` | `api_client.py` | Collected records before cleaning |
| `data/output.db` | `loader.py` | SQLite3 database with cleaned records |
| `data/pipeline_summary.json` | `main.py` | Record counts for every stage |
| `figures/*.png` | `analysis.py` | Visualizations |

At the end `main.py` prints a summary:
```
===== Pipeline summary =====
Collected from API          ...
Published to Kafka          ...
Consumed from Kafka         ...
Valid records               ...
Invalid records             ...
Records after cleaning      ...
Stored in SQLite (load 1)   ...
Stored in SQLite (load 2)   ...
Idempotency check: OK, no duplicates after repeated loading
```

## Verifying the results

1. Published and consumed counts are equal.
2. Invalid records include the test records created in `validator.py`.
3. The row count is the same after the first and the second load (idempotency).
4. SQL verification queries run without errors:
   ```bash
   sqlite3 data/output.db < sql/verification_queries.sql
   ```
5. The `figures/` folder contains the generated plots.

## Project structure

```
seismic-pipeline/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── main.py
├── src/
│   ├── config.py
│   ├── api_client.py
│   ├── producer.py
│   ├── consumer.py
│   ├── validator.py
│   ├── cleaner.py
│   ├── loader.py
│   └── analysis.py
├── sql/
│   └── verification_queries.sql
├── data/
├── figures/
├── docs/
│   ├── interfaces.md
│   └── data_dictionary.md
└── report/
    └── project_report.pdf
```

## Troubleshooting

| Problem | Solution |
|---|---|
| `Module src/producer.py is missing` | This part is not added yet. Use `python main.py --skip-kafka` for development |
| Kafka connection error | Check that Docker is running and `docker ps` shows the kafka container |
| `API returned 0 records` | Increase the date range or decrease `--min-magnitude` |
| `HTTP 400` from the API | Check the date format, it must be `YYYY-MM-DD` |

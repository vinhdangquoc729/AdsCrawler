# Marketing Analytics Pipeline

A Modern Data Stack for Facebook Ads analytics. Generates deterministic mock ad performance data, lands it in a data lake, transforms it with Spark, and serves it through a ClickHouse data warehouse to Superset dashboards.

## Architecture

<img width="1565" height="647" alt="image" src="https://github.com/user-attachments/assets/87539a83-c562-4959-96d6-9aa9829b2f31" />

**11 Docker containers total:**

| Container | Role |
|---|---|
| `mkt_airflow_webserver` | DAG UI & trigger |
| `mkt_airflow_scheduler` | Daily schedule runner |
| `mkt_airflow_init` | One-shot DB migration |
| `mkt_spark_master` | Spark cluster master |
| `mkt_spark_worker` | Spark executor |
| `mkt_clickhouse` | Columnar data warehouse |
| `mkt_minio` | S3-compatible data lake |
| `mkt_minio_init` | One-shot bucket creation |
| `mkt_kafka` | Message broker (KRaft mode) |
| `mkt_superset` | BI dashboards |
| `mkt_postgres` | Airflow metadata DB |

## Data Pipeline

The `marketing_data_pipeline` DAG runs two tasks in sequence:

**Task 1 — `mock_generation_task`**

Runs `python -m ingest.facebook.main --mode mock`. Generates deterministic mock Facebook Ads data and uploads 8 JSON files to MinIO:

| MinIO Path | Content |
|---|---|
| `fad_ad_daily_report/` | Ad-level daily performance (primary source) |
| `fad_age_gender_detailed_report/` | Same data broken down by age/gender |
| `fad_adset_daily_report/` | Ad set level aggregations |
| `fad_campaign_daily_report/` | Campaign level aggregations |
| `fad_account_daily_report/` | Account level aggregations |
| `fad_ad_performance_report/` | Lifetime ad performance |
| `fad_adset_performance_report/` | Lifetime ad set performance |
| `fad_campaign_overview_report/` | Lifetime campaign performance |

Each file lands at: `{table}/{YYYY}/{MM}/{DD}/{HH}/{mm}/{user}_{timestamp}_{uuid}.json`

**Task 2 — `minio_to_clickhouse_ingest`**

Submits `spark_consumer/minio_ingest.py` to the Spark cluster. Reads from MinIO and loads into ClickHouse using JDBC.

## Project Structure

```
AdsCrawler_test/
├── airflow/
│   └── dags/
│       └── mkt_pipeline_dag.py     # DAG definition
├── ingest/
│   ├── facebook/
│   │   ├── mock.py                 # Deterministic mock data generator
│   │   ├── main.py                 # Ingestion entry point (mock/real)
│   │   └── crawler.py              # Facebook Ads crawler
│   └── utils/
│       └── minio_client.py         # MinIO upload client
├── spark_consumer/
│   ├── minio_ingest.py             # MinIO → ClickHouse Spark job (active)
│   ├── facebook_processor.py       # Kafka → ClickHouse processor (future)
│   ├── base_processor.py           # JDBC writer base class
│   └── main.py                     # Kafka consumer entry point (future)
├── superset/
│   └── SQL_dashboard.sql           # Dashboard query definitions
├── init_clickhouse.sql             # ClickHouse table DDL
├── schema_visualization.md         # ERD diagram
├── Dockerfile.airflow              # Airflow image with Java/Spark/JARs
├── Dockerfile.superset             # Superset image with ClickHouse driver
└── docker-compose.yml
```

## Setup

**Prerequisites:** Docker Desktop with WSL2, minimum 12 GB RAM allocated to Docker.

**1. Create required directories**
```bash
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/.ivy2
```

**2. Build and start all services**
```bash
docker-compose up -d --build
```

> If the build fails downloading Spark dependencies, pre-download [spark-3.5.1-bin-hadoop3.tgz](https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz) and place it in the project root.

**3. Wait for Airflow initialization to complete**
```bash
docker logs -f mkt_airflow_init
```
Wait until the container exits with code 0.

**4. Create the Kafka topic**
```bash
docker exec -it mkt_kafka kafka-topics --create --topic topic_fb_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Service URLs

| Service | URL | Username | Password |
|---|---|---|---|
| Airflow | http://localhost:8082 | admin | password123 |
| Superset | http://localhost:8088 | admin | password123 |
| MinIO Console | http://localhost:9006 | admin | password123 |
| Spark Master | http://localhost:8081 | — | — |
| ClickHouse HTTP | http://localhost:8123 | admin | password123 |

## Running the Pipeline

1. Open Airflow at http://localhost:8082
2. Enable and trigger the `marketing_data_pipeline` DAG
3. Task 1 generates mock data and uploads to MinIO (~1 min)
4. Task 2 runs the Spark job to load ClickHouse (~3-5 min)
5. Query results in Superset at http://localhost:8088 or directly in ClickHouse

To connect Superset to ClickHouse: **Settings → Database Connections → + Database → ClickHouse** with URI `clickhouse+http://admin:password123@clickhouse:8123/marketing_db`.

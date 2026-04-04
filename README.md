# Marketing Data Pipeline (AdsCrawler Test)

This project implements an automated, end-to-end Modern Data Stack for marketing analytics. It orchestrates the flow of data from raw JSON sources into a distributed infrastructure comprising a Message Broker, Data Lake, and Data Warehouse.

## 1. System Architecture
<img width="1529" height="679" alt="image" src="https://github.com/user-attachments/assets/d95976c1-37a1-41f8-879e-47f3720e5cd4" />

The pipeline is composed of 11 Docker containers working in synchronization:
- Orchestration: Apache Airflow (Webserver, Scheduler, and Init) manages the workflow.
- Data Ingestion: A Python-based Kafka Producer that crawls raw files.
- Message Broker: Apache Kafka (KRaft mode) handles real-time data streams.
- Processing Engine: Apache Spark (Master and Worker) performs ETL and schema mapping.
- Data Warehouse: ClickHouse stores processed data for high-performance analytics.
- Visualization: Apache Superset provides dashboards for marketing insights.

## 2. Project Structure
The project is organized into modular components as seen in the repository:

```
AdsCrawler_test/
├── airflow/
│   ├── dags/             # Airflow DAG definitions (mkt_pipeline_dag.py)
│   ├── logs/             # Operational logs
│   └── .ivy2/            # Cached Spark dependencies
├── kafka_ingestion/      # Python module for loading data into Kafka
│   ├── base.py           # Producer base class
│   ├── filecrawler.py    # Sample crawling logic (scanning JSON files), should be extended for real APIs 
│   └── main.py           # Ingestion entry point
├── spark_consumer/       # Spark processing module
│   ├── base_processor.py # JDBC ClickHouse writer
│   ├── facebook_processor.py # Sample processor for Facebook Ads data, should be extended for other sources
│   └── main.py           # Spark application entry point
├── Dockerfile.airflow    # Custom Airflow image with Java/Spark/JARs
├── Dockerfile.superset   # Custom Superset image with ClickHouse driver
└── docker-compose.yml    # Main infrastructure configuration
```

## 3. Installation & Setup
**Prerequisites**
- Docker Desktop installed and configured with WSL2.
- System Resources: Minimum 12GB RAM allocated to Docker.

**Step 1: Initialize Folders**
Ensure the local directories exist for volume mapping:
```bash
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/.ivy2
```

**Step 2: Build and Deploy**
Build the custom images and start all services:

```bash
docker-compose up -d --build
```

**Step 3: Verify Initialization**
Monitor the Airflow initialization process:

```bash
docker logs -f mkt_airflow_init
```

**Step 4: Infrastructure Setup (Kafka Topics)**
Before running the pipeline for the first time, you must manually create the required Kafka topic so that the Spark consumer can listen to it without errors.

Run the following command from your host terminal:
```bash
docker exec -it mkt_kafka kafka-topics --create \
    --topic topic_fb_raw \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```

The system is ready once this container exits with code 0.

## 4. Service Access (Credentials)
| Service         | URL                       | Username | Password    |
|-----------------|---------------------------|----------|-------------|
| Airflow UI      | http://localhost:8082     | admin    | password123 |
| Superset UI     | http://localhost:8088     | admin    | password123 |
| MinIO Console   | http://localhost:9006     | admin    | password123 |
| Spark Master    | http://localhost:8081     | N/A      | N/A         |
| ClickHouse      | http://localhost:8123     | admin    | password123 |

## 5. Usage Guide
The pipeline follows a Batch-on-Streaming logic orchestrated by the `marketing_data_pipeline` DAG.

### 1. Trigger the DAG: Access the Airflow UI and trigger `marketing_data_pipeline`.

### 2. Data Ingestion (`t1_ingestion`): This task runs `kafka_ingestion/main.py`, which reads local JSON data and pushes it to the Kafka topic `topic_fb_raw`.

### 3. Data Processing (`t2_spark_consumer`): This task submits the Spark job using `spark-submit`.

- Kafka Consumption: Spark reads the stream from Kafka topic `topic_fb_raw`.

- Data Lake Archival: Raw data is partitioned by platform and report type and saved as Parquet in MinIO (`s3a://marketing-datalake/raw_zone/`).

- Warehouse Loading: Data is transformed into a Star Schema and written to ClickHouse tables via JDBC.

- Termination: Using the `.trigger(availableNow=True)` setting, the Spark job processes all available data and terminates, allowing Airflow to mark the task as successful.

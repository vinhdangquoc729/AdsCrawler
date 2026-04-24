# airflow/dags/mkt_pipeline_dag.py
# Lambda architecture pipeline:
#   Batch path : producers → topic_batch_requests → consumer → MinIO → Spark batch → ClickHouse
#   Stream path: producers → topic_fb_raw / topic_google_raw → Spark streaming → ClickHouse

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

_SETUP = (
    "pip install --quiet minio openpyxl pandas confluent-kafka && "
    "cd /opt/spark/work-dir && "
    "export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir && "
    "export MINIO_ENDPOINT=minio:9000 && "
    "export MINIO_ACCESS_KEY=admin && "
    "export MINIO_SECRET_KEY=password123 && "
    "export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 && "
)

_SPARK_BATCH_JARS = (
    "/opt/airflow/jars/clickhouse-jdbc.jar,"
    "/opt/airflow/jars/hadoop-aws.jar,"
    "/opt/airflow/jars/aws-java-sdk-bundle.jar,"
    "/opt/airflow/jars/commons-pool2.jar"
)

_SPARK_STREAM_JARS = (
    "/opt/airflow/jars/spark-sql-kafka.jar,"
    "/opt/airflow/jars/kafka-clients.jar,"
    "/opt/airflow/jars/clickhouse-jdbc.jar,"
    "/opt/airflow/jars/hadoop-aws.jar,"
    "/opt/airflow/jars/aws-java-sdk-bundle.jar,"
    "/opt/airflow/jars/commons-pool2.jar,"
    "/opt/airflow/jars/spark-token-provider-kafka.jar"
)

with DAG(
    'marketing_data_pipeline',
    default_args=default_args,
    description='Lambda pipeline: batch (MinIO→ClickHouse) + stream (Kafka→ClickHouse)',
    schedule_interval='@daily',
    catchup=False,
    tags=['marketing', 'minio', 'kafka', 'mock']
) as dag:

    # ── BATCH PATH ─────────────────────────────────────────────────────────────
    # Workers publish job requests to Kafka; consumer triggers ingest → MinIO;
    # Spark batch job reads MinIO and loads to ClickHouse.

    t0_batch_produce_facebook = BashOperator(
        task_id='batch_produce_facebook',
        bash_command=f"""
            {_SETUP}python3 -m ingest.workers.batch_producer --platform facebook
        """
    )

    t0_batch_produce_google = BashOperator(
        task_id='batch_produce_google',
        bash_command=f"""
            {_SETUP}python3 -m ingest.workers.batch_producer --platform google
        """
    )

    t1_batch_consume = BashOperator(
        task_id='batch_consume',
        bash_command=f"""
            {_SETUP}python3 -m ingest.workers.batch_consumer
        """
    )

    t2_minio_to_clickhouse = BashOperator(
        task_id='minio_to_clickhouse_ingest',
        bash_command=f"""
            spark-submit --master spark://spark-master:7077 \
            --jars {_SPARK_BATCH_JARS} \
            /opt/spark/work-dir/spark_consumer/minio_ingest.py
        """
    )

    # ── STREAM PATH ────────────────────────────────────────────────────────────
    # Workers publish ad records directly to Kafka stream topics;
    # Spark Structured Streaming reads and writes to ClickHouse.

    t0_stream_produce_facebook = BashOperator(
        task_id='stream_produce_facebook',
        bash_command=f"""
            {_SETUP}python3 -m ingest.workers.stream_producer --platform facebook
        """
    )

    t0_stream_produce_google = BashOperator(
        task_id='stream_produce_google',
        bash_command=f"""
            {_SETUP}python3 -m ingest.workers.stream_producer --platform google
        """
    )

    t1_spark_stream = BashOperator(
        task_id='spark_stream_to_clickhouse',
        bash_command=f"""
            spark-submit --master spark://spark-master:7077 \
            --jars {_SPARK_STREAM_JARS} \
            /opt/spark/work-dir/spark_consumer/main.py
        """
    )

    # ── DEPENDENCIES ───────────────────────────────────────────────────────────

    [t0_batch_produce_facebook, t0_batch_produce_google] >> t1_batch_consume >> t2_minio_to_clickhouse
    [t0_stream_produce_facebook, t0_stream_produce_google] >> t1_spark_stream

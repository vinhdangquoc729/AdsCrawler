# airflow/dags/mkt_pipeline_dag.py
# This DAG defines a marketing data pipeline that automates the ingestion of mock data to MinIO.

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

with DAG(
    'marketing_data_pipeline',
    default_args=default_args,
    description='Automated pipeline for landing mock marketing data in MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['marketing', 'minio', 'mock']
) as dag:

    t0_mock_generation = BashOperator(
        task_id='mock_generation_task',
        bash_command="""
            pip install --quiet minio openpyxl pandas &&
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir/kafka_ingestion &&
            export MINIO_ENDPOINT=minio:9000 &&
            export MINIO_ACCESS_KEY=admin &&
            export MINIO_SECRET_KEY=password123 &&
            python3 /opt/spark/work-dir/kafka_ingestion/run_mock.py
        """
    )

    t1_minio_ingest = BashOperator(
        task_id='minio_to_clickhouse_ingest',
        bash_command="""
            spark-submit --master spark://spark-master:7077 \
            --jars /opt/airflow/jars/clickhouse-jdbc.jar,\
/opt/airflow/jars/hadoop-aws.jar,\
/opt/airflow/jars/aws-java-sdk-bundle.jar,\
/opt/airflow/jars/commons-pool2.jar \
            /opt/spark/work-dir/spark_consumer/minio_ingest.py
        """
    )

    # t2_spark_processor = BashOperator(
    #     task_id='spark_batch_processor',
    #     bash_command="""
    #         spark-submit --master spark://spark-master:7077 \
    #         --jars /opt/airflow/jars/spark-sql-kafka.jar,\
    # /opt/airflow/jars/kafka-clients.jar,\
    # /opt/airflow/jars/clickhouse-jdbc.jar,\
    # /opt/airflow/jars/hadoop-aws.jar,\
    # /opt/airflow/jars/aws-java-sdk-bundle.jar,\
    # /opt/airflow/jars/commons-pool2.jar,\
    # /opt/airflow/jars/spark-token-provider-kafka.jar \
    #         /opt/spark/work-dir/spark_consumer/main.py
    #     """
    # )

    # t0_mock_generation >> t1_minio_ingest >> t2_spark_processor
    t0_mock_generation >> t1_minio_ingest
# airflow/dags/mkt_spark_ingest_dag.py
# This DAG is responsible ONLY for submitting the Spark Batch job
# to ingest data from MinIO into ClickHouse.

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
    'marketing_spark_ingestion',
    default_args=default_args,
    description='Automated pipeline: Spark Batch Ingest (MinIO → ClickHouse)',
    schedule_interval='@daily',
    catchup=False,
    tags=['marketing', 'spark', 'minio', 'clickhouse']
) as dag:

    t1_minio_ingest = BashOperator(
        task_id='minio_to_clickhouse_ingest',
        bash_command="""
            spark-submit --master spark://spark-master:7077 \
            --conf spark.cores.max=8 \
            --conf spark.executor.memory=1g \
            --jars /opt/airflow/jars/clickhouse-jdbc.jar,\
/opt/airflow/jars/hadoop-aws.jar,\
/opt/airflow/jars/aws-java-sdk-bundle.jar,\
/opt/airflow/jars/commons-pool2.jar \
            /opt/spark/work-dir/spark_consumer/minio_ingest.py \
            --date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}
        """
    )

    t1_minio_ingest

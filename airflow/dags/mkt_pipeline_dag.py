# airflow/dags/mkt_pipeline_dag.py
# This DAG defines a marketing data pipeline that automates the ingestion of data from Kafka and processing it with Spark.

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
    description='Automated pipeline for ingesting marketing data from Kafka and processing with Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=['marketing', 'kafka', 'spark']
) as dag:

    t1_ingestion = BashOperator(
        task_id='kafka_ingestion_task',
        bash_command="""
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir && \
            python3 /opt/spark/work-dir/kafka_ingestion/main.py
        """
    )

    t2_spark_consumer = BashOperator(
        task_id='spark_consumer_task',
        bash_command="""
            spark-submit --master spark://spark-master:7077 \
            --jars /opt/airflow/jars/spark-sql-kafka.jar,\
/opt/airflow/jars/kafka-clients.jar,\
/opt/airflow/jars/clickhouse-jdbc.jar,\
/opt/airflow/jars/hadoop-aws.jar,\
/opt/airflow/jars/aws-java-sdk-bundle.jar,\
/opt/airflow/jars/commons-pool2.jar,\
/opt/airflow/jars/spark-token-provider-kafka.jar \
            /opt/spark/work-dir/spark_consumer/main.py
        """
    )

    # Thứ tự: Ingestion xong mới đến Spark Consumer
    t1_ingestion >> t2_spark_consumer
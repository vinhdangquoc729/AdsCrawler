# airflow/dags/mkt_mock_data_dag.py
# This DAG is responsible ONLY for generating mock data and publishing it to Kafka

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
    'marketing_mock_data_generation',
    default_args=default_args,
    description='Automated pipeline: Mock Data Generation → Kafka',
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['marketing', 'kafka', 'mock']
) as dag:

    t_setup_deps = BashOperator(
        task_id='setup_dependencies',
        bash_command="pip install --quiet --no-cache-dir minio openpyxl pandas confluent-kafka"
    )

    t0_mock_facebook = BashOperator(
        task_id='mock_generation_facebook',
        bash_command="""
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir &&
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 &&
            python3 -m ingest.facebook.main --mode mock --output kafka \
                --start-date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }} --end-date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}
        """
    )

    t0_mock_google = BashOperator(
        task_id='mock_generation_google',
        bash_command="""
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir &&
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 &&
            python3 -m ingest.google.main --mode mock --output kafka \
                --start-date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }} --end-date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}
        """
    )

    t0_mock_tiktok = BashOperator(
        task_id='mock_generation_tiktok',
        bash_command="""
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir &&
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 &&
            python3 -m ingest.tiktok.main --mode mock --output kafka \
                --start-date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }} --end-date {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}
        """
    )

    # Wait for Kafka Connect to flush data to MinIO so it is ready for Spark
    t_wait_flush = BashOperator(
        task_id='wait_kafka_connect_flush',
        bash_command="echo 'Waiting 90s for Kafka Connect to flush to MinIO...' && sleep 90"
    )

    # DAG: Setup deps -> Mock → Kafka → (wait flush)
    t_setup_deps >> [t0_mock_facebook, t0_mock_google, t0_mock_tiktok] >> t_wait_flush

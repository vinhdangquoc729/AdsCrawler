# airflow/dags/mkt_pipeline_dag.py
# This DAG defines a marketing data pipeline that automates the ingestion of mock data
# through Kafka → Kafka Connect → MinIO → Spark → ClickHouse.

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
    description='Automated pipeline: Mock → Kafka → MinIO → Spark → ClickHouse',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['marketing', 'kafka', 'minio', 'mock']
) as dag:

    t0_mock_facebook = BashOperator(
        task_id='mock_generation_facebook',
        bash_command="""
            pip install --quiet minio openpyxl pandas confluent-kafka &&
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir &&
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 &&
            python3 -m ingest.facebook.main --mode mock --output kafka \
                --start-date {{ ds }} --end-date {{ ds }}
        """
    )

    t0_mock_google = BashOperator(
        task_id='mock_generation_google',
        bash_command="""
            pip install --quiet minio openpyxl pandas confluent-kafka &&
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir &&
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 &&
            python3 -m ingest.google.main --mode mock --output kafka \
                --start-date {{ ds }} --end-date {{ ds }}
        """
    )

    t0_mock_tiktok = BashOperator(
        task_id='mock_generation_tiktok',
        bash_command="""
            pip install --quiet minio openpyxl pandas confluent-kafka &&
            cd /opt/spark/work-dir &&
            export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir &&
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 &&
            python3 -m ingest.tiktok.main --mode mock --output kafka \
                --start-date {{ ds }} --end-date {{ ds }}
        """
    )

    # Wait for Kafka Connect to flush data to MinIO
    t_wait_flush = BashOperator(
        task_id='wait_kafka_connect_flush',
        bash_command="echo 'Waiting 90s for Kafka Connect to flush to MinIO...' && sleep 90"
    )

    # Pause speed-layer to free up Spark Worker core for batch ingest
    t_pause_speed_layer = BashOperator(
        task_id='pause_speed_layer',
        bash_command="""
            TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token) && \
            curl -s -k -o /dev/null -w '%{http_code}' \
              -X PATCH https://kubernetes.default.svc/apis/apps/v1/namespaces/marketing/deployments/speed-layer \
              -H "Authorization: Bearer $TOKEN" \
              -H 'Content-Type: application/strategic-merge-patch+json' \
              -d '{"spec":{"replicas":0}}' && \
            echo ' speed-layer scaled to 0' && sleep 15
        """
    )

    t1_minio_ingest = BashOperator(
        task_id='minio_to_clickhouse_ingest',
        bash_command="""
            DRIVER_IP=$(hostname -i) && \
            spark-submit --master spark://spark-master:7077 \
            --conf spark.driver.host=$DRIVER_IP \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.driver.memory=512m \
            --conf spark.cores.max=2 \
            --conf spark.executor.memory=512m \
            --jars /opt/airflow/jars/clickhouse-jdbc.jar,/opt/airflow/jars/hadoop-aws.jar,/opt/airflow/jars/aws-java-sdk-bundle.jar,/opt/airflow/jars/commons-pool2.jar \
            /opt/spark/work-dir/spark_consumer/minio_ingest.py \
            --date {{ ds }}
        """
    )

    # Resume speed-layer after ingest (even if ingest failed)
    t_resume_speed_layer = BashOperator(
        task_id='resume_speed_layer',
        bash_command="""
            TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token) && \
            curl -s -k -o /dev/null -w '%{http_code}' \
              -X PATCH https://kubernetes.default.svc/apis/apps/v1/namespaces/marketing/deployments/speed-layer \
              -H "Authorization: Bearer $TOKEN" \
              -H 'Content-Type: application/strategic-merge-patch+json' \
              -d '{"spec":{"replicas":1}}' && \
            echo ' speed-layer scaled to 1'
        """,
        trigger_rule='all_done'
    )

    # DAG: Mock → Kafka → wait flush → pause speed-layer → Spark ingest → resume speed-layer
    [t0_mock_facebook, t0_mock_google, t0_mock_tiktok] >> t_wait_flush >> t_pause_speed_layer >> t1_minio_ingest >> t_resume_speed_layer

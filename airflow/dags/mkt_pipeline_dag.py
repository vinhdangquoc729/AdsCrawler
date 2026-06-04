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
                --start-date {{ ds }} --end-date {{ ds }}
        """
    )

    t0_mock_google = BashOperator(
        task_id='mock_generation_google',
        bash_command="""
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

    # Spark Worker có 2 core: speed-layer dùng 1 core (spark.cores.max=1),
    # batch ingest dùng 1 core còn lại — chạy song song, không cần pause/resume.
    t1_minio_ingest = BashOperator(
        task_id='minio_to_clickhouse_ingest',
        bash_command="""
            DRIVER_IP=$(hostname -i) && \
            spark-submit --master spark://spark-master:7077 \
            --conf spark.driver.host=$DRIVER_IP \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.driver.memory=512m \
            --conf spark.cores.max=1 \
            --conf spark.executor.memory=512m \
            --jars /opt/airflow/jars/clickhouse-jdbc.jar,/opt/airflow/jars/hadoop-aws.jar,/opt/airflow/jars/aws-java-sdk-bundle.jar,/opt/airflow/jars/commons-pool2.jar \
            /opt/spark/work-dir/spark_consumer/minio_ingest.py \
            --date {{ ds }}
        """
    )

    # Sau khi Spark ghi xong, chạy OPTIMIZE FINAL trên tất cả dimension tables.
    # Dimension tables dùng ReplacingMergeTree(updated_at) — merge là async.
    # OPTIMIZE FINAL ép ClickHouse deduplicate ngay lập tức, đảm bảo
    # Superset query không thấy duplicate dù Spark vừa retry.
    t2_optimize_dims = BashOperator(
        task_id='optimize_clickhouse_dims',
        bash_command="""
            DIM_TABLES="dim_account dim_campaign dim_adset dim_ad dim_creative dim_date \
                        dim_gg_campaign dim_gg_adgroup dim_gg_asset \
                        dim_tta_advertiser dim_tta_ad"
            for TABLE in $DIM_TABLES; do
                echo "OPTIMIZE $TABLE..."
                curl -sf -u admin:password123 http://clickhouse:8123/ \
                    --data "OPTIMIZE TABLE marketing_db.${TABLE} FINAL DEDUPLICATE" \
                    || echo "  [WARN] OPTIMIZE failed for $TABLE (non-fatal)"
            done
            echo "Optimize dims done."
        """
    )

    # DAG: Mock (parallel) → wait flush → Spark ingest → Optimize dims
    [t0_mock_facebook, t0_mock_google, t0_mock_tiktok] >> t_wait_flush >> t1_minio_ingest >> t2_optimize_dims

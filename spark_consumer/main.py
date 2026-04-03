# spark_consumer/main.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

from facebook_processor import FacebookAdsProcessor

def create_spark():
    spark = SparkSession.builder \
        .appName("MarketingDataProcessing_StarSchema") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark


def main():
    spark = create_spark()

    # metadata/schema used to route messages
    metadata_schema = StructType([
        StructField("platform", StringType()),
        StructField("report_type", StringType()),
        StructField("ingested_at", StringType()),
        StructField("crawling_type", StringType())
    ])

    base_schema = StructType([
        StructField("metadata", metadata_schema),
        StructField("data", StringType())
    ])

    try:
        raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "topic_fb_raw") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
    except Exception as e:
        print(f"[ERROR] Failed to read from Kafka: {e}")
        spark.stop()
        return

    base_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), base_schema).alias("payload")) \
        .select(
            col("payload.metadata.platform").alias("platform"),
            col("payload.metadata.report_type").alias("report_type"),
            col("payload.metadata.ingested_at").alias("ingested_at"),
            col("payload.data").alias("raw_data")
        )

    # archive raw data to datalake
    archive_query = base_stream.writeStream \
        .format("parquet") \
        .option("path", "s3a://marketing-datalake/raw_zone/") \
        .option("checkpointLocation", "s3a://marketing-datalake/checkpoints/raw_zone_archive/") \
        .partitionBy("platform", "report_type") \
        .start()

    fb_processor = FacebookAdsProcessor(spark)

    def route_data_to_dwh(df, epoch_id):
        fb_processor.process_batch(df, epoch_id)

    dwh_query = base_stream.writeStream \
        .foreachBatch(route_data_to_dwh) \
        .trigger(availableNow=True) \
        .option("checkpointLocation", "s3a://marketing-datalake/checkpoints/data_warehouse_router/") \
        .start()
    
    # wait for any stream to finish (in this case, the availableNow stream will terminate after processing all data)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

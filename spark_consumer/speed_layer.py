# spark_consumer/speed_layer.py
"""
Speed Layer: Spark Structured Streaming
  Sources : Kafka raw topics
              fad_ad_daily_report
              fad_age_gender_detailed_report
              topic_google_raw
              TTA_ad_performance
  Sink    : Kafka processed topics — one per dim/fact table
  Trigger : every 30 seconds (configurable via SPEED_LAYER_TRIGGER env var)

  Facebook output topics (10):
    processed_dim_account
    processed_dim_campaign
    processed_dim_adset
    processed_dim_ad
    processed_dim_creative
    processed_dim_date
    processed_fact_fb_ad_daily
    processed_fact_fb_ad_creative_daily
    processed_fad_ad_daily_report
    processed_fact_fb_ad_demographic_daily

  Google output topics (18):
    processed_gad_campaign_daily_report
    processed_gad_ad_group_daily_report
    processed_gad_account_daily_report
    processed_gad_keyword_performance_report
    processed_gad_age_report
    processed_gad_gender_report
    processed_gad_ad_asset_daily_report
    processed_gad_click_type_report
    processed_dim_gg_campaign
    processed_dim_gg_adgroup
    processed_dim_gg_asset
    processed_fact_gg_campaign_daily
    processed_fact_gg_adgroup_daily
    processed_fact_gg_keyword_daily
    processed_fact_gg_age_daily
    processed_fact_gg_gender_daily
    processed_fact_gg_asset_daily
    processed_fact_gg_click_type_daily

  TikTok output topics (4):
    processed_tta_ad_performance
    processed_dim_tta_advertiser
    processed_dim_tta_ad
    processed_fact_tta_ad_daily

  Run (inside Docker, from airflow-scheduler container which has spark-submit + JARs):
    spark-submit --master spark://spark-master:7077 \\
      --jars /opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,\\
/opt/airflow/jars/kafka-clients-3.5.1.jar,\\
/opt/airflow/jars/hadoop-aws.jar,\\
/opt/airflow/jars/aws-java-sdk-bundle.jar,\\
/opt/airflow/jars/commons-pool2.jar \\
      /opt/spark/work-dir/spark_consumer/speed_layer.py
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType,
)

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS     = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET     = os.getenv("MINIO_SECRET_KEY",  "password123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "marketing-datalake")
TRIGGER_INTERVAL = os.getenv("SPEED_LAYER_TRIGGER", "30 seconds")

# Raw input topics
TOPIC_AD_DAILY   = "fad_ad_daily_report"
TOPIC_AGE_GENDER = "fad_age_gender_detailed_report"
TOPIC_TTA_RAW    = "TTA_ad_performance"
# Google raw input topics (one per report type, produced by ingest/google/mock.py)
GG_RAW_TOPICS = ",".join([
    "gad_campaign_daily_report",
    "gad_ad_group_daily_report",
    "gad_account_daily_report",
    "gad_keyword_performance_report",
    "gad_age_report",
    "gad_gender_report",
    "gad_ad_asset_daily_report",
    "gad_click_type_report",
])

# Facebook processed output topics (1 per ClickHouse table)
TOPIC_DIM_ACCOUNT      = "processed_dim_account"
TOPIC_DIM_CAMPAIGN     = "processed_dim_campaign"
TOPIC_DIM_ADSET        = "processed_dim_adset"
TOPIC_DIM_AD           = "processed_dim_ad"
TOPIC_DIM_CREATIVE     = "processed_dim_creative"
TOPIC_DIM_DATE         = "processed_dim_date"
TOPIC_FACT_AD_DAILY    = "processed_fact_fb_ad_daily"
TOPIC_FACT_AD_CREATIVE = "processed_fact_fb_ad_creative_daily"
TOPIC_FACT_DEMOGRAPHIC = "processed_fact_fb_ad_demographic_daily"
TOPIC_FAD_REPORT       = "processed_fad_ad_daily_report"

# Google processed output topics
TOPIC_DIM_GG_CAMPAIGN     = "processed_dim_gg_campaign"
TOPIC_GG_CAMPAIGN_DAILY   = "processed_gad_campaign_daily_report"
TOPIC_GG_ADGROUP_DAILY    = "processed_gad_ad_group_daily_report"
TOPIC_GG_ACCOUNT_DAILY    = "processed_gad_account_daily_report"
TOPIC_GG_KEYWORD_DAILY    = "processed_gad_keyword_performance_report"
TOPIC_GG_AGE_REPORT       = "processed_gad_age_report"
TOPIC_GG_GENDER_REPORT    = "processed_gad_gender_report"
TOPIC_GG_AD_ASSET         = "processed_gad_ad_asset_daily_report"
TOPIC_GG_CLICK_TYPE       = "processed_gad_click_type_report"
TOPIC_DIM_GG_ADGROUP      = "processed_dim_gg_adgroup"
TOPIC_DIM_GG_ASSET        = "processed_dim_gg_asset"
TOPIC_FACT_GG_CAMPAIGN    = "processed_fact_gg_campaign_daily"
TOPIC_FACT_GG_ADGROUP     = "processed_fact_gg_adgroup_daily"
TOPIC_FACT_GG_KEYWORD     = "processed_fact_gg_keyword_daily"
TOPIC_FACT_GG_AGE         = "processed_fact_gg_age_daily"
TOPIC_FACT_GG_GENDER      = "processed_fact_gg_gender_daily"
TOPIC_FACT_GG_ASSET       = "processed_fact_gg_asset_daily"
TOPIC_FACT_GG_CLICK_TYPE  = "processed_fact_gg_click_type_daily"

# TikTok processed output topics
TOPIC_TTA_AD_PERFORMANCE  = "processed_tta_ad_performance"
TOPIC_DIM_TTA_ADVERTISER  = "processed_dim_tta_advertiser"
TOPIC_DIM_TTA_AD          = "processed_dim_tta_ad"
TOPIC_FACT_TTA_AD_DAILY   = "processed_fact_tta_ad_daily"

# ── Input schemas ──────────────────────────────────────────────────────────────

_AD_DAILY_FIELDS = [
    StructField("account_id",              StringType()),
    StructField("account_name",            StringType()),
    StructField("campaign_id",             StringType()),
    StructField("campaign_name",           StringType()),
    StructField("ad_set_id",               StringType()),
    StructField("ad_set_name",             StringType()),
    StructField("id",                      StringType()),
    StructField("name",                    StringType()),
    StructField("date_start",              StringType()),
    StructField("date_stop",               StringType()),
    StructField("user_id",                 StringType()),
    StructField("spend",                   FloatType()),
    StructField("impressions",             IntegerType()),
    StructField("reach",                   IntegerType()),
    StructField("clicks",                  IntegerType()),
    StructField("linkClicks",              IntegerType()),
    StructField("landingPageViews",        IntegerType()),
    StructField("messagingFirstReply",     IntegerType()),
    StructField("newMessagingConnections", IntegerType()),
    StructField("postComments",            IntegerType()),
    StructField("postShares",              IntegerType()),
    StructField("postSaves",               IntegerType()),
    StructField("postReactions",           IntegerType()),
    StructField("photoViews",              IntegerType()),
    StructField("pageLikes",               IntegerType()),
    StructField("postEngagements",         IntegerType()),
    StructField("creative_id",             StringType()),
    StructField("creative_name",           StringType()),
    StructField("thruPlay",                IntegerType()),
    StructField("videoViewsP25",           IntegerType()),
    StructField("videoViewsP50",           IntegerType()),
    StructField("videoViewsP75",           IntegerType()),
    StructField("videoViewsP95",           IntegerType()),
    StructField("videoViewsP100",          IntegerType()),
    StructField("daily_budget",            IntegerType()),
    StructField("year",                    StringType()),
    StructField("month",                   StringType()),
    StructField("day",                     StringType()),
]

AD_DAILY_SCHEMA  = StructType(_AD_DAILY_FIELDS)
AGE_GENDER_SCHEMA = StructType(_AD_DAILY_FIELDS + [
    StructField("age",    StringType()),
    StructField("gender", StringType()),
])

# ── Google input schemas ───────────────────────────────────────────────────────

_GG_CAMPAIGN_SCHEMA = StructType([
    StructField("id",              StringType()),
    StructField("name",            StringType()),
    StructField("date",            StringType()),
    StructField("impressions",     IntegerType()),
    StructField("clicks",          IntegerType()),
    StructField("cost",            FloatType()),
    StructField("all_conversions", IntegerType()),
    StructField("ctr",             FloatType()),
])

_GG_ADGROUP_SCHEMA = StructType([
    StructField("id",              StringType()),
    StructField("name",            StringType()),
    StructField("date",            StringType()),
    StructField("impressions",     IntegerType()),
    StructField("clicks",          IntegerType()),
    StructField("cost",            FloatType()),
    StructField("all_conversions", IntegerType()),
    StructField("ctr",             FloatType()),
])

_GG_ACCOUNT_SCHEMA = StructType([
    StructField("id",              StringType()),
    StructField("name",            StringType()),
    StructField("date",            StringType()),
    StructField("impressions",     IntegerType()),
    StructField("clicks",          IntegerType()),
    StructField("cost",            FloatType()),
    StructField("all_conversions", IntegerType()),
    StructField("account_id",      StringType()),
    StructField("ctr",             FloatType()),
])

_GG_BREAKDOWN_FIELDS = [
    StructField("adgroup_id",          StringType()),
    StructField("date",                StringType()),
    StructField("campaign_id",         StringType()),
    StructField("campaign_name",       StringType()),
    StructField("adgroup_name",        StringType()),
    StructField("account_id",          StringType()),
    StructField("account_name",        StringType()),
    StructField("device",              StringType()),
    StructField("impressions",         IntegerType()),
    StructField("clicks",              IntegerType()),
    StructField("ctr",                 FloatType()),
    StructField("conversions",         IntegerType()),
    StructField("all_conversions",     IntegerType()),
    StructField("average_cpc",         FloatType()),
    StructField("cost_per_conversion", FloatType()),
    StructField("cost",                FloatType()),
]

_GG_KEYWORD_SCHEMA = StructType(_GG_BREAKDOWN_FIELDS + [
    StructField("keyword",       StringType()),
    StructField("quality_score", IntegerType()),
])

_GG_AGE_SCHEMA = StructType(_GG_BREAKDOWN_FIELDS + [
    StructField("age_range", StringType()),
])

_GG_GENDER_SCHEMA = StructType(_GG_BREAKDOWN_FIELDS + [
    StructField("gender", StringType()),
])

_GG_AD_ASSET_SCHEMA = StructType([
    StructField("ad_id",             StringType()),
    StructField("asset_id",          StringType()),
    StructField("date",              StringType()),
    StructField("campaign_id",       StringType()),
    StructField("campaign_name",     StringType()),
    StructField("adgroup_id",        StringType()),
    StructField("adgroup_name",      StringType()),
    StructField("asset_name",        StringType()),
    StructField("asset_type",        StringType()),
    StructField("asset_text",        StringType()),
    StructField("image_url",         StringType()),
    StructField("asset_performance", StringType()),
    StructField("impressions",       IntegerType()),
    StructField("clicks",            IntegerType()),
    StructField("ctr",               FloatType()),
    StructField("all_conversions",   IntegerType()),
    StructField("cost",              FloatType()),
    StructField("account_id",        StringType()),
    StructField("account_name",      StringType()),
])

_GG_CLICK_TYPE_SCHEMA = StructType([
    StructField("campaign_id",     StringType()),
    StructField("date",            StringType()),
    StructField("click_type",      StringType()),
    StructField("campaign_name",   StringType()),
    StructField("campaign_status", StringType()),
    StructField("impressions",     IntegerType()),
    StructField("clicks",          IntegerType()),
    StructField("ctr",             FloatType()),
    StructField("conversions",     IntegerType()),
    StructField("all_conversions", IntegerType()),
    StructField("device",          StringType()),
    StructField("ad_network_type", StringType()),
    StructField("cost",            FloatType()),
    StructField("account_id",      StringType()),
    StructField("account_name",    StringType()),
])

# ── SparkSession ───────────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SpeedLayer_Kafka_Streaming")
        .config("spark.hadoop.fs.s3a.endpoint",        MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",      MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",      MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

# ── Kafka helpers ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession, topic: str):
    """Return a streaming DataFrame subscribed to one Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def produce_to_kafka(df: DataFrame, topic: str) -> None:
    """Serialize a batch DataFrame to JSON and write to a Kafka topic."""
    if df is None:
        return
    count = df.count()
    if count == 0:
        print(f"  [SKIP] {topic} — 0 rows")
        return
    (
        df.select(F.to_json(F.struct(*df.columns)).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_SERVERS)
          .option("topic", topic)
          .save()
    )
    print(f"  [OK]   {topic} — {count} rows")

# ── Transform helpers (logic mirrored from minio_ingest.py) ───────────────────

def _normalize_base(df: DataFrame) -> DataFrame:
    """Rename ad_set_* fields and cast core metric columns — identical to minio_ingest."""
    return (
        df
        .withColumnRenamed("ad_set_id",   "adset_id")
        .withColumnRenamed("ad_set_name", "adset_name")
        .withColumn("date_start",  F.to_date(F.col("date_start"),  "yyyy-MM-dd"))
        .withColumn("date_stop",   F.to_date(F.col("date_stop"),   "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast("float"))
        .withColumn("impressions", F.col("impressions").cast("int"))
        .withColumn("reach",       F.col("reach").cast("int"))
        .withColumn("clicks",      F.col("clicks").cast("int"))
    )


def _build_dim_date(dates_df: DataFrame) -> DataFrame:
    """
    Streaming version of populate_dim_date: derive calendar attributes only for
    the unique dates present in the current micro-batch (no full date spine).
    Vietnamese public holidays match MockGenerator and minio_ingest.py.
    """
    iso_dow = F.date_format(F.col("date"), "u").cast("int")

    holiday_name = (
        F.when((F.month("date") == 1) & (F.dayofmonth("date") == 1),
               F.lit("Tết Dương Lịch"))
         .when(((F.month("date") == 1) & (F.dayofmonth("date") >= 15)) |
               ((F.month("date") == 2) & (F.dayofmonth("date") <= 5)),
               F.lit("Tết Nguyên Đán"))
         .when((F.month("date") == 4) & (F.dayofmonth("date") == 30),
               F.lit("Ngày Giải Phóng Miền Nam"))
         .when((F.month("date") == 5) & (F.dayofmonth("date") == 1),
               F.lit("Ngày Quốc Tế Lao Động"))
         .when((F.month("date") == 9) & (F.dayofmonth("date") == 2),
               F.lit("Quốc Khánh"))
         .otherwise(F.lit(None).cast("string"))
    )

    return dates_df.select(
        F.col("date"),
        F.year("date").cast("short").alias("year"),
        F.quarter("date").cast("byte").alias("quarter"),
        F.month("date").cast("byte").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").cast("byte").alias("week"),
        F.dayofyear("date").cast("short").alias("day_of_year"),
        F.dayofmonth("date").cast("byte").alias("day_of_month"),
        iso_dow.cast("byte").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.when(iso_dow >= 6, F.lit(1)).otherwise(F.lit(0)).cast("byte").alias("is_weekend"),
        F.when(holiday_name.isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("byte").alias("is_holiday"),
        holiday_name.alias("holiday_name"),
    )

# ── foreachBatch handlers ──────────────────────────────────────────────────────

def process_ad_daily_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """
    foreachBatch handler for fad_ad_daily_report.
    Applies the same transform as process_ad_daily() + populate_dim_date()
    in minio_ingest.py, then produces to 9 processed Kafka topics.
    """
    if batch_df.rdd.isEmpty():
        return

    print(f"\n[epoch={epoch_id}] fad_ad_daily_report — transforming...")
    base = _normalize_base(batch_df)
    base.persist()

    # dim_account
    produce_to_kafka(
        base.select(
            F.col("account_id"),
            F.coalesce(F.col("account_name"), F.lit("Unknown")).alias("account_name"),
        ).dropDuplicates(["account_id"]),
        TOPIC_DIM_ACCOUNT,
    )

    # dim_campaign
    produce_to_kafka(
        base.select(
            F.col("campaign_id"),
            F.col("account_id"),
            F.coalesce(F.col("campaign_name"), F.lit("Unknown")).alias("campaign_name"),
        ).filter(F.col("campaign_id").isNotNull())
         .dropDuplicates(["campaign_id"]),
        TOPIC_DIM_CAMPAIGN,
    )

    # dim_adset
    produce_to_kafka(
        base.select(
            F.col("adset_id"),
            F.col("campaign_id"),
            F.coalesce(F.col("adset_name"), F.lit("Unknown")).alias("adset_name"),
        ).filter(F.col("adset_id").isNotNull())
         .dropDuplicates(["adset_id"]),
        TOPIC_DIM_ADSET,
    )

    # dim_ad
    produce_to_kafka(
        base.select(
            F.col("id").alias("ad_id"),
            F.col("adset_id"),
            F.coalesce(F.col("name"), F.lit("Unknown")).alias("ad_name"),
            F.lit("ACTIVE").alias("status"),
            F.lit("ACTIVE").alias("effective_status"),
            F.lit(None).cast("timestamp").alias("created_time"),
        ).dropDuplicates(["ad_id"]),
        TOPIC_DIM_AD,
    )

    # dim_creative
    produce_to_kafka(
        base.filter(F.col("creative_id").isNotNull())
            .select(
                F.col("creative_id"),
                F.coalesce(F.col("creative_name"), F.lit("")).alias("creative_title"),
                F.lit("").alias("creative_body"),
                F.lit("").alias("creative_thumbnail_raw_url"),
                F.lit("").alias("creative_link"),
                F.lit("Unknown").alias("page_name"),
            ).dropDuplicates(["creative_id"]),
        TOPIC_DIM_CREATIVE,
    )

    # dim_date — only dates seen in this micro-batch (streaming version)
    dates_df = base.select(F.col("date_start").alias("date")).dropDuplicates(["date"])
    produce_to_kafka(_build_dim_date(dates_df), TOPIC_DIM_DATE)

    # fact_fb_ad_daily
    produce_to_kafka(
        base.select(
            F.col("date_start"),
            F.col("account_id"),
            F.col("id").alias("ad_id"),
            F.col("spend"),
            F.col("impressions"),
            F.col("reach"),
            F.col("clicks"),
            F.lit(0.0).cast("float").alias("ctr"),
            F.lit(0.0).cast("float").alias("cpc"),
            F.lit(0.0).cast("float").alias("cpm"),
            F.lit(0.0).cast("float").alias("frequency"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
            F.lit(0.0).cast("float").alias("cost_per_new_messaging"),
            F.coalesce(F.col("linkClicks").cast("int"),       F.lit(0)).alias("link_clicks"),
            F.coalesce(F.col("landingPageViews").cast("int"), F.lit(0)).alias("landing_page_views"),
        ),
        TOPIC_FACT_AD_DAILY,
    )

    # fact_fb_ad_creative_daily
    produce_to_kafka(
        base.filter(F.col("creative_id").isNotNull())
            .select(
                F.col("date_start"),
                F.col("account_id"),
                F.col("id").alias("ad_id"),
                F.col("creative_id"),
                F.col("spend"),
                F.col("impressions"),
                F.col("reach"),
                F.col("clicks"),
                F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
                F.coalesce(F.col("postEngagements").cast("int"),         F.lit(0)).alias("post_engagements"),
                F.coalesce(F.col("postReactions").cast("int"),           F.lit(0)).alias("post_reactions"),
                F.coalesce(F.col("postShares").cast("int"),              F.lit(0)).alias("post_shares"),
                F.coalesce(F.col("photoViews").cast("int"),              F.lit(0)).alias("photo_views"),
            ),
        TOPIC_FACT_AD_CREATIVE,
    )

    # fad_ad_daily_report (flat/denormalized)
    produce_to_kafka(
        base.select(
            F.col("id"),
            F.coalesce(F.col("name"),          F.lit("")).alias("name"),
            F.col("adset_id"),
            F.coalesce(F.col("adset_name"),    F.lit("")).alias("adset_name"),
            F.col("campaign_id"),
            F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
            F.col("account_id"),
            F.coalesce(F.col("account_name"),  F.lit("")).alias("account_name"),
            F.col("date_start"),
            F.col("date_stop"),
            F.col("spend"),
            F.col("impressions"),
            F.col("reach"),
            F.col("clicks"),
            F.coalesce(F.col("messagingFirstReply").cast("int"),     F.lit(0)).alias("messaging_first_reply"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
            F.coalesce(F.col("postComments").cast("int"),            F.lit(0)).alias("post_comments"),
            F.coalesce(F.col("linkClicks").cast("int"),              F.lit(0)).alias("link_clicks"),
            F.coalesce(F.col("landingPageViews").cast("int"),        F.lit(0)).alias("landing_page_views"),
            F.coalesce(F.col("pageLikes").cast("int"),               F.lit(0)).alias("page_likes"),
            F.coalesce(F.col("thruPlay").cast("int"),                F.lit(0)).alias("thru_play"),
        ),
        TOPIC_FAD_REPORT,
    )

    base.unpersist()
    print(f"[epoch={epoch_id}] Done.")


def process_age_gender_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """
    foreachBatch handler for fad_age_gender_detailed_report.
    Mirrors process_age_gender() from minio_ingest.py.
    """
    if batch_df.rdd.isEmpty():
        return

    print(f"\n[epoch={epoch_id}] fad_age_gender_detailed_report — transforming...")

    produce_to_kafka(
        batch_df.filter(
            F.col("id").isNotNull()
            & F.col("date_start").isNotNull()
            & F.col("age").isNotNull()
        ).select(
            F.to_date(F.col("date_start"), "yyyy-MM-dd").alias("date_start"),
            F.col("account_id"),
            F.col("id").alias("ad_id"),
            F.col("age"),
            F.coalesce(F.col("gender"),                              F.lit("unknown")).alias("gender"),
            F.coalesce(F.col("spend").cast("float"),                 F.lit(0.0)).alias("spend"),
            F.coalesce(F.col("impressions").cast("int"),             F.lit(0)).alias("impressions"),
            F.coalesce(F.col("reach").cast("int"),                   F.lit(0)).alias("reach"),
            F.coalesce(F.col("clicks").cast("int"),                  F.lit(0)).alias("clicks"),
            F.coalesce(F.col("linkClicks").cast("int"),              F.lit(0)).alias("inline_link_clicks"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
        ),
        TOPIC_FACT_DEMOGRAPHIC,
    )
    print(f"[epoch={epoch_id}] Done.")


def process_google_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """
    foreachBatch handler for 8 gad_* Kafka topics.
    batch_df is the raw Kafka DataFrame with (value, topic, ...) columns.
    Dispatches by the `topic` column and produces to 15 processed_gg_* Kafka topics.
    """
    if batch_df.rdd.isEmpty():
        return

    print(f"\n[epoch={epoch_id}] gad_* topics — transforming...")
    batch_df.persist()

    def _parse(topic_name, schema):
        return batch_df.filter(F.col("topic") == topic_name).select(
            F.from_json(F.col("value").cast("string"), schema).alias("d")
        ).select("d.*")

    # ── 1. campaign_daily ─────────────────────────────────────────────────────
    camp = _parse("gad_campaign_daily_report", _GG_CAMPAIGN_SCHEMA)
    camp.persist()
    base_camp = camp.filter(F.col("id").isNotNull()).select(
        F.col("id").alias("campaign_id"),
        F.coalesce(F.col("name"), F.lit("Unknown")).alias("campaign_name"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.coalesce(F.col("impressions"),          F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks"),               F.lit(0)).alias("clicks"),
        F.coalesce(F.col("cost").cast("float"),   F.lit(0.0)).alias("cost"),
        F.coalesce(F.col("all_conversions"),      F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("ctr").cast("float"),    F.lit(0.0)).alias("ctr"),
    )
    produce_to_kafka(
        base_camp.select("campaign_id", "campaign_name").dropDuplicates(["campaign_id"]),
        TOPIC_DIM_GG_CAMPAIGN,
    )
    produce_to_kafka(base_camp, TOPIC_GG_CAMPAIGN_DAILY)
    produce_to_kafka(
        base_camp.select("campaign_id", "date", "impressions", "clicks",
                         "cost", "all_conversions", "ctr"),
        TOPIC_FACT_GG_CAMPAIGN,
    )
    camp.unpersist()

    # ── 2. ad_group_daily ─────────────────────────────────────────────────────
    adg = _parse("gad_ad_group_daily_report", _GG_ADGROUP_SCHEMA)
    adg.persist()
    base_adg = adg.filter(F.col("id").isNotNull()).select(
        F.col("id").alias("adgroup_id"),
        F.coalesce(F.col("name"), F.lit("Unknown")).alias("adgroup_name"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.coalesce(F.col("impressions"),          F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks"),               F.lit(0)).alias("clicks"),
        F.coalesce(F.col("cost").cast("float"),   F.lit(0.0)).alias("cost"),
        F.coalesce(F.col("all_conversions"),      F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("ctr").cast("float"),    F.lit(0.0)).alias("ctr"),
    )
    produce_to_kafka(base_adg, TOPIC_GG_ADGROUP_DAILY)
    produce_to_kafka(
        base_adg.select("adgroup_id", "date", "impressions", "clicks",
                         "cost", "all_conversions", "ctr"),
        TOPIC_FACT_GG_ADGROUP,
    )
    adg.unpersist()

    # ── 3. account ────────────────────────────────────────────────────────────
    acct = _parse("gad_account_daily_report", _GG_ACCOUNT_SCHEMA)
    produce_to_kafka(
        acct.filter(F.col("account_id").isNotNull()).select(
            F.col("account_id"),
            F.coalesce(F.col("name"), F.lit("Unknown")).alias("account_name"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            F.coalesce(F.col("impressions"),          F.lit(0)).alias("impressions"),
            F.coalesce(F.col("clicks"),               F.lit(0)).alias("clicks"),
            F.coalesce(F.col("cost").cast("float"),   F.lit(0.0)).alias("cost"),
            F.coalesce(F.col("all_conversions"),      F.lit(0)).alias("all_conversions"),
            F.coalesce(F.col("ctr").cast("float"),    F.lit(0.0)).alias("ctr"),
        ),
        TOPIC_GG_ACCOUNT_DAILY,
    )

    # ── 4. keyword ────────────────────────────────────────────────────────────
    kw = _parse("gad_keyword_performance_report", _GG_KEYWORD_SCHEMA)
    kw.persist()
    valid_kw = kw.filter(F.col("adgroup_id").isNotNull() & F.col("keyword").isNotNull())
    produce_to_kafka(
        valid_kw.select(
            F.col("adgroup_id"),
            F.coalesce(F.col("campaign_id"),    F.lit("")).alias("campaign_id"),
            F.coalesce(F.col("adgroup_name"),   F.lit("Unknown")).alias("adgroup_name"),
        ).dropDuplicates(["adgroup_id"]),
        TOPIC_DIM_GG_ADGROUP,
    )
    flat_kw = valid_kw.select(
        F.col("adgroup_id"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.col("campaign_id"),
        F.coalesce(F.col("campaign_name"),             F.lit("")).alias("campaign_name"),
        F.coalesce(F.col("adgroup_name"),              F.lit("")).alias("adgroup_name"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"),              F.lit("")).alias("account_name"),
        F.coalesce(F.col("device"),                    F.lit("UNKNOWN")).alias("device"),
        F.col("keyword"),
        F.coalesce(F.col("quality_score"),             F.lit(0)).alias("quality_score"),
        F.coalesce(F.col("impressions"),               F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks"),                    F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),         F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("conversions"),               F.lit(0)).alias("conversions"),
        F.coalesce(F.col("all_conversions"),           F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("average_cpc").cast("float"), F.lit(0.0)).alias("average_cpc"),
        F.coalesce(F.col("cost_per_conversion").cast("float"), F.lit(0.0)).alias("cost_per_conversion"),
        F.coalesce(F.col("cost").cast("float"),        F.lit(0.0)).alias("cost"),
    )
    produce_to_kafka(flat_kw, TOPIC_GG_KEYWORD_DAILY)
    produce_to_kafka(
        flat_kw.select(
            "date", "account_id", "campaign_id", "adgroup_id",
            "keyword", "device", "quality_score",
            "impressions", "clicks", "cost", "conversions", "all_conversions",
            "ctr", "average_cpc", "cost_per_conversion",
        ),
        TOPIC_FACT_GG_KEYWORD,
    )
    kw.unpersist()

    # ── 5a. age breakdown ─────────────────────────────────────────────────────
    age_df = _parse("gad_age_report", _GG_AGE_SCHEMA)
    if not age_df.rdd.isEmpty():
        flat_age = age_df.filter(F.col("adgroup_id").isNotNull()).select(
            F.col("adgroup_id"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            F.col("campaign_id"),
            F.coalesce(F.col("campaign_name"),             F.lit("")).alias("campaign_name"),
            F.coalesce(F.col("adgroup_name"),              F.lit("")).alias("adgroup_name"),
            F.col("account_id"),
            F.coalesce(F.col("account_name"),              F.lit("")).alias("account_name"),
            F.coalesce(F.col("device"),                    F.lit("UNKNOWN")).alias("device"),
            F.col("age_range"),
            F.coalesce(F.col("impressions"),               F.lit(0)).alias("impressions"),
            F.coalesce(F.col("clicks"),                    F.lit(0)).alias("clicks"),
            F.coalesce(F.col("ctr").cast("float"),         F.lit(0.0)).alias("ctr"),
            F.coalesce(F.col("conversions"),               F.lit(0)).alias("conversions"),
            F.coalesce(F.col("all_conversions"),           F.lit(0)).alias("all_conversions"),
            F.coalesce(F.col("average_cpc").cast("float"), F.lit(0.0)).alias("average_cpc"),
            F.coalesce(F.col("cost_per_conversion").cast("float"), F.lit(0.0)).alias("cost_per_conversion"),
            F.coalesce(F.col("cost").cast("float"),        F.lit(0.0)).alias("cost"),
        )
        produce_to_kafka(flat_age, TOPIC_GG_AGE_REPORT)
        produce_to_kafka(
            flat_age.select("date", "account_id", "campaign_id", "adgroup_id",
                            "age_range", "device", "impressions", "clicks", "cost",
                            "conversions", "all_conversions", "ctr", "average_cpc", "cost_per_conversion"),
            TOPIC_FACT_GG_AGE,
        )

    # ── 5b. gender breakdown ──────────────────────────────────────────────────
    gender_df = _parse("gad_gender_report", _GG_GENDER_SCHEMA)
    if not gender_df.rdd.isEmpty():
        flat_gender = gender_df.filter(F.col("adgroup_id").isNotNull()).select(
            F.col("adgroup_id"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            F.col("campaign_id"),
            F.coalesce(F.col("campaign_name"),             F.lit("")).alias("campaign_name"),
            F.coalesce(F.col("adgroup_name"),              F.lit("")).alias("adgroup_name"),
            F.col("account_id"),
            F.coalesce(F.col("account_name"),              F.lit("")).alias("account_name"),
            F.coalesce(F.col("device"),                    F.lit("UNKNOWN")).alias("device"),
            F.col("gender"),
            F.coalesce(F.col("impressions"),               F.lit(0)).alias("impressions"),
            F.coalesce(F.col("clicks"),                    F.lit(0)).alias("clicks"),
            F.coalesce(F.col("ctr").cast("float"),         F.lit(0.0)).alias("ctr"),
            F.coalesce(F.col("conversions"),               F.lit(0)).alias("conversions"),
            F.coalesce(F.col("all_conversions"),           F.lit(0)).alias("all_conversions"),
            F.coalesce(F.col("average_cpc").cast("float"), F.lit(0.0)).alias("average_cpc"),
            F.coalesce(F.col("cost_per_conversion").cast("float"), F.lit(0.0)).alias("cost_per_conversion"),
            F.coalesce(F.col("cost").cast("float"),        F.lit(0.0)).alias("cost"),
        )
        produce_to_kafka(flat_gender, TOPIC_GG_GENDER_REPORT)
        produce_to_kafka(
            flat_gender.select("date", "account_id", "campaign_id", "adgroup_id",
                               "gender", "device", "impressions", "clicks", "cost",
                               "conversions", "all_conversions", "ctr", "average_cpc", "cost_per_conversion"),
            TOPIC_FACT_GG_GENDER,
        )

    # ── 6. ad_asset ───────────────────────────────────────────────────────────
    asset = _parse("gad_ad_asset_daily_report", _GG_AD_ASSET_SCHEMA)
    asset.persist()
    valid_asset = asset.filter(F.col("ad_id").isNotNull() & F.col("asset_id").isNotNull())
    produce_to_kafka(
        valid_asset.select(
            F.col("asset_id"),
            F.coalesce(F.col("ad_id"),       F.lit("")).alias("ad_id"),
            F.coalesce(F.col("asset_name"),  F.lit("")).alias("asset_name"),
            F.coalesce(F.col("asset_type"),  F.lit("")).alias("asset_type"),
            F.coalesce(F.col("asset_text"),  F.lit("")).alias("asset_text"),
            F.coalesce(F.col("image_url"),   F.lit("")).alias("image_url"),
        ).dropDuplicates(["asset_id"]),
        TOPIC_DIM_GG_ASSET,
    )
    flat_asset = valid_asset.select(
        F.col("ad_id"),
        F.col("asset_id"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.col("campaign_id"),
        F.coalesce(F.col("campaign_name"),             F.lit("")).alias("campaign_name"),
        F.col("adgroup_id"),
        F.coalesce(F.col("adgroup_name"),              F.lit("")).alias("adgroup_name"),
        F.coalesce(F.col("asset_name"),                F.lit("")).alias("asset_name"),
        F.coalesce(F.col("asset_type"),                F.lit("")).alias("asset_type"),
        F.coalesce(F.col("asset_text"),                F.lit("")).alias("asset_text"),
        F.coalesce(F.col("image_url"),                 F.lit("")).alias("image_url"),
        F.coalesce(F.col("asset_performance"),         F.lit("")).alias("asset_performance"),
        F.coalesce(F.col("impressions"),               F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks"),                    F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),         F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("all_conversions"),           F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("cost").cast("float"),        F.lit(0.0)).alias("cost"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"),              F.lit("")).alias("account_name"),
    )
    produce_to_kafka(flat_asset, TOPIC_GG_AD_ASSET)
    produce_to_kafka(
        flat_asset.select(
            "date", "account_id", "campaign_id", "adgroup_id", "ad_id", "asset_id",
            "asset_performance", "impressions", "clicks", "cost", "all_conversions", "ctr",
        ),
        TOPIC_FACT_GG_ASSET,
    )
    asset.unpersist()

    # ── 7. click_type ─────────────────────────────────────────────────────────
    ct = _parse("gad_click_type_report", _GG_CLICK_TYPE_SCHEMA)
    valid_ct = ct.filter(F.col("campaign_id").isNotNull() & F.col("click_type").isNotNull())
    flat_ct = valid_ct.select(
        F.col("campaign_id"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.col("click_type"),
        F.coalesce(F.col("campaign_name"),             F.lit("")).alias("campaign_name"),
        F.coalesce(F.col("campaign_status"),           F.lit("")).alias("campaign_status"),
        F.coalesce(F.col("impressions"),               F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks"),                    F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),         F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("conversions"),               F.lit(0)).alias("conversions"),
        F.coalesce(F.col("all_conversions"),           F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("device"),                    F.lit("UNKNOWN")).alias("device"),
        F.coalesce(F.col("ad_network_type"),           F.lit("")).alias("ad_network_type"),
        F.coalesce(F.col("cost").cast("float"),        F.lit(0.0)).alias("cost"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"),              F.lit("")).alias("account_name"),
    )
    produce_to_kafka(flat_ct, TOPIC_GG_CLICK_TYPE)
    produce_to_kafka(
        flat_ct.select(
            "date", "account_id", "campaign_id",
            "click_type", "device", "ad_network_type",
            "impressions", "clicks", "cost",
            "conversions", "all_conversions", "ctr",
        ),
        TOPIC_FACT_GG_CLICK_TYPE,
    )

    batch_df.unpersist()
    print(f"[epoch={epoch_id}] Google Done.")


# ── TikTok schema ──────────────────────────────────────────────────────────────

_TTA_AD_PERFORMANCE_SCHEMA = StructType([
    StructField("pkId",                        StringType()),
    StructField("user_id",                     StringType()),
    StructField("stat_time_day",               StringType()),   # "2026-05-25 17:00:00.000"
    StructField("ad_id",                       StringType()),
    StructField("ad_name",                     StringType()),
    StructField("ad_text",                     StringType()),
    StructField("adgroup_name",                StringType()),
    StructField("campaign_name",               StringType()),
    StructField("advertiser_id",               StringType()),
    StructField("advertiser_name",             StringType()),
    StructField("start_date",                  StringType()),   # "2026-05-01 00:00:00.000"
    StructField("end_date",                    StringType()),   # "2026-07-30 00:00:00.000"
    StructField("spend",                       FloatType()),
    StructField("impressions",                 IntegerType()),
    StructField("clicks",                      IntegerType()),
    StructField("ctr",                         FloatType()),
    StructField("cpc",                         FloatType()),
    StructField("cpm",                         FloatType()),
    StructField("reach",                       IntegerType()),
    StructField("frequency",                   FloatType()),
    StructField("conversion",                  IntegerType()),
    StructField("cost_per_conversion",         FloatType()),
    StructField("conversion_rate",             FloatType()),
    StructField("video_play_actions",          IntegerType()),
    StructField("profile_visits",              IntegerType()),
    StructField("likes",                       IntegerType()),
    StructField("comments",                    IntegerType()),
    StructField("shares",                      IntegerType()),
    StructField("follows",                     IntegerType()),
    StructField("live_views",                  IntegerType()),
    StructField("purchase",                    IntegerType()),
    StructField("onsite_shopping",             IntegerType()),
    StructField("total_onsite_shopping_value", FloatType()),
    StructField("onsite_shopping_roas",        FloatType()),
    StructField("cost_per_onsite_shopping",    FloatType()),
    StructField("createdAt",                   StringType()),
    StructField("updatedAt",                   StringType()),
])


def process_tiktok_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """
    foreachBatch handler for TTA_ad_performance.
    Mirrors process_tta_ad_performance() from minio_ingest.py.
    Produces to 4 processed_tta_* Kafka topics.
    """
    if batch_df.rdd.isEmpty():
        return

    print(f"\n[epoch={epoch_id}] TTA_ad_performance — transforming...")

    base = batch_df.filter(F.col("ad_id").isNotNull()).select(
        F.col("pkId"),
        F.col("user_id"),
        F.to_date(F.col("stat_time_day"), "yyyy-MM-dd HH:mm:ss.SSS").alias("stat_time_day"),
        F.col("ad_id"),
        F.coalesce(F.col("ad_name"),         F.lit("")).alias("ad_name"),
        F.coalesce(F.col("ad_text"),         F.lit("")).alias("ad_text"),
        F.coalesce(F.col("adgroup_name"),    F.lit("")).alias("adgroup_name"),
        F.coalesce(F.col("campaign_name"),   F.lit("")).alias("campaign_name"),
        F.col("advertiser_id"),
        F.coalesce(F.col("advertiser_name"), F.lit("")).alias("advertiser_name"),
        F.to_date(F.col("start_date"), "yyyy-MM-dd HH:mm:ss.SSS").alias("start_date"),
        F.to_date(F.col("end_date"),   "yyyy-MM-dd HH:mm:ss.SSS").alias("end_date"),
        F.coalesce(F.col("spend").cast("float"),                    F.lit(0.0)).alias("spend"),
        F.coalesce(F.col("impressions").cast("int"),                F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),                     F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),                      F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("cpc").cast("float"),                      F.lit(0.0)).alias("cpc"),
        F.coalesce(F.col("cpm").cast("float"),                      F.lit(0.0)).alias("cpm"),
        F.coalesce(F.col("reach").cast("int"),                      F.lit(0)).alias("reach"),
        F.coalesce(F.col("frequency").cast("float"),                F.lit(0.0)).alias("frequency"),
        F.coalesce(F.col("conversion").cast("int"),                 F.lit(0)).alias("conversion"),
        F.coalesce(F.col("cost_per_conversion").cast("float"),      F.lit(0.0)).alias("cost_per_conversion"),
        F.coalesce(F.col("conversion_rate").cast("float"),          F.lit(0.0)).alias("conversion_rate"),
        F.coalesce(F.col("video_play_actions").cast("int"),         F.lit(0)).alias("video_play_actions"),
        F.coalesce(F.col("profile_visits").cast("int"),             F.lit(0)).alias("profile_visits"),
        F.coalesce(F.col("likes").cast("int"),                      F.lit(0)).alias("likes"),
        F.coalesce(F.col("comments").cast("int"),                   F.lit(0)).alias("comments"),
        F.coalesce(F.col("shares").cast("int"),                     F.lit(0)).alias("shares"),
        F.coalesce(F.col("follows").cast("int"),                    F.lit(0)).alias("follows"),
        F.coalesce(F.col("live_views").cast("int"),                 F.lit(0)).alias("live_views"),
        F.coalesce(F.col("purchase").cast("int"),                   F.lit(0)).alias("purchase"),
        F.coalesce(F.col("onsite_shopping").cast("int"),            F.lit(0)).alias("onsite_shopping"),
        F.coalesce(F.col("total_onsite_shopping_value").cast("float"), F.lit(0.0)).alias("total_onsite_shopping_value"),
        F.coalesce(F.col("onsite_shopping_roas").cast("float"),     F.lit(0.0)).alias("onsite_shopping_roas"),
        F.coalesce(F.col("cost_per_onsite_shopping").cast("float"), F.lit(0.0)).alias("cost_per_onsite_shopping"),
    )
    base.persist()

    # dim_tta_advertiser
    produce_to_kafka(
        base.select("advertiser_id", "advertiser_name")
            .dropDuplicates(["advertiser_id"]),
        TOPIC_DIM_TTA_ADVERTISER,
    )

    # dim_tta_ad
    produce_to_kafka(
        base.select(
            F.col("ad_id"),
            F.col("advertiser_id"),
            F.col("campaign_name"),
            F.col("adgroup_name"),
            F.col("ad_name"),
            F.col("ad_text"),
        ).dropDuplicates(["ad_id"]),
        TOPIC_DIM_TTA_AD,
    )

    # processed_tta_ad_performance (flat/denormalized real-time copy)
    produce_to_kafka(base, TOPIC_TTA_AD_PERFORMANCE)

    # fact_tta_ad_daily
    produce_to_kafka(
        base.select(
            F.col("stat_time_day").alias("date"),
            F.col("advertiser_id"),
            F.col("ad_id"),
            F.col("spend"),
            F.col("impressions"),
            F.col("clicks"),
            F.col("ctr"),
            F.col("cpc"),
            F.col("cpm"),
            F.col("reach"),
            F.col("frequency"),
            F.col("conversion"),
            F.col("cost_per_conversion"),
            F.col("conversion_rate"),
            F.col("video_play_actions"),
            F.col("profile_visits"),
            F.col("likes"),
            F.col("comments"),
            F.col("shares"),
            F.col("follows"),
            F.col("live_views"),
            F.col("purchase"),
            F.col("onsite_shopping"),
            F.col("total_onsite_shopping_value"),
            F.col("onsite_shopping_roas"),
            F.col("cost_per_onsite_shopping"),
        ),
        TOPIC_FACT_TTA_AD_DAILY,
    )

    base.unpersist()
    print(f"[epoch={epoch_id}] TikTok Done.")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Speed Layer — Kafka Structured Streaming")
    print(f"  Broker      : {KAFKA_SERVERS}")
    print(f"  Trigger     : {TRIGGER_INTERVAL}")
    print(f"  Checkpoints : s3a://{MINIO_BUCKET}/checkpoints/speed_layer/")
    print("=" * 60)

    # Stream 1: fad_ad_daily_report → 9 processed topics
    ad_daily_raw = read_kafka_stream(spark, TOPIC_AD_DAILY)
    parsed_ad_daily = ad_daily_raw.select(
        F.from_json(F.col("value").cast("string"), AD_DAILY_SCHEMA).alias("d")
    ).select("d.*")

    q_ad_daily = (
        parsed_ad_daily.writeStream
        .foreachBatch(process_ad_daily_batch)
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET}/checkpoints/speed_layer/ad_daily/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # Stream 2: fad_age_gender_detailed_report → processed_fact_fb_ad_demographic_daily
    age_gender_raw = read_kafka_stream(spark, TOPIC_AGE_GENDER)
    parsed_age_gender = age_gender_raw.select(
        F.from_json(F.col("value").cast("string"), AGE_GENDER_SCHEMA).alias("d")
    ).select("d.*")

    q_age_gender = (
        parsed_age_gender.writeStream
        .foreachBatch(process_age_gender_batch)
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET}/checkpoints/speed_layer/age_gender/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # Stream 3: 8 gad_* topics → 17 processed_gg_* topics
    # Subscribe to all Google raw topics in one stream; dispatch inside
    # process_google_batch by the Kafka `topic` column.
    google_raw = read_kafka_stream(spark, GG_RAW_TOPICS)

    q_google = (
        google_raw.writeStream
        .foreachBatch(process_google_batch)
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET}/checkpoints/speed_layer/google/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # Stream 4: TTA_ad_performance → 4 processed_tta_* topics
    tiktok_raw = read_kafka_stream(spark, TOPIC_TTA_RAW)
    parsed_tiktok = tiktok_raw.select(
        F.from_json(F.col("value").cast("string"), _TTA_AD_PERFORMANCE_SCHEMA).alias("d")
    ).select("d.*")

    q_tiktok = (
        parsed_tiktok.writeStream
        .foreachBatch(process_tiktok_batch)
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET}/checkpoints/speed_layer/tiktok/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"\nStreaming queries active: {q_ad_daily.id}, {q_age_gender.id}, {q_google.id}, {q_tiktok.id}")
    print("Waiting for termination (Ctrl+C to stop)...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

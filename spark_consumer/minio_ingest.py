# spark_consumer/minio_ingest.py
"""
Batch Spark job: read mock data JSON files from MinIO -> write to ClickHouse.

MinIO tables consumed:
  fad_ad_daily_report -> dim_account, dim_campaign, dim_adset, dim_ad,
                                    dim_creative, dim_date,
                                    fact_fb_ad_daily, fact_fb_ad_creative_daily,
                                    fad_ad_daily_report
  fad_age_gender_detailed_report -> fact_fb_ad_demographic_daily

Run (inside Docker):
  spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/airflow/jars/clickhouse-jdbc.jar,\
/opt/airflow/jars/hadoop-aws.jar,\
/opt/airflow/jars/aws-java-sdk-bundle.jar,\
/opt/airflow/jars/commons-pool2.jar \
    /opt/spark/work-dir/spark_consumer/minio_ingest.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET", "marketing-datalake")

CLICKHOUSE_URL  = "jdbc:clickhouse://clickhouse:8123/marketing_db?ssl=false&compress=false"
CLICKHOUSE_PROPS = {
    "user": "admin",
    "password": "password123",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "isolationLevel": "NONE",
}

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MinIO_to_ClickHouse_Ingestion")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def read_table(spark: SparkSession, table_name: str):
    """
    Read all JSON files for a MinIO landing-zone table.

    MinIO path layout written by MockGenerator/MinioClient:
      {bucket}/{table_name}/{YYYY}/{MM}/{DD}/{HH}/{mm}/{userId}_{ts}_{uuid}.json

    Each file is a JSON array of records, multiLine=True lets Spark
    parse an array-rooted file into one row per element.
    """
    path = f"s3a://{MINIO_BUCKET}/{table_name}/*/*/*/*/*/*.json"
    try:
        df = spark.read.option("multiLine", "true").json(path)
        print(f"  [READ] {table_name}: {df.count()} rows")
        return df
    except Exception as exc:
        print(f"  [WARN] Cannot read {table_name}: {exc}")
        return None


def write_ch(df, table: str) -> None:
    """Append a DataFrame to a ClickHouse table via JDBC."""
    if df is None:
        return
    count = df.count()
    if count == 0:
        print(f"  [SKIP] {table} — 0 rows")
        return
    df.write.jdbc(url=CLICKHOUSE_URL, table=table, mode="append", properties=CLICKHOUSE_PROPS)
    print(f"  [OK]   {table} — {count} rows")

def process_ad_daily(df) -> None:
    """
    Source  : fad_ad_daily_report (mock output)
    Targets : dim_account, dim_campaign, dim_adset, dim_ad, dim_creative
              fact_fb_ad_daily, fact_fb_ad_creative_daily
              fad_ad_daily_report (flat/denormalized)

    Field mapping vs ClickHouse:
      mock -> ClickHouse
      ad_set_id -> adset_id
      ad_set_name -> adset_name
      linkClicks -> link_clicks
      landingPageViews -> landing_page_views
      messagingFirstReply -> messaging_first_reply
      newMessagingConnections -> new_messaging_connections
      postComments -> post_comments
      pageLikes -> page_likes
      thruPlay -> thru_play
    """
    # -- Normalize common fields ----------------------------------------
    base = (
        df
        .withColumnRenamed("ad_set_id",   "adset_id")
        .withColumnRenamed("ad_set_name", "adset_name")
        .withColumn("date_start",  F.to_date(F.col("date_start"), "yyyy-MM-dd"))
        .withColumn("date_stop",   F.to_date(F.col("date_stop"),  "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast("float"))
        .withColumn("impressions", F.col("impressions").cast("int"))
        .withColumn("reach",       F.col("reach").cast("int"))
        .withColumn("clicks",      F.col("clicks").cast("int"))
    )
    base.persist()

    # -- dim_account ---------------------------------------------------------
    write_ch(
        base.select(
            F.col("account_id"),
            F.coalesce(F.col("account_name"), F.lit("Unknown")).alias("account_name"),
        ).dropDuplicates(["account_id"]),
        "marketing_db.dim_account",
    )

    # -- dim_campaign --------------------------------------------------------
    write_ch(
        base.select(
            F.col("campaign_id"), F.col("account_id"),
            F.coalesce(F.col("campaign_name"), F.lit("Unknown")).alias("campaign_name"),
        ).filter(F.col("campaign_id").isNotNull())
         .dropDuplicates(["campaign_id"]),
        "marketing_db.dim_campaign",
    )

    # -- dim_adset -----------------------------------------------------------
    write_ch(
        base.select(
            F.col("adset_id"), F.col("campaign_id"),
            F.coalesce(F.col("adset_name"), F.lit("Unknown")).alias("adset_name"),
        ).filter(F.col("adset_id").isNotNull())
         .dropDuplicates(["adset_id"]),
        "marketing_db.dim_adset",
    )

    # -- dim_ad --------------------------------------------------------------
    write_ch(
        base.select(
            F.col("id").alias("ad_id"),
            F.col("adset_id"),
            F.coalesce(F.col("name"), F.lit("Unknown")).alias("ad_name"),
            F.lit("ACTIVE").alias("status"),
            F.lit("ACTIVE").alias("effective_status"),
            F.lit(None).cast("timestamp").alias("created_time"),
        ).dropDuplicates(["ad_id"]),
        "marketing_db.dim_ad",
    )

    # -- dim_creative --------------------------------------------------------
    write_ch(
        base.filter(F.col("creative_id").isNotNull())
            .select(
                F.col("creative_id"),
                F.coalesce(F.col("creative_name"), F.lit("")).alias("creative_title"),
                F.lit("").alias("creative_body"),
                F.lit("").alias("creative_thumbnail_raw_url"),
                F.lit("").alias("creative_link"),
                F.lit("Unknown").alias("page_name"),
            ).dropDuplicates(["creative_id"]),
        "marketing_db.dim_creative",
    )

    # -- fact_fb_ad_daily ----------------------------------------------------
    write_ch(
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
            F.coalesce(F.col("linkClicks").cast("int"),      F.lit(0)).alias("link_clicks"),
            F.coalesce(F.col("landingPageViews").cast("int"), F.lit(0)).alias("landing_page_views"),
        ),
        "marketing_db.fact_fb_ad_daily",
    )

    # -- fact_fb_ad_creative_daily -------------------------------------------
    write_ch(
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
                F.coalesce(F.col("postEngagements").cast("int"), F.lit(0)).alias("post_engagements"),
                F.coalesce(F.col("postReactions").cast("int"),   F.lit(0)).alias("post_reactions"),
                F.coalesce(F.col("postShares").cast("int"),      F.lit(0)).alias("post_shares"),
                F.coalesce(F.col("photoViews").cast("int"),      F.lit(0)).alias("photo_views"),
            ),
        "marketing_db.fact_fb_ad_creative_daily",
    )

    # -- fad_ad_daily_report (flat/denormalized) ------------------------------
    write_ch(
        base.select(
            F.col("id"),
            F.coalesce(F.col("name"), F.lit("")).alias("name"),
            F.col("adset_id"),
            F.coalesce(F.col("adset_name"), F.lit("")).alias("adset_name"),
            F.col("campaign_id"),
            F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
            F.col("account_id"),
            F.coalesce(F.col("account_name"), F.lit("")).alias("account_name"),
            F.col("date_start"),
            F.col("date_stop"),
            F.col("spend"),
            F.col("impressions"),
            F.col("reach"),
            F.col("clicks"),
            F.coalesce(F.col("messagingFirstReply").cast("int"),      F.lit(0)).alias("messaging_first_reply"),
            F.coalesce(F.col("newMessagingConnections").cast("int"),  F.lit(0)).alias("new_messaging_connections"),
            F.coalesce(F.col("postComments").cast("int"),             F.lit(0)).alias("post_comments"),
            F.coalesce(F.col("linkClicks").cast("int"),               F.lit(0)).alias("link_clicks"),
            F.coalesce(F.col("landingPageViews").cast("int"),         F.lit(0)).alias("landing_page_views"),
            F.coalesce(F.col("pageLikes").cast("int"),                F.lit(0)).alias("page_likes"),
            F.coalesce(F.col("thruPlay").cast("int"),                 F.lit(0)).alias("thru_play"),
        ),
        "marketing_db.fad_ad_daily_report",
    )

    base.unpersist()


def populate_dim_date(spark: SparkSession, df_daily) -> None:
    """
    Build dim_date from the date range present in the ad daily data.

    Strategy: generate a complete date spine from min(date_start) to
    max(date_start) in the data, then derive every attribute using Spark
    built-in date functions.

    Vietnamese public holidays hard-coded:
      - Jan  1         : Tết Dương Lịch (New Year's Day)
      - Jan 15 – Feb 5 : Tết Nguyên Đán (same approximation as MockGenerator)
      - Apr 30         : Ngày Giải Phóng Miền Nam
      - May  1         : Ngày Quốc Tế Lao Động
      - Sep  2         : Quốc Khánh
    """
    bounds = df_daily.agg(
        F.min("date_start").alias("min_d"),
        F.max("date_start").alias("max_d"),
    ).collect()[0]

    min_d, max_d = bounds["min_d"], bounds["max_d"]
    if min_d is None or max_d is None:
        print("  [SKIP] dim_date — no dates found in data")
        return

    # Complete date spine (one row per calendar day)
    date_spine = spark.sql(
        f"SELECT explode(sequence(date'{min_d}', date'{max_d}', interval 1 day)) AS date"
    )

    # day_of_week using ISO 8601: 1=Mon, 2=Tue, ... 6=Sat, 7=Sun
    # Spark's date_format("u") returns ISO weekday as a string
    iso_dow = F.date_format(F.col("date"), "u").cast("int")

    holiday_name = (
        F.when(
            (F.month("date") == 1) & (F.dayofmonth("date") == 1),
            F.lit("Tết Dương Lịch")
        ).when(
            ((F.month("date") == 1) & (F.dayofmonth("date") >= 15)) |
            ((F.month("date") == 2) & (F.dayofmonth("date") <= 5)),
            F.lit("Tết Nguyên Đán")
        ).when(
            (F.month("date") == 4) & (F.dayofmonth("date") == 30),
            F.lit("Ngày Giải Phóng Miền Nam")
        ).when(
            (F.month("date") == 5) & (F.dayofmonth("date") == 1),
            F.lit("Ngày Quốc Tế Lao Động")
        ).when(
            (F.month("date") == 9) & (F.dayofmonth("date") == 2),
            F.lit("Quốc Khánh")
        ).otherwise(F.lit(None).cast("string"))
    )

    dim_date = date_spine.select(
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

    write_ch(dim_date, "marketing_db.dim_date")


def process_age_gender(df) -> None:
    """
    Source : fad_age_gender_detailed_report (mock output)
    Target : fact_fb_ad_demographic_daily
    """
    write_ch(
        df.filter(
            F.col("id").isNotNull()
            & F.col("date_start").isNotNull()
            & F.col("age").isNotNull()
        ).select(
            F.to_date(F.col("date_start"), "yyyy-MM-dd").alias("date_start"),
            F.col("account_id"),
            F.col("id").alias("ad_id"),
            F.col("age"),
            F.coalesce(F.col("gender"), F.lit("unknown")).alias("gender"),
            F.coalesce(F.col("spend").cast("float"),       F.lit(0.0)).alias("spend"),
            F.coalesce(F.col("impressions").cast("int"),   F.lit(0)).alias("impressions"),
            F.coalesce(F.col("reach").cast("int"),         F.lit(0)).alias("reach"),
            F.coalesce(F.col("clicks").cast("int"),        F.lit(0)).alias("clicks"),
            F.coalesce(F.col("linkClicks").cast("int"),    F.lit(0)).alias("inline_link_clicks"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
        ),
        "marketing_db.fact_fb_ad_demographic_daily",
    )

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 55)
    print("  MinIO -> ClickHouse Batch Ingestion")
    print("=" * 55)

    print("\n[1/3] fad_ad_daily_report")
    df_daily = read_table(spark, "fad_ad_daily_report")
    if df_daily is not None:
        df_daily.persist()
        process_ad_daily(df_daily)
        populate_dim_date(spark, df_daily)
        df_daily.unpersist()

    print("\n[2/3] fad_age_gender_detailed_report")
    df_demo = read_table(spark, "fad_age_gender_detailed_report")
    if df_demo is not None:
        process_age_gender(df_demo)

    print("\n[DONE] All tables written to ClickHouse.")
    spark.stop()

if __name__ == "__main__":
    main()

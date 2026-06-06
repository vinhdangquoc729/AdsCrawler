# spark_consumer/minio_ingest.py
"""
Batch Spark job: read mock data JSON files from MinIO -> write to ClickHouse.

MinIO tables consumed:
  fad_ad_daily_report              -> dim_account, dim_campaign, dim_adset, dim_ad,
                                      dim_creative, dim_date,
                                      fact_fb_ad_daily, fact_fb_ad_creative_daily,
                                      fad_ad_daily_report
  fad_age_gender_detailed_report   -> fact_fb_ad_demographic_daily
  gad_campaign_daily_report        -> gad_campaign_daily_report, fact_gg_campaign_daily
  gad_ad_group_daily_report        -> gad_ad_group_daily_report, fact_gg_adgroup_daily
  gad_account_daily_report         -> gad_account_daily_report, dim_account
  gad_keyword_performance_report   -> gad_keyword_performance_report, dim_campaign,
                                      dim_gg_adgroup, fact_gg_keyword_daily
  gad_age_report                     -> gad_age_report, fact_gg_age_daily
  gad_gender_report                  -> gad_gender_report, fact_gg_gender_daily
  gad_ad_asset_daily_report        -> gad_ad_asset_daily_report, dim_gg_asset,
                                      fact_gg_asset_daily
  gad_click_type_report            -> gad_click_type_report, fact_gg_click_type_daily
  TTA_ad_performance               -> tta_ad_performance, dim_tta_advertiser,
                                      dim_tta_ad, fact_tta_ad_daily

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
import argparse
import base64
import time
import urllib.request
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

# ── ClickHouse HTTP helpers (dùng cho purge và optimize) ──────────────────────

_CH_HTTP_URL  = os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123/")
_CH_HTTP_AUTH = base64.b64encode(b"admin:password123").decode()

def _ch_exec(query: str) -> str:
    """Gửi query DDL/DML tới ClickHouse qua HTTP API, trả về response text."""
    req = urllib.request.Request(_CH_HTTP_URL, data=query.encode(), method="POST")
    req.add_header("Authorization", f"Basic {_CH_HTTP_AUTH}")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.read().decode().strip()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"ClickHouse HTTP {e.code}: {e.read().decode()}")

def _wait_mutations(table: str, timeout: int = 120) -> None:
    """Block cho đến khi ClickHouse hoàn thành tất cả mutation đang chờ của table."""
    check = (
        f"SELECT count() FROM system.mutations "
        f"WHERE database='marketing_db' AND table='{table}' AND is_done=0 "
        f"FORMAT TabSeparated"
    )
    for _ in range(timeout):
        if _ch_exec(check) == "0":
            return
        time.sleep(1)
    print(f"  [WARN] Mutation wait timeout for {table}, proceeding anyway.")

def _purge_date(table: str, date_col: str, process_date: str) -> None:
    """
    Xóa toàn bộ rows của process_date trong table trước khi re-insert.
    Đảm bảo idempotency: retry bao nhiêu lần cũng không bị duplicate.
    Chỉ gọi khi process_date != None (incremental mode).
    """
    print(f"  [PURGE] marketing_db.{table} WHERE {date_col} = '{process_date}'")
    _ch_exec(
        f"ALTER TABLE marketing_db.{table} DELETE WHERE {date_col} = '{process_date}'"
    )
    _wait_mutations(table)

# Mapping: tên table → tên cột date dùng để purge.
# Chỉ fact/staging tables (có date column).
# Dimension tables (dim_*) KHÔNG cần purge: ReplacingMergeTree(updated_at) đã idempotent.
_FACT_DATE_MAP: dict = {
    "fad_ad_daily_report":            "date_start",
    "fact_fb_ad_daily":               "date_start",
    "fact_fb_ad_creative_daily":      "date_start",
    "fact_fb_ad_demographic_daily":   "date_start",
    "gad_campaign_daily_report":      "date",
    "gad_ad_group_daily_report":      "date",
    "gad_account_daily_report":       "date",
    "gad_keyword_performance_report": "date",
    "gad_age_report":                 "date",
    "gad_gender_report":              "date",
    "gad_ad_asset_daily_report":      "date",
    "gad_click_type_report":          "date",
    "fact_gg_campaign_daily":         "date",
    "fact_gg_adgroup_daily":          "date",
    "fact_gg_keyword_daily":          "date",
    "fact_gg_age_daily":              "date",
    "fact_gg_gender_daily":           "date",
    "fact_gg_asset_daily":            "date",
    "fact_gg_click_type_daily":       "date",
    "tta_ad_performance":             "stat_time_day",
    "fact_tta_ad_daily":              "date",
}

def purge_all_fact_tables(process_date: str) -> None:
    """Xóa data của process_date khỏi tất cả fact/staging tables trước khi ghi mới."""
    print(f"\n[PURGE] Xóa data cũ cho {process_date} để đảm bảo idempotency...")
    for table, date_col in _FACT_DATE_MAP.items():
        _purge_date(table, date_col, process_date)
    print("[PURGE] Xong.\n")

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


def read_table(spark: SparkSession, table_name: str, process_date: str = None):
    """
    Read JSON files for a MinIO landing-zone table.

    If process_date is given (YYYY-MM-DD), read only that day's partition:
      {bucket}/{table_name}/year=YYYY/month=MM/day=DD/*.json

    If process_date is None (legacy), read all files recursively:
      {bucket}/{table_name}/
    """
    if process_date:
        year, month, day = process_date.split("-")
        path = (
            f"s3a://{MINIO_BUCKET}/{table_name}/"
            f"year={year}/month={month}/day={day}/*.json"
        )
    else:
        # Legacy: read all files (backward-compatible with direct MinIO uploads)
        path = f"s3a://{MINIO_BUCKET}/{table_name}/"
    try:
        read_opts = spark.read
        if process_date:
            # Kafka Connect writes JSON Lines (one record per line)
            df = read_opts.json(path)
        else:
            # Legacy direct uploads are JSON arrays (multiLine)
            df = read_opts.option("multiLine", "true") \
                          .option("recursiveFileLookup", "true") \
                          .json(path)
        print(f"  [READ] {table_name}: {df.count()} rows (date={process_date or 'ALL'})")
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


# ── Google Ads batch processing ───────────────────────────────────────────────

def process_gg_campaign(df) -> None:
    base = df.filter(F.col("id").isNotNull()).select(
        F.col("id").alias("campaign_id"),
        F.coalesce(F.col("name"), F.lit("Unknown")).alias("campaign_name"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.coalesce(F.col("impressions").cast("int"),     F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),          F.lit(0)).alias("clicks"),
        F.coalesce(F.col("cost").cast("float"),          F.lit(0.0)).alias("cost"),
        F.coalesce(F.col("all_conversions").cast("int"), F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("ctr").cast("float"),           F.lit(0.0)).alias("ctr"),
    )
    base.persist()
    write_ch(
        base.select("campaign_id", "campaign_name").dropDuplicates(["campaign_id"]),
        "marketing_db.dim_gg_campaign",
    )
    write_ch(base, "marketing_db.gad_campaign_daily_report")
    write_ch(
        base.select("campaign_id", "date", "impressions", "clicks", "cost", "all_conversions", "ctr"),
        "marketing_db.fact_gg_campaign_daily",
    )
    base.unpersist()


def process_gg_adgroup(df) -> None:
    base = df.filter(F.col("id").isNotNull()).select(
        F.col("id").alias("adgroup_id"),
        F.coalesce(F.col("name"), F.lit("Unknown")).alias("adgroup_name"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.coalesce(F.col("impressions").cast("int"),     F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),          F.lit(0)).alias("clicks"),
        F.coalesce(F.col("cost").cast("float"),          F.lit(0.0)).alias("cost"),
        F.coalesce(F.col("all_conversions").cast("int"), F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("ctr").cast("float"),           F.lit(0.0)).alias("ctr"),
    )
    base.persist()
    write_ch(base, "marketing_db.gad_ad_group_daily_report")
    write_ch(
        base.select("adgroup_id", "date", "impressions", "clicks", "cost", "all_conversions", "ctr"),
        "marketing_db.fact_gg_adgroup_daily",
    )
    base.unpersist()


def process_gg_account(df) -> None:
    flat = df.filter(F.col("account_id").isNotNull()).select(
        F.col("account_id"),
        F.coalesce(F.col("name"), F.lit("Unknown")).alias("account_name"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.coalesce(F.col("impressions").cast("int"),     F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),          F.lit(0)).alias("clicks"),
        F.coalesce(F.col("cost").cast("float"),          F.lit(0.0)).alias("cost"),
        F.coalesce(F.col("all_conversions").cast("int"), F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("ctr").cast("float"),           F.lit(0.0)).alias("ctr"),
    )
    flat.persist()
    write_ch(flat, "marketing_db.gad_account_daily_report")
    write_ch(
        flat.select("account_id", "account_name").dropDuplicates(["account_id"]),
        "marketing_db.dim_account",
    )
    flat.unpersist()


def process_gg_keyword(df) -> None:
    valid = df.filter(F.col("adgroup_id").isNotNull() & F.col("keyword").isNotNull())
    valid.persist()

    write_ch(
        valid.select(
            F.col("campaign_id"),
            F.coalesce(F.col("account_id"), F.lit("")).alias("account_id"),
            F.coalesce(F.col("campaign_name"), F.lit("Unknown")).alias("campaign_name"),
        ).filter(F.col("campaign_id").isNotNull()).dropDuplicates(["campaign_id"]),
        "marketing_db.dim_campaign",
    )
    write_ch(
        valid.select(
            F.col("adgroup_id"),
            F.coalesce(F.col("campaign_id"), F.lit("")).alias("campaign_id"),
            F.coalesce(F.col("adgroup_name"), F.lit("Unknown")).alias("adgroup_name"),
        ).dropDuplicates(["adgroup_id"]),
        "marketing_db.dim_gg_adgroup",
    )

    flat = valid.select(
        F.col("adgroup_id"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.col("campaign_id"),
        F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
        F.coalesce(F.col("adgroup_name"), F.lit("")).alias("adgroup_name"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"), F.lit("")).alias("account_name"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device"),
        F.col("keyword"),
        F.coalesce(F.col("quality_score").cast("int"), F.lit(0)).alias("quality_score"),
        F.coalesce(F.col("impressions").cast("int"),          F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),               F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),                F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("conversions").cast("int"),          F.lit(0)).alias("conversions"),
        F.coalesce(F.col("all_conversions").cast("int"),      F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("average_cpc").cast("float"),        F.lit(0.0)).alias("average_cpc"),
        F.coalesce(F.col("cost_per_conversion").cast("float"), F.lit(0.0)).alias("cost_per_conversion"),
        F.coalesce(F.col("cost").cast("float"),               F.lit(0.0)).alias("cost"),
    )
    write_ch(flat, "marketing_db.gad_keyword_performance_report")
    write_ch(
        flat.select(
            "date", "account_id", "campaign_id", "adgroup_id", "keyword", "device",
            "quality_score", "impressions", "clicks", "cost",
            "conversions", "all_conversions", "ctr", "average_cpc", "cost_per_conversion",
        ),
        "marketing_db.fact_gg_keyword_daily",
    )
    valid.unpersist()


def _gg_breakdown_select(df, dim_col, dim_name):
    return df.filter(F.col("adgroup_id").isNotNull()).select(
        F.col("adgroup_id"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.col("campaign_id"),
        F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
        F.coalesce(F.col("adgroup_name"), F.lit("")).alias("adgroup_name"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"), F.lit("")).alias("account_name"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device"),
        F.col(dim_col).alias(dim_name),
        F.coalesce(F.col("impressions").cast("int"),           F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),                F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),                 F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("conversions").cast("int"),           F.lit(0)).alias("conversions"),
        F.coalesce(F.col("all_conversions").cast("int"),       F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("average_cpc").cast("float"),         F.lit(0.0)).alias("average_cpc"),
        F.coalesce(F.col("cost_per_conversion").cast("float"), F.lit(0.0)).alias("cost_per_conversion"),
        F.coalesce(F.col("cost").cast("float"),                F.lit(0.0)).alias("cost"),
    )


def process_gg_age(df) -> None:
    flat = _gg_breakdown_select(df, "age_range", "age_range")
    write_ch(flat, "marketing_db.gad_age_report")
    write_ch(
        flat.select("date", "account_id", "campaign_id", "adgroup_id",
                    "age_range", "device", "impressions", "clicks", "cost",
                    "conversions", "all_conversions", "ctr", "average_cpc", "cost_per_conversion"),
        "marketing_db.fact_gg_age_daily",
    )


def process_gg_gender(df) -> None:
    flat = _gg_breakdown_select(df, "gender", "gender")
    write_ch(flat, "marketing_db.gad_gender_report")
    write_ch(
        flat.select("date", "account_id", "campaign_id", "adgroup_id",
                    "gender", "device", "impressions", "clicks", "cost",
                    "conversions", "all_conversions", "ctr", "average_cpc", "cost_per_conversion"),
        "marketing_db.fact_gg_gender_daily",
    )


def process_gg_ad_asset(df) -> None:
    valid = df.filter(F.col("ad_id").isNotNull() & F.col("asset_id").isNotNull())
    valid.persist()

    write_ch(
        valid.select(
            F.col("asset_id"),
            F.coalesce(F.col("ad_id"), F.lit("")).alias("ad_id"),
            F.coalesce(F.col("asset_name"), F.lit("")).alias("asset_name"),
            F.coalesce(F.col("asset_type"), F.lit("")).alias("asset_type"),
            F.coalesce(F.col("asset_text"), F.lit("")).alias("asset_text"),
            F.coalesce(F.col("image_url"), F.lit("")).alias("image_url"),
        ).dropDuplicates(["asset_id"]),
        "marketing_db.dim_gg_asset",
    )
    write_ch(
        valid.select(
            F.col("ad_id"), F.col("asset_id"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            F.col("campaign_id"),
            F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
            F.col("adgroup_id"),
            F.coalesce(F.col("adgroup_name"), F.lit("")).alias("adgroup_name"),
            F.coalesce(F.col("asset_name"), F.lit("")).alias("asset_name"),
            F.coalesce(F.col("asset_type"), F.lit("")).alias("asset_type"),
            F.coalesce(F.col("asset_text"), F.lit("")).alias("asset_text"),
            F.coalesce(F.col("image_url"), F.lit("")).alias("image_url"),
            F.coalesce(F.col("asset_performance"), F.lit("")).alias("asset_performance"),
            F.coalesce(F.col("impressions").cast("int"),     F.lit(0)).alias("impressions"),
            F.coalesce(F.col("clicks").cast("int"),          F.lit(0)).alias("clicks"),
            F.coalesce(F.col("ctr").cast("float"),           F.lit(0.0)).alias("ctr"),
            F.coalesce(F.col("all_conversions").cast("int"), F.lit(0)).alias("all_conversions"),
            F.coalesce(F.col("cost").cast("float"),          F.lit(0.0)).alias("cost"),
            F.col("account_id"),
            F.coalesce(F.col("account_name"), F.lit("")).alias("account_name"),
        ),
        "marketing_db.gad_ad_asset_daily_report",
    )
    write_ch(
        valid.select(
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            F.col("account_id"), F.col("campaign_id"), F.col("adgroup_id"),
            F.col("ad_id"), F.col("asset_id"),
            F.coalesce(F.col("asset_performance"), F.lit("")).alias("asset_performance"),
            F.coalesce(F.col("impressions").cast("int"),     F.lit(0)).alias("impressions"),
            F.coalesce(F.col("clicks").cast("int"),          F.lit(0)).alias("clicks"),
            F.coalesce(F.col("cost").cast("float"),          F.lit(0.0)).alias("cost"),
            F.coalesce(F.col("all_conversions").cast("int"), F.lit(0)).alias("all_conversions"),
            F.coalesce(F.col("ctr").cast("float"),           F.lit(0.0)).alias("ctr"),
        ),
        "marketing_db.fact_gg_asset_daily",
    )
    valid.unpersist()


def process_gg_click_type(df) -> None:
    valid = df.filter(F.col("campaign_id").isNotNull() & F.col("click_type").isNotNull())
    flat = valid.select(
        F.col("campaign_id"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
        F.col("click_type"),
        F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
        F.coalesce(F.col("campaign_status"), F.lit("")).alias("campaign_status"),
        F.coalesce(F.col("impressions").cast("int"),     F.lit(0)).alias("impressions"),
        F.coalesce(F.col("clicks").cast("int"),          F.lit(0)).alias("clicks"),
        F.coalesce(F.col("ctr").cast("float"),           F.lit(0.0)).alias("ctr"),
        F.coalesce(F.col("conversions").cast("int"),     F.lit(0)).alias("conversions"),
        F.coalesce(F.col("all_conversions").cast("int"), F.lit(0)).alias("all_conversions"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device"),
        F.coalesce(F.col("ad_network_type"), F.lit("")).alias("ad_network_type"),
        F.coalesce(F.col("cost").cast("float"),          F.lit(0.0)).alias("cost"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"), F.lit("")).alias("account_name"),
    )
    write_ch(flat, "marketing_db.gad_click_type_report")
    write_ch(
        flat.select(
            "date", "account_id", "campaign_id", "click_type", "device", "ad_network_type",
            "impressions", "clicks", "cost", "conversions", "all_conversions", "ctr",
        ),
        "marketing_db.fact_gg_click_type_daily",
    )


# ── TikTok Ads batch processing ───────────────────────────────────────────────

def process_tta_ad_performance(df) -> None:
    """
    Source  : TTA_ad_performance (mock output — single flat table)
    Targets : tta_ad_performance  (flat/denormalized raw staging)
              dim_tta_advertiser
              dim_tta_ad
              fact_tta_ad_daily

    Field notes:
      stat_time_day  : "2026-05-25 17:00:00.000" → parsed to Date
      start_date     : "2026-05-01 00:00:00.000" → parsed to Date
      end_date       : "2026-07-30 00:00:00.000" → parsed to Date
      createdAt      : "2026-05-25 17:00:00.123" → parsed to DateTime
      conversion     : singular field name (TikTok convention)
    """
    base = df.filter(F.col("ad_id").isNotNull()).select(
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
        F.to_timestamp(F.col("createdAt"), "yyyy-MM-dd HH:mm:ss.SSS").alias("createdAt"),
        F.to_timestamp(F.col("updatedAt"), "yyyy-MM-dd HH:mm:ss.SSS").alias("updatedAt"),
    )
    base.persist()

    # -- dim_tta_advertiser ---------------------------------------------------
    write_ch(
        base.select("advertiser_id", "advertiser_name")
            .dropDuplicates(["advertiser_id"]),
        "marketing_db.dim_tta_advertiser",
    )

    # -- dim_tta_ad -----------------------------------------------------------
    write_ch(
        base.select(
            F.col("ad_id"),
            F.col("advertiser_id"),
            F.col("campaign_name"),
            F.col("adgroup_name"),
            F.col("ad_name"),
            F.col("ad_text"),
        ).dropDuplicates(["ad_id"]),
        "marketing_db.dim_tta_ad",
    )

    # -- tta_ad_performance (flat/denormalized staging) -----------------------
    write_ch(base, "marketing_db.tta_ad_performance")

    # -- fact_tta_ad_daily ----------------------------------------------------
    write_ch(
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
        "marketing_db.fact_tta_ad_daily",
    )

    base.unpersist()


def main():
    parser = argparse.ArgumentParser(description="MinIO to ClickHouse Batch Ingestion")
    parser.add_argument(
        "--date", type=str, default=None,
        help="Process date (YYYY-MM-DD). If omitted, reads ALL data (legacy mode)."
    )
    args = parser.parse_args()
    process_date = args.date

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 55)
    print("  MinIO -> ClickHouse Batch Ingestion")
    if process_date:
        print(f"  Processing date: {process_date}")
    else:
        print("  Mode: FULL (all data)")
    print("=" * 55)

    # Idempotency: xóa data cũ cho ngày này trước khi ghi.
    # Nếu Airflow retry task, sẽ không bị duplicate.
    # Legacy mode (process_date=None) bỏ qua bước này.
    if process_date:
        purge_all_fact_tables(process_date)

    print("\n[1/10] fad_ad_daily_report")
    df_daily = read_table(spark, "fad_ad_daily_report", process_date)
    if df_daily is not None:
        df_daily.persist()
        process_ad_daily(df_daily)
        populate_dim_date(spark, df_daily)
        df_daily.unpersist()

    print("\n[2/10] fad_age_gender_detailed_report")
    df_demo = read_table(spark, "fad_age_gender_detailed_report", process_date)
    if df_demo is not None:
        process_age_gender(df_demo)

    print("\n[3/10] gad_campaign_daily_report")
    df_cam = read_table(spark, "gad_campaign_daily_report", process_date)
    if df_cam is not None:
        process_gg_campaign(df_cam)

    print("\n[4/10] gad_ad_group_daily_report")
    df_grp = read_table(spark, "gad_ad_group_daily_report", process_date)
    if df_grp is not None:
        process_gg_adgroup(df_grp)

    print("\n[5/10] gad_account_daily_report")
    df_acc = read_table(spark, "gad_account_daily_report", process_date)
    if df_acc is not None:
        process_gg_account(df_acc)

    print("\n[6/10] gad_keyword_performance_report")
    df_kw = read_table(spark, "gad_keyword_performance_report", process_date)
    if df_kw is not None:
        process_gg_keyword(df_kw)

    print("\n[7/10] gad_age_report")
    df_age = read_table(spark, "gad_age_report", process_date)
    if df_age is not None:
        process_gg_age(df_age)

    print("\n[7b/10] gad_gender_report")
    df_gender = read_table(spark, "gad_gender_report", process_date)
    if df_gender is not None:
        process_gg_gender(df_gender)

    print("\n[8/10] gad_ad_asset_daily_report")
    df_asset = read_table(spark, "gad_ad_asset_daily_report", process_date)
    if df_asset is not None:
        process_gg_ad_asset(df_asset)

    print("\n[9/10] gad_click_type_report")
    df_ct = read_table(spark, "gad_click_type_report", process_date)
    if df_ct is not None:
        process_gg_click_type(df_ct)

    print("\n[10/10] TTA_ad_performance")
    df_tta = read_table(spark, "TTA_ad_performance", process_date)
    if df_tta is not None:
        process_tta_ad_performance(df_tta)

    print("\n[DONE] All tables written to ClickHouse.")
    spark.stop()

if __name__ == "__main__":
    main()

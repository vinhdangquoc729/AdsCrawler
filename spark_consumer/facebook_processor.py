# spark_consumer/facebook_processor.py

from pyspark.sql.functions import from_json, col, to_date, coalesce, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from base_processor import BaseDataProcessor


class FacebookAdsProcessor(BaseDataProcessor):
    """Processor specialized for Facebook Ads payloads."""
    def __init__(self, spark_session):
        super().__init__(spark_session)

        # Schemas for different report types
        self.daily_schema = StructType([
            StructField("id", StringType()), StructField("name", StringType()),
            StructField("campaign_id", StringType()), StructField("campaign_name", StringType()),
            StructField("adset_id", StringType()), StructField("adset_name", StringType()),
            StructField("account_id", StringType()), StructField("account_name", StringType()),
            StructField("date_start", StringType()), StructField("status", StringType()),
            StructField("effective_status", StringType()), StructField("created_time", StringType()),
            StructField("spend", StringType()), StructField("impressions", StringType()),
            StructField("reach", StringType()), StructField("clicks", StringType()),
            StructField("ctr", StringType()), StructField("cpc", StringType()),
            StructField("cpm", StringType()), StructField("frequency", StringType()),
            StructField("New Messaging Connections", IntegerType()), StructField("Cost per New Messaging", StringType()),
            StructField("Link clicks", IntegerType()), StructField("Landing page views", IntegerType())
        ])

        self.creative_schema = StructType([
            StructField("id", StringType()), StructField("account_id", StringType()),
            StructField("creative_id", StringType()), StructField("creative_title", StringType()),
            StructField("creative_body", StringType()), StructField("creative_thumbnail_raw_url", StringType()),
            StructField("creative_link", StringType()), StructField("page_name", StringType()),
            StructField("date_start", StringType()), StructField("spend", StringType()),
            StructField("impressions", StringType()), StructField("reach", StringType()),
            StructField("clicks", StringType()), StructField("New Messaging Connections", IntegerType()),
            StructField("Post engagements", IntegerType()), StructField("Post reactions", IntegerType()),
            StructField("Post shares", IntegerType()), StructField("Photo views", IntegerType())
        ])

        self.demographic_schema = StructType([
            StructField("id", StringType()), StructField("account_id", StringType()),
            StructField("date_start", StringType()), StructField("age", StringType()),
            StructField("gender", StringType()), StructField("spend", StringType()),
            StructField("impressions", StringType()), StructField("reach", StringType()),
            StructField("clicks", StringType()), StructField("inline_link_clicks", IntegerType()),
            StructField("New Messaging Connections", IntegerType())
        ])

    def process_batch(self, df, epoch_id):
        df.persist()
        print(f"\n=== BẮT ĐẦU XỬ LÝ BATCH {epoch_id} ===")

        # AD DAILY
        daily_raw = df.filter((col("platform") == "facebook") & (col("report_type") == "ad_daily"))
        if not daily_raw.isEmpty():
            parsed_daily = daily_raw.select(from_json(col("raw_data"), self.daily_schema).alias("d")).select("d.*")
            valid_daily = parsed_daily.filter(col("id").isNotNull() & col("date_start").isNotNull())

            dim_account = valid_daily.select(
                col("account_id"),
                coalesce(col("account_name"), lit("Unknown")).alias("account_name")
            ).dropDuplicates(["account_id"])

            dim_campaign = valid_daily.select(
                col("campaign_id"), col("account_id"),
                coalesce(col("campaign_name"), lit("Unknown")).alias("campaign_name")
            ).filter(col("campaign_id").isNotNull()).dropDuplicates(["campaign_id"])

            dim_adset = valid_daily.select(
                col("adset_id"), col("campaign_id"),
                coalesce(col("adset_name"), lit("Unknown")).alias("adset_name")
            ).filter(col("adset_id").isNotNull()).dropDuplicates(["adset_id"])

            dim_ad = valid_daily.select(
                col("id").alias("ad_id"), col("adset_id"),
                coalesce(col("name"), lit("Unknown")).alias("ad_name"),
                coalesce(col("status"), lit("UNKNOWN")).alias("status"),
                coalesce(col("effective_status"), lit("UNKNOWN")).alias("effective_status"),
                to_timestamp(col("created_time"), "yyyy-MM-dd'T'HH:mm:ssZ").alias("created_time")
            ).dropDuplicates(["ad_id"])

            fact_daily = valid_daily.select(
                to_date(col("date_start"), "yyyy-MM-dd").alias("date_start"),
                col("account_id"), col("id").alias("ad_id"),
                coalesce(col("spend").cast("float"), lit(0.0)).alias("spend"),
                coalesce(col("impressions").cast("int"), lit(0)).alias("impressions"),
                coalesce(col("reach").cast("int"), lit(0)).alias("reach"),
                coalesce(col("clicks").cast("int"), lit(0)).alias("clicks"),
                coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
                coalesce(col("cpc").cast("float"), lit(0.0)).alias("cpc"),
                coalesce(col("cpm").cast("float"), lit(0.0)).alias("cpm"),
                coalesce(col("frequency").cast("float"), lit(0.0)).alias("frequency"),
                coalesce(col("New Messaging Connections").cast("int"), lit(0)).alias("new_messaging_connections"),
                coalesce(col("Cost per New Messaging").cast("float"), lit(0.0)).alias("cost_per_new_messaging"),
                coalesce(col("Link clicks").cast("int"), lit(0)).alias("link_clicks"),
                coalesce(col("Landing page views").cast("int"), lit(0)).alias("landing_page_views")
            )

            print(" -> Đang ghi Báo cáo Daily (Dims & Facts)...")
            self.write_to_clickhouse(dim_account, "marketing_db.dim_account")
            self.write_to_clickhouse(dim_campaign, "marketing_db.dim_campaign")
            self.write_to_clickhouse(dim_adset, "marketing_db.dim_adset")
            self.write_to_clickhouse(dim_ad, "marketing_db.dim_ad")
            self.write_to_clickhouse(fact_daily, "marketing_db.fact_fb_ad_daily")

            # --- Ghi bản denormalized vào bảng fad_ad_daily_report ---
            print(" -> Đang ghi bảng denormalized fad_ad_daily_report...")
            daily_report = valid_daily.select(
                col("id"),
                coalesce(col("name"), lit("")).alias("name"),
                col("adset_id"),
                coalesce(col("adset_name"), lit("")).alias("adset_name"),
                col("campaign_id"),
                coalesce(col("campaign_name"), lit("")).alias("campaign_name"),
                col("account_id"),
                coalesce(col("account_name"), lit("")).alias("account_name"),
                to_date(col("date_start"), "yyyy-MM-dd").alias("date_start"),
                to_date(col("date_start"), "yyyy-MM-dd").alias("date_stop"),
                # Chỉ số Hiệu suất
                coalesce(col("spend").cast("float"), lit(0.0)).alias("spend"),
                coalesce(col("impressions").cast("int"), lit(0)).alias("impressions"),
                coalesce(col("reach").cast("int"), lit(0)).alias("reach"),
                coalesce(col("clicks").cast("int"), lit(0)).alias("clicks"),
                coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
                coalesce(col("cpc").cast("float"), lit(0.0)).alias("cpc"),
                coalesce(col("cpm").cast("float"), lit(0.0)).alias("cpm"),
                coalesce(col("frequency").cast("float"), lit(0.0)).alias("frequency"),
                # Chỉ số Chuyển đổi
                coalesce(col("New Messaging Connections").cast("int"), lit(0)).alias("new_messaging_connections"),
                coalesce(col("Cost per New Messaging").cast("float"), lit(0.0)).alias("cost_per_new_messaging"),
                coalesce(col("Link clicks").cast("int"), lit(0)).alias("link_clicks"),
                coalesce(col("Landing page views").cast("int"), lit(0)).alias("landing_page_views")
            )
            self.write_to_clickhouse(daily_report, "marketing_db.fad_ad_daily_report")

        # AD CREATIVE
        creative_raw = df.filter((col("platform") == "facebook") & (col("report_type") == "ad_creative"))
        if not creative_raw.isEmpty():
            parsed_cr = creative_raw.select(from_json(col("raw_data"), self.creative_schema).alias("d")).select("d.*")
            valid_cr = parsed_cr.filter(col("id").isNotNull() & col("creative_id").isNotNull() & col("date_start").isNotNull())

            dim_creative = valid_cr.select(
                col("creative_id"),
                coalesce(col("creative_title"), lit("")).alias("creative_title"),
                coalesce(col("creative_body"), lit("")).alias("creative_body"),
                coalesce(col("creative_thumbnail_raw_url"), lit("")).alias("creative_thumbnail_raw_url"),
                coalesce(col("creative_link"), lit("")).alias("creative_link"),
                coalesce(col("page_name"), lit("Unknown")).alias("page_name")
            ).dropDuplicates(["creative_id"])

            fact_creative = valid_cr.select(
                to_date(col("date_start"), "yyyy-MM-dd").alias("date_start"),
                col("account_id"), col("id").alias("ad_id"), col("creative_id"),
                coalesce(col("spend").cast("float"), lit(0.0)).alias("spend"),
                coalesce(col("impressions").cast("int"), lit(0)).alias("impressions"),
                coalesce(col("reach").cast("int"), lit(0)).alias("reach"),
                coalesce(col("clicks").cast("int"), lit(0)).alias("clicks"),
                coalesce(col("New Messaging Connections").cast("int"), lit(0)).alias("new_messaging_connections"),
                coalesce(col("Post engagements").cast("int"), lit(0)).alias("post_engagements"),
                coalesce(col("Post reactions").cast("int"), lit(0)).alias("post_reactions"),
                coalesce(col("Post shares").cast("int"), lit(0)).alias("post_shares"),
                coalesce(col("Photo views").cast("int"), lit(0)).alias("photo_views")
            )

            print(" -> Đang ghi Báo cáo Creative (Dims & Facts)...")
            self.write_to_clickhouse(dim_creative, "marketing_db.dim_creative")
            self.write_to_clickhouse(fact_creative, "marketing_db.fact_fb_ad_creative_daily")

        # AGE & GENDER
        demo_raw = df.filter((col("platform") == "facebook") & (col("report_type") == "age_gender"))
        if not demo_raw.isEmpty():
            parsed_demo = demo_raw.select(from_json(col("raw_data"), self.demographic_schema).alias("d")).select("d.*")
            valid_demo = parsed_demo.filter(col("id").isNotNull() & col("date_start").isNotNull() & col("age").isNotNull())

            fact_demo = valid_demo.select(
                to_date(col("date_start"), "yyyy-MM-dd").alias("date_start"),
                col("account_id"), col("id").alias("ad_id"), col("age"),
                coalesce(col("gender"), lit("unknown")).alias("gender"),
                coalesce(col("spend").cast("float"), lit(0.0)).alias("spend"),
                coalesce(col("impressions").cast("int"), lit(0)).alias("impressions"),
                coalesce(col("reach").cast("int"), lit(0)).alias("reach"),
                coalesce(col("clicks").cast("int"), lit(0)).alias("clicks"),
                coalesce(col("inline_link_clicks").cast("int"), lit(0)).alias("inline_link_clicks"),
                coalesce(col("New Messaging Connections").cast("int"), lit(0)).alias("new_messaging_connections")
            )

            print(" -> Đang ghi Báo cáo Demographic (Facts)...")
            self.write_to_clickhouse(fact_demo, "marketing_db.fact_fb_ad_demographic_daily")

        df.unpersist()
        print(f"=== HOÀN TẤT BATCH {epoch_id} ===\n")

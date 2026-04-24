# spark_consumer/google_processor.py

from pyspark.sql.functions import from_json, col, to_date, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from base_processor import BaseDataProcessor


class GoogleAdsProcessor(BaseDataProcessor):
    """Processor for Google Ads stream payloads from topic_google_raw.

    Each report type writes to two layers:
      - gad_*  flat/denormalized report  (mirrors fad_ for Facebook)
      - dim_gg_ / fact_gg_  snowflake layer  (mirrors dim_ / fact_fb_ for Facebook)
    """

    def __init__(self, spark_session):
        super().__init__(spark_session)

        self.campaign_daily_schema = StructType([
            StructField("id", StringType()), StructField("name", StringType()),
            StructField("date", StringType()), StructField("impressions", IntegerType()),
            StructField("clicks", IntegerType()), StructField("cost", FloatType()),
            StructField("all_conversions", IntegerType()), StructField("ctr", FloatType()),
        ])

        self.ad_group_daily_schema = StructType([
            StructField("id", StringType()), StructField("name", StringType()),
            StructField("date", StringType()), StructField("impressions", IntegerType()),
            StructField("clicks", IntegerType()), StructField("cost", FloatType()),
            StructField("all_conversions", IntegerType()), StructField("ctr", FloatType()),
        ])

        self.account_schema = StructType([
            StructField("id", StringType()), StructField("name", StringType()),
            StructField("date", StringType()), StructField("impressions", IntegerType()),
            StructField("clicks", IntegerType()), StructField("cost", FloatType()),
            StructField("all_conversions", IntegerType()),
            StructField("account_id", StringType()), StructField("ctr", FloatType()),
        ])

        _breakdown_fields = [
            StructField("adgroup_id", StringType()), StructField("date", StringType()),
            StructField("campaign_id", StringType()), StructField("campaign_name", StringType()),
            StructField("adgroup_name", StringType()), StructField("account_id", StringType()),
            StructField("account_name", StringType()), StructField("device", StringType()),
            StructField("impressions", IntegerType()), StructField("clicks", IntegerType()),
            StructField("ctr", FloatType()), StructField("conversions", IntegerType()),
            StructField("all_conversions", IntegerType()), StructField("average_cpc", FloatType()),
            StructField("cost_per_conversion", FloatType()), StructField("cost", FloatType()),
        ]

        self.keyword_schema = StructType(_breakdown_fields + [
            StructField("keyword", StringType()), StructField("quality_score", IntegerType()),
        ])

        self.age_schema = StructType(_breakdown_fields + [
            StructField("age_range", StringType()),
        ])

        self.gender_schema = StructType(_breakdown_fields + [
            StructField("gender", StringType()),
        ])

        self.ad_asset_schema = StructType([
            StructField("ad_id", StringType()), StructField("asset_id", StringType()),
            StructField("date", StringType()), StructField("campaign_id", StringType()),
            StructField("campaign_name", StringType()), StructField("adgroup_id", StringType()),
            StructField("adgroup_name", StringType()), StructField("asset_name", StringType()),
            StructField("asset_type", StringType()), StructField("asset_text", StringType()),
            StructField("image_url", StringType()), StructField("asset_performance", StringType()),
            StructField("impressions", IntegerType()), StructField("clicks", IntegerType()),
            StructField("ctr", FloatType()), StructField("all_conversions", IntegerType()),
            StructField("cost", FloatType()), StructField("account_id", StringType()),
            StructField("account_name", StringType()),
        ])

        self.click_type_schema = StructType([
            StructField("campaign_id", StringType()), StructField("date", StringType()),
            StructField("click_type", StringType()), StructField("campaign_name", StringType()),
            StructField("campaign_status", StringType()), StructField("impressions", IntegerType()),
            StructField("clicks", IntegerType()), StructField("ctr", FloatType()),
            StructField("conversions", IntegerType()), StructField("all_conversions", IntegerType()),
            StructField("device", StringType()), StructField("ad_network_type", StringType()),
            StructField("cost", FloatType()), StructField("account_id", StringType()),
            StructField("account_name", StringType()),
        ])

    # ── Public entry point ────────────────────────────────────────────────────

    def process_batch(self, df, epoch_id):
        if df.isEmpty():
            return
        df.persist()
        print(f"\n=== GOOGLE BATCH {epoch_id} ===")

        self._handle_campaign_daily(df)
        self._handle_ad_group_daily(df)
        self._handle_account(df)
        self._handle_keyword(df)
        self._handle_demographic(df)
        self._handle_ad_asset(df)
        self._handle_click_type(df)

        df.unpersist()
        print(f"=== GOOGLE BATCH {epoch_id} DONE ===\n")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _parse(self, df, report_type, schema):
        raw = df.filter((col("platform") == "google") & (col("report_type") == report_type))
        if raw.isEmpty():
            return None
        return raw.select(from_json(col("raw_data"), schema).alias("d")).select("d.*")

    def _f(self, df, *filter_cols):
        """Return df filtered to rows where all listed columns are not null."""
        condition = col(filter_cols[0]).isNotNull()
        for c in filter_cols[1:]:
            condition = condition & col(c).isNotNull()
        return df.filter(condition)

    # ── Report handlers ───────────────────────────────────────────────────────

    def _handle_campaign_daily(self, df):
        parsed = self._parse(df, "campaign_daily", self.campaign_daily_schema)
        if parsed is None:
            return
        parsed.persist()

        base = self._f(parsed, "id").select(
            col("id").alias("campaign_id"),
            coalesce(col("name"), lit("Unknown")).alias("campaign_name"),
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
        )

        self.write_to_clickhouse(base, "marketing_db.gad_campaign_daily_report")
        self.write_to_clickhouse(
            base.select("campaign_id", "date", "impressions", "clicks", "cost", "all_conversions", "ctr"),
            "marketing_db.fact_gg_campaign_daily",
        )

        parsed.unpersist()

    def _handle_ad_group_daily(self, df):
        parsed = self._parse(df, "ad_group_daily", self.ad_group_daily_schema)
        if parsed is None:
            return
        parsed.persist()

        base = self._f(parsed, "id").select(
            col("id").alias("adgroup_id"),
            coalesce(col("name"), lit("Unknown")).alias("adgroup_name"),
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
        )

        self.write_to_clickhouse(base, "marketing_db.gad_ad_group_daily_report")
        self.write_to_clickhouse(
            base.select("adgroup_id", "date", "impressions", "clicks", "cost", "all_conversions", "ctr"),
            "marketing_db.fact_gg_adgroup_daily",
        )

        parsed.unpersist()

    def _handle_account(self, df):
        parsed = self._parse(df, "account", self.account_schema)
        if parsed is None:
            return

        flat = self._f(parsed, "account_id").select(
            col("account_id"),
            coalesce(col("name"), lit("Unknown")).alias("account_name"),
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
        )
        self.write_to_clickhouse(flat, "marketing_db.gad_account_daily_report")

        # populate shared dim_account
        dim = flat.select(
            col("account_id"),
            col("account_name"),
        ).dropDuplicates(["account_id"])
        self.write_to_clickhouse(dim, "marketing_db.dim_account")

    def _handle_keyword(self, df):
        parsed = self._parse(df, "keyword", self.keyword_schema)
        if parsed is None:
            return
        parsed.persist()

        valid = self._f(parsed, "adgroup_id", "keyword")

        # populate shared dims from keyword records (they carry full hierarchy)
        self.write_to_clickhouse(
            valid.select(
                col("campaign_id"),
                coalesce(col("account_id"), lit("")).alias("account_id"),
                coalesce(col("campaign_name"), lit("Unknown")).alias("campaign_name"),
            ).filter(col("campaign_id").isNotNull()).dropDuplicates(["campaign_id"]),
            "marketing_db.dim_campaign",
        )
        self.write_to_clickhouse(
            valid.select(
                col("adgroup_id"),
                coalesce(col("campaign_id"), lit("")).alias("campaign_id"),
                coalesce(col("adgroup_name"), lit("Unknown")).alias("adgroup_name"),
            ).dropDuplicates(["adgroup_id"]),
            "marketing_db.dim_gg_adgroup",
        )

        metrics = valid.select(
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            col("account_id"), col("campaign_id"), col("adgroup_id"),
            col("keyword"), coalesce(col("device"), lit("UNKNOWN")).alias("device"),
            coalesce(col("quality_score"), lit(0)).alias("quality_score"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
            coalesce(col("conversions"), lit(0)).alias("conversions"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("average_cpc").cast("float"), lit(0.0)).alias("average_cpc"),
            coalesce(col("cost_per_conversion").cast("float"), lit(0.0)).alias("cost_per_conversion"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
        )

        flat = valid.select(
            col("adgroup_id"), to_date(col("date"), "yyyy-MM-dd").alias("date"),
            col("campaign_id"), coalesce(col("campaign_name"), lit("")).alias("campaign_name"),
            coalesce(col("adgroup_name"), lit("")).alias("adgroup_name"),
            col("account_id"), coalesce(col("account_name"), lit("")).alias("account_name"),
            coalesce(col("device"), lit("UNKNOWN")).alias("device"),
            col("keyword"), coalesce(col("quality_score"), lit(0)).alias("quality_score"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
            coalesce(col("conversions"), lit(0)).alias("conversions"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("average_cpc").cast("float"), lit(0.0)).alias("average_cpc"),
            coalesce(col("cost_per_conversion").cast("float"), lit(0.0)).alias("cost_per_conversion"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
        )
        self.write_to_clickhouse(flat, "marketing_db.gad_keyword_performance_report")
        self.write_to_clickhouse(metrics, "marketing_db.fact_gg_keyword_daily")

        parsed.unpersist()

    def _handle_demographic(self, df):
        """Merge age and gender breakdown records into a single demographic table."""
        age_parsed = self._parse(df, "age", self.age_schema)
        gender_parsed = self._parse(df, "gender", self.gender_schema)

        if age_parsed is None and gender_parsed is None:
            return

        def _shape(parsed, age_col, gender_col):
            return self._f(parsed, "adgroup_id").select(
                col("adgroup_id"), to_date(col("date"), "yyyy-MM-dd").alias("date"),
                col("campaign_id"),
                coalesce(col("campaign_name"), lit("")).alias("campaign_name"),
                coalesce(col("adgroup_name"), lit("")).alias("adgroup_name"),
                col("account_id"),
                coalesce(col("account_name"), lit("")).alias("account_name"),
                coalesce(col("device"), lit("UNKNOWN")).alias("device"),
                age_col.alias("age_range"),
                gender_col.alias("gender"),
                coalesce(col("impressions"), lit(0)).alias("impressions"),
                coalesce(col("clicks"), lit(0)).alias("clicks"),
                coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
                coalesce(col("conversions"), lit(0)).alias("conversions"),
                coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
                coalesce(col("average_cpc").cast("float"), lit(0.0)).alias("average_cpc"),
                coalesce(col("cost_per_conversion").cast("float"), lit(0.0)).alias("cost_per_conversion"),
                coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            )

        parts = []
        if age_parsed is not None:
            parts.append(_shape(age_parsed, col("age_range"), lit("")))
        if gender_parsed is not None:
            parts.append(_shape(gender_parsed, lit(""), col("gender")))

        combined = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
        combined.persist()

        self.write_to_clickhouse(combined, "marketing_db.gad_demographic_report")

        self.write_to_clickhouse(
            combined.select(
                col("date"), col("account_id"), col("campaign_id"), col("adgroup_id"),
                col("age_range"), col("gender"), col("device"),
                col("impressions"), col("clicks"), col("cost"),
                col("conversions"), col("all_conversions"),
                col("ctr"), col("average_cpc"), col("cost_per_conversion"),
            ),
            "marketing_db.fact_gg_demographic_daily",
        )

        combined.unpersist()

    def _handle_ad_asset(self, df):
        parsed = self._parse(df, "ad_asset", self.ad_asset_schema)
        if parsed is None:
            return
        parsed.persist()

        valid = self._f(parsed, "ad_id", "asset_id")

        self.write_to_clickhouse(
            valid.select(
                col("asset_id"),
                coalesce(col("ad_id"), lit("")).alias("ad_id"),
                coalesce(col("asset_name"), lit("")).alias("asset_name"),
                coalesce(col("asset_type"), lit("")).alias("asset_type"),
                coalesce(col("asset_text"), lit("")).alias("asset_text"),
                coalesce(col("image_url"), lit("")).alias("image_url"),
            ).dropDuplicates(["asset_id"]),
            "marketing_db.dim_gg_asset",
        )

        flat = valid.select(
            col("ad_id"), col("asset_id"), to_date(col("date"), "yyyy-MM-dd").alias("date"),
            col("campaign_id"), coalesce(col("campaign_name"), lit("")).alias("campaign_name"),
            col("adgroup_id"), coalesce(col("adgroup_name"), lit("")).alias("adgroup_name"),
            coalesce(col("asset_name"), lit("")).alias("asset_name"),
            coalesce(col("asset_type"), lit("")).alias("asset_type"),
            coalesce(col("asset_text"), lit("")).alias("asset_text"),
            coalesce(col("image_url"), lit("")).alias("image_url"),
            coalesce(col("asset_performance"), lit("")).alias("asset_performance"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            col("account_id"), coalesce(col("account_name"), lit("")).alias("account_name"),
        )
        self.write_to_clickhouse(flat, "marketing_db.gad_ad_asset_daily_report")

        fact = valid.select(
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            col("account_id"), col("campaign_id"), col("adgroup_id"),
            col("ad_id"), col("asset_id"),
            coalesce(col("asset_performance"), lit("")).alias("asset_performance"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
        )
        self.write_to_clickhouse(fact, "marketing_db.fact_gg_asset_daily")

        parsed.unpersist()

    def _handle_click_type(self, df):
        parsed = self._parse(df, "click_type", self.click_type_schema)
        if parsed is None:
            return

        valid = self._f(parsed, "campaign_id", "click_type")

        flat = valid.select(
            col("campaign_id"), to_date(col("date"), "yyyy-MM-dd").alias("date"),
            col("click_type"), coalesce(col("campaign_name"), lit("")).alias("campaign_name"),
            coalesce(col("campaign_status"), lit("")).alias("campaign_status"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
            coalesce(col("conversions"), lit(0)).alias("conversions"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("device"), lit("UNKNOWN")).alias("device"),
            coalesce(col("ad_network_type"), lit("")).alias("ad_network_type"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            col("account_id"), coalesce(col("account_name"), lit("")).alias("account_name"),
        )
        self.write_to_clickhouse(flat, "marketing_db.gad_click_type_report")

        fact = valid.select(
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            col("account_id"), col("campaign_id"),
            col("click_type"), coalesce(col("device"), lit("UNKNOWN")).alias("device"),
            coalesce(col("ad_network_type"), lit("")).alias("ad_network_type"),
            coalesce(col("impressions"), lit(0)).alias("impressions"),
            coalesce(col("clicks"), lit(0)).alias("clicks"),
            coalesce(col("cost").cast("float"), lit(0.0)).alias("cost"),
            coalesce(col("conversions"), lit(0)).alias("conversions"),
            coalesce(col("all_conversions"), lit(0)).alias("all_conversions"),
            coalesce(col("ctr").cast("float"), lit(0.0)).alias("ctr"),
        )
        self.write_to_clickhouse(fact, "marketing_db.fact_gg_click_type_daily")

-- =============================================================================
-- Real-Time Layer: Kafka Engine → MergeTree → Materialized View
-- Source : processed_* Kafka topics (output of spark_consumer/speed_layer.py)
-- Pattern: 3 objects per topic
--   1. kafka_rt_<name>  — Kafka Engine table (reads live from topic, no storage)
--   2. rt_<name>        — ReplacingMergeTree (actual storage, TTL = 1 day)
--   3. mv_rt_<name>     — Materialized View (pipes Kafka → storage automatically)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. processed_dim_account
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_account
(
    account_id   String,
    account_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_account',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_account
(
    account_id   String,
    account_name String,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_account
TO marketing_db.rt_dim_account
AS SELECT * FROM marketing_db.kafka_rt_dim_account;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. processed_dim_campaign
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_campaign
(
    campaign_id   String,
    account_id    String,
    campaign_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_campaign',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_campaign
(
    campaign_id   String,
    account_id    String,
    campaign_name String,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_campaign
TO marketing_db.rt_dim_campaign
AS SELECT * FROM marketing_db.kafka_rt_dim_campaign;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. processed_dim_adset
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_adset
(
    adset_id    String,
    campaign_id String,
    adset_name  String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_adset',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_adset
(
    adset_id    String,
    campaign_id String,
    adset_name  String,
    updated_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY adset_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_adset
TO marketing_db.rt_dim_adset
AS SELECT * FROM marketing_db.kafka_rt_dim_adset;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. processed_dim_ad
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_ad
(
    ad_id            String,
    adset_id         String,
    ad_name          String,
    status           String,
    effective_status String,
    created_time     Nullable(DateTime)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_ad',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_ad
(
    ad_id            String,
    adset_id         String,
    ad_name          String,
    status           String,
    effective_status String,
    created_time     Nullable(DateTime),
    updated_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY ad_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_ad
TO marketing_db.rt_dim_ad
AS SELECT * FROM marketing_db.kafka_rt_dim_ad;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. processed_dim_creative
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_creative
(
    creative_id                String,
    creative_title             String,
    creative_body              String,
    creative_thumbnail_raw_url String,
    creative_link              String,
    page_name                  String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_creative',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_creative
(
    creative_id                String,
    creative_title             String,
    creative_body              String,
    creative_thumbnail_raw_url String,
    creative_link              String,
    page_name                  String,
    updated_at                 DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY creative_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_creative
TO marketing_db.rt_dim_creative
AS SELECT * FROM marketing_db.kafka_rt_dim_creative;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. processed_dim_date
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_date
(
    date         Date,
    year         Int16,
    quarter      Int8,
    month        Int8,
    month_name   String,
    week         Int8,
    day_of_year  Int16,
    day_of_month Int8,
    day_of_week  Int8,
    day_name     String,
    is_weekend   UInt8,
    is_holiday   UInt8,
    holiday_name Nullable(String)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_date',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_date
(
    date         Date,
    year         Int16,
    quarter      Int8,
    month        Int8,
    month_name   String,
    week         Int8,
    day_of_year  Int16,
    day_of_month Int8,
    day_of_week  Int8,
    day_name     String,
    is_weekend   UInt8,
    is_holiday   UInt8,
    holiday_name Nullable(String),
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY date
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_date
TO marketing_db.rt_dim_date
AS SELECT * FROM marketing_db.kafka_rt_dim_date;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. processed_fact_fb_ad_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_fb_ad_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    ctr                       Float32,
    cpc                       Float32,
    cpm                       Float32,
    frequency                 Float32,
    new_messaging_connections Int32,
    cost_per_new_messaging    Float32,
    link_clicks               Int32,
    landing_page_views        Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_fb_ad_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_fb_ad_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    ctr                       Float32 DEFAULT 0,
    cpc                       Float32 DEFAULT 0,
    cpm                       Float32 DEFAULT 0,
    frequency                 Float32 DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    cost_per_new_messaging    Float32 DEFAULT 0,
    link_clicks               Int32   DEFAULT 0,
    landing_page_views        Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, ad_id, date_start)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_fb_ad_daily
TO marketing_db.rt_fact_fb_ad_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_fb_ad_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. processed_fact_fb_ad_creative_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_fb_ad_creative_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    creative_id               String,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    new_messaging_connections Int32,
    post_engagements          Int32,
    post_reactions            Int32,
    post_shares               Int32,
    photo_views               Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_fb_ad_creative_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_fb_ad_creative_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    creative_id               String,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    post_engagements          Int32   DEFAULT 0,
    post_reactions            Int32   DEFAULT 0,
    post_shares               Int32   DEFAULT 0,
    photo_views               Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, ad_id, creative_id, date_start)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_fb_ad_creative_daily
TO marketing_db.rt_fact_fb_ad_creative_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_fb_ad_creative_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. processed_fad_ad_daily_report  (flat/denormalized real-time copy)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fad_ad_daily_report
(
    id                        String,
    name                      String,
    adset_id                  String,
    adset_name                String,
    campaign_id               String,
    campaign_name             String,
    account_id                String,
    account_name              String,
    date_start                Date,
    date_stop                 Date,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    messaging_first_reply     Int32,
    new_messaging_connections Int32,
    post_comments             Int32,
    link_clicks               Int32,
    landing_page_views        Int32,
    page_likes                Int32,
    thru_play                 Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fad_ad_daily_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fad_ad_daily_report
(
    id                        String,
    name                      String,
    adset_id                  String,
    adset_name                String,
    campaign_id               String,
    campaign_name             String,
    account_id                String,
    account_name              String,
    date_start                Date,
    date_stop                 Date,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    messaging_first_reply     Int32   DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    post_comments             Int32   DEFAULT 0,
    link_clicks               Int32   DEFAULT 0,
    landing_page_views        Int32   DEFAULT 0,
    page_likes                Int32   DEFAULT 0,
    thru_play                 Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, date_start, id)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fad_ad_daily_report
TO marketing_db.rt_fad_ad_daily_report
AS SELECT * FROM marketing_db.kafka_rt_fad_ad_daily_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 10. processed_fact_fb_ad_demographic_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_fb_ad_demographic_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    age                       String,
    gender                    String,
    spend                     Float32,
    impressions               Int32,
    reach                     Int32,
    clicks                    Int32,
    inline_link_clicks        Int32,
    new_messaging_connections Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_fb_ad_demographic_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_fb_ad_demographic_daily
(
    date_start                Date,
    account_id                String,
    ad_id                     String,
    age                       String,
    gender                    String,
    spend                     Float32 DEFAULT 0,
    impressions               Int32   DEFAULT 0,
    reach                     Int32   DEFAULT 0,
    clicks                    Int32   DEFAULT 0,
    inline_link_clicks        Int32   DEFAULT 0,
    new_messaging_connections Int32   DEFAULT 0,
    updated_at                DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, ad_id, age, gender, date_start)
TTL date_start + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_fb_ad_demographic_daily
TO marketing_db.rt_fact_fb_ad_demographic_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_fb_ad_demographic_daily;


-- =============================================================================
-- Google Real-Time Layer
-- Source : processed_gg_* / processed_gad_* topics (output of speed_layer.py)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 11. processed_gad_campaign_daily_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_campaign_daily_report
(
    campaign_id    String,
    campaign_name  String,
    date           Date,
    impressions    Int32,
    clicks         Int32,
    cost           Float32,
    all_conversions Int32,
    ctr            Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_campaign_daily_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_campaign_daily_report
(
    campaign_id     String,
    campaign_name   String,
    date            Date,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_campaign_daily_report
TO marketing_db.rt_gad_campaign_daily_report
AS SELECT * FROM marketing_db.kafka_rt_gad_campaign_daily_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 12. processed_gad_ad_group_daily_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_ad_group_daily_report
(
    adgroup_id      String,
    adgroup_name    String,
    date            Date,
    impressions     Int32,
    clicks          Int32,
    cost            Float32,
    all_conversions Int32,
    ctr             Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_ad_group_daily_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_ad_group_daily_report
(
    adgroup_id      String,
    adgroup_name    String,
    date            Date,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (adgroup_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_ad_group_daily_report
TO marketing_db.rt_gad_ad_group_daily_report
AS SELECT * FROM marketing_db.kafka_rt_gad_ad_group_daily_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 13. processed_gad_account_daily_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_account_daily_report
(
    account_id      String,
    account_name    String,
    date            Date,
    impressions     Int32,
    clicks          Int32,
    cost            Float32,
    all_conversions Int32,
    ctr             Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_account_daily_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_account_daily_report
(
    account_id      String,
    account_name    String,
    date            Date,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_account_daily_report
TO marketing_db.rt_gad_account_daily_report
AS SELECT * FROM marketing_db.kafka_rt_gad_account_daily_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 14. processed_gad_keyword_performance_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_keyword_performance_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       String,
    adgroup_name        String,
    account_id          String,
    account_name        String,
    device              String,
    keyword             String,
    quality_score       Int32,
    impressions         Int32,
    clicks              Int32,
    ctr                 Float32,
    conversions         Int32,
    all_conversions     Int32,
    average_cpc         Float32,
    cost_per_conversion Float32,
    cost                Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_keyword_performance_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_keyword_performance_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       String,
    adgroup_name        String,
    account_id          String,
    account_name        String,
    device              String,
    keyword             String,
    quality_score       Int32   DEFAULT 0,
    impressions         Int32   DEFAULT 0,
    clicks              Int32   DEFAULT 0,
    ctr                 Float32 DEFAULT 0,
    conversions         Int32   DEFAULT 0,
    all_conversions     Int32   DEFAULT 0,
    average_cpc         Float32 DEFAULT 0,
    cost_per_conversion Float32 DEFAULT 0,
    cost                Float32 DEFAULT 0,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (adgroup_id, keyword, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_keyword_performance_report
TO marketing_db.rt_gad_keyword_performance_report
AS SELECT * FROM marketing_db.kafka_rt_gad_keyword_performance_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 15. processed_gad_age_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_age_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       String,
    adgroup_name        String,
    account_id          String,
    account_name        String,
    device              String,
    age_range           String,
    impressions         Int32,
    clicks              Int32,
    ctr                 Float32,
    conversions         Int32,
    all_conversions     Int32,
    average_cpc         Float32,
    cost_per_conversion Float32,
    cost                Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_age_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_age_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       String,
    adgroup_name        String,
    account_id          String,
    account_name        String,
    device              String,
    age_range           String,
    impressions         Int32   DEFAULT 0,
    clicks              Int32   DEFAULT 0,
    ctr                 Float32 DEFAULT 0,
    conversions         Int32   DEFAULT 0,
    all_conversions     Int32   DEFAULT 0,
    average_cpc         Float32 DEFAULT 0,
    cost_per_conversion Float32 DEFAULT 0,
    cost                Float32 DEFAULT 0,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (adgroup_id, age_range, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_age_report
TO marketing_db.rt_gad_age_report
AS SELECT * FROM marketing_db.kafka_rt_gad_age_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 15b. processed_gad_gender_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_gender_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       String,
    adgroup_name        String,
    account_id          String,
    account_name        String,
    device              String,
    gender              String,
    impressions         Int32,
    clicks              Int32,
    ctr                 Float32,
    conversions         Int32,
    all_conversions     Int32,
    average_cpc         Float32,
    cost_per_conversion Float32,
    cost                Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_gender_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_gender_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       String,
    adgroup_name        String,
    account_id          String,
    account_name        String,
    device              String,
    gender              String,
    impressions         Int32   DEFAULT 0,
    clicks              Int32   DEFAULT 0,
    ctr                 Float32 DEFAULT 0,
    conversions         Int32   DEFAULT 0,
    all_conversions     Int32   DEFAULT 0,
    average_cpc         Float32 DEFAULT 0,
    cost_per_conversion Float32 DEFAULT 0,
    cost                Float32 DEFAULT 0,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (adgroup_id, gender, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_gender_report
TO marketing_db.rt_gad_gender_report
AS SELECT * FROM marketing_db.kafka_rt_gad_gender_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 16. processed_gad_ad_asset_daily_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_ad_asset_daily_report
(
    ad_id            String,
    asset_id         String,
    date             Date,
    campaign_id      String,
    campaign_name    String,
    adgroup_id       String,
    adgroup_name     String,
    asset_name       String,
    asset_type       String,
    asset_text       String,
    image_url        String,
    asset_performance String,
    impressions      Int32,
    clicks           Int32,
    ctr              Float32,
    all_conversions  Int32,
    cost             Float32,
    account_id       String,
    account_name     String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_ad_asset_daily_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_ad_asset_daily_report
(
    ad_id             String,
    asset_id          String,
    date              Date,
    campaign_id       String,
    campaign_name     String,
    adgroup_id        String,
    adgroup_name      String,
    asset_name        String,
    asset_type        String,
    asset_text        String,
    image_url         String,
    asset_performance String,
    impressions       Int32   DEFAULT 0,
    clicks            Int32   DEFAULT 0,
    ctr               Float32 DEFAULT 0,
    all_conversions   Int32   DEFAULT 0,
    cost              Float32 DEFAULT 0,
    account_id        String,
    account_name      String,
    updated_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (ad_id, asset_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_ad_asset_daily_report
TO marketing_db.rt_gad_ad_asset_daily_report
AS SELECT * FROM marketing_db.kafka_rt_gad_ad_asset_daily_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 17. processed_gad_click_type_report
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_gad_click_type_report
(
    campaign_id     String,
    date            Date,
    click_type      String,
    campaign_name   String,
    campaign_status String,
    impressions     Int32,
    clicks          Int32,
    ctr             Float32,
    conversions     Int32,
    all_conversions Int32,
    device          String,
    ad_network_type String,
    cost            Float32,
    account_id      String,
    account_name    String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_gad_click_type_report',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_gad_click_type_report
(
    campaign_id     String,
    date            Date,
    click_type      String,
    campaign_name   String,
    campaign_status String,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    conversions     Int32   DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    device          String,
    ad_network_type String,
    cost            Float32 DEFAULT 0,
    account_id      String,
    account_name    String,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id, click_type, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_gad_click_type_report
TO marketing_db.rt_gad_click_type_report
AS SELECT * FROM marketing_db.kafka_rt_gad_click_type_report;


-- ─────────────────────────────────────────────────────────────────────────────
-- 17b. processed_dim_gg_campaign
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_gg_campaign
(
    campaign_id   String,
    campaign_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_gg_campaign',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_gg_campaign
(
    campaign_id   String,
    campaign_name String,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_gg_campaign
TO marketing_db.rt_dim_gg_campaign
AS SELECT * FROM marketing_db.kafka_rt_dim_gg_campaign;


-- ─────────────────────────────────────────────────────────────────────────────
-- 18. processed_dim_gg_adgroup
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_gg_adgroup
(
    adgroup_id   String,
    campaign_id  String,
    adgroup_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_gg_adgroup',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_gg_adgroup
(
    adgroup_id   String,
    campaign_id  String,
    adgroup_name String,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY adgroup_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_gg_adgroup
TO marketing_db.rt_dim_gg_adgroup
AS SELECT * FROM marketing_db.kafka_rt_dim_gg_adgroup;


-- ─────────────────────────────────────────────────────────────────────────────
-- 19. processed_dim_gg_asset
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_gg_asset
(
    asset_id   String,
    ad_id      String,
    asset_name String,
    asset_type String,
    asset_text String,
    image_url  String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_gg_asset',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_gg_asset
(
    asset_id   String,
    ad_id      String,
    asset_name String,
    asset_type String,
    asset_text String,
    image_url  String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY asset_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_gg_asset
TO marketing_db.rt_dim_gg_asset
AS SELECT * FROM marketing_db.kafka_rt_dim_gg_asset;


-- ─────────────────────────────────────────────────────────────────────────────
-- 20. processed_fact_gg_campaign_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_campaign_daily
(
    campaign_id     String,
    date            Date,
    impressions     Int32,
    clicks          Int32,
    cost            Float32,
    all_conversions Int32,
    ctr             Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_campaign_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_campaign_daily
(
    campaign_id     String,
    date            Date,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_campaign_daily
TO marketing_db.rt_fact_gg_campaign_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_campaign_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 21. processed_fact_gg_adgroup_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_adgroup_daily
(
    adgroup_id      String,
    date            Date,
    impressions     Int32,
    clicks          Int32,
    cost            Float32,
    all_conversions Int32,
    ctr             Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_adgroup_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_adgroup_daily
(
    adgroup_id      String,
    date            Date,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (adgroup_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_adgroup_daily
TO marketing_db.rt_fact_gg_adgroup_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_adgroup_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 22. processed_fact_gg_keyword_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_keyword_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    keyword             String,
    device              String,
    quality_score       Int32,
    impressions         Int32,
    clicks              Int32,
    cost                Float32,
    conversions         Int32,
    all_conversions     Int32,
    ctr                 Float32,
    average_cpc         Float32,
    cost_per_conversion Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_keyword_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_keyword_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    keyword             String,
    device              String,
    quality_score       Int32   DEFAULT 0,
    impressions         Int32   DEFAULT 0,
    clicks              Int32   DEFAULT 0,
    cost                Float32 DEFAULT 0,
    conversions         Int32   DEFAULT 0,
    all_conversions     Int32   DEFAULT 0,
    ctr                 Float32 DEFAULT 0,
    average_cpc         Float32 DEFAULT 0,
    cost_per_conversion Float32 DEFAULT 0,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, campaign_id, adgroup_id, keyword, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_keyword_daily
TO marketing_db.rt_fact_gg_keyword_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_keyword_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 23. processed_fact_gg_age_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_age_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    age_range           String,
    device              String,
    impressions         Int32,
    clicks              Int32,
    cost                Float32,
    conversions         Int32,
    all_conversions     Int32,
    ctr                 Float32,
    average_cpc         Float32,
    cost_per_conversion Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_age_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_age_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    age_range           String,
    device              String,
    impressions         Int32   DEFAULT 0,
    clicks              Int32   DEFAULT 0,
    cost                Float32 DEFAULT 0,
    conversions         Int32   DEFAULT 0,
    all_conversions     Int32   DEFAULT 0,
    ctr                 Float32 DEFAULT 0,
    average_cpc         Float32 DEFAULT 0,
    cost_per_conversion Float32 DEFAULT 0,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, adgroup_id, age_range, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_age_daily
TO marketing_db.rt_fact_gg_age_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_age_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 23b. processed_fact_gg_gender_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_gender_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    gender              String,
    device              String,
    impressions         Int32,
    clicks              Int32,
    cost                Float32,
    conversions         Int32,
    all_conversions     Int32,
    ctr                 Float32,
    average_cpc         Float32,
    cost_per_conversion Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_gender_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_gender_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    gender              String,
    device              String,
    impressions         Int32   DEFAULT 0,
    clicks              Int32   DEFAULT 0,
    cost                Float32 DEFAULT 0,
    conversions         Int32   DEFAULT 0,
    all_conversions     Int32   DEFAULT 0,
    ctr                 Float32 DEFAULT 0,
    average_cpc         Float32 DEFAULT 0,
    cost_per_conversion Float32 DEFAULT 0,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, adgroup_id, gender, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_gender_daily
TO marketing_db.rt_fact_gg_gender_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_gender_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 24. processed_fact_gg_asset_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_asset_daily
(
    date              Date,
    account_id        String,
    campaign_id       String,
    adgroup_id        String,
    ad_id             String,
    asset_id          String,
    asset_performance String,
    impressions       Int32,
    clicks            Int32,
    cost              Float32,
    all_conversions   Int32,
    ctr               Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_asset_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_asset_daily
(
    date              Date,
    account_id        String,
    campaign_id       String,
    adgroup_id        String,
    ad_id             String,
    asset_id          String,
    asset_performance String,
    impressions       Int32   DEFAULT 0,
    clicks            Int32   DEFAULT 0,
    cost              Float32 DEFAULT 0,
    all_conversions   Int32   DEFAULT 0,
    ctr               Float32 DEFAULT 0,
    updated_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, campaign_id, adgroup_id, ad_id, asset_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_asset_daily
TO marketing_db.rt_fact_gg_asset_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_asset_daily;


-- ─────────────────────────────────────────────────────────────────────────────
-- 25. processed_fact_gg_click_type_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_gg_click_type_daily
(
    date            Date,
    account_id      String,
    campaign_id     String,
    click_type      String,
    device          String,
    ad_network_type String,
    impressions     Int32,
    clicks          Int32,
    cost            Float32,
    conversions     Int32,
    all_conversions Int32,
    ctr             Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_gg_click_type_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_gg_click_type_daily
(
    date            Date,
    account_id      String,
    campaign_id     String,
    click_type      String,
    device          String,
    ad_network_type String,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    conversions     Int32   DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, campaign_id, click_type, device, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_gg_click_type_daily
TO marketing_db.rt_fact_gg_click_type_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_gg_click_type_daily;


-- =============================================================================
-- TikTok Real-Time Layer
-- Source : processed_tta_* topics (output of speed_layer.py)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 26. processed_tta_ad_performance  (flat/denormalized real-time copy)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_tta_ad_performance
(
    pkId                         String,
    user_id                      String,
    stat_time_day                Date,
    ad_id                        String,
    ad_name                      String,
    ad_text                      String,
    adgroup_name                 String,
    campaign_name                String,
    advertiser_id                String,
    advertiser_name              String,
    start_date                   Date,
    end_date                     Date,
    spend                        Float32,
    impressions                  Int32,
    clicks                       Int32,
    ctr                          Float32,
    cpc                          Float32,
    cpm                          Float32,
    reach                        Int32,
    frequency                    Float32,
    conversion                   Int32,
    cost_per_conversion          Float32,
    conversion_rate              Float32,
    video_play_actions           Int32,
    profile_visits               Int32,
    likes                        Int32,
    comments                     Int32,
    shares                       Int32,
    follows                      Int32,
    live_views                   Int32,
    purchase                     Int32,
    onsite_shopping              Int32,
    total_onsite_shopping_value  Float32,
    onsite_shopping_roas         Float32,
    cost_per_onsite_shopping     Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_tta_ad_performance',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_tta_ad_performance
(
    pkId                         String,
    user_id                      String,
    stat_time_day                Date,
    ad_id                        String,
    ad_name                      String,
    ad_text                      String,
    adgroup_name                 String,
    campaign_name                String,
    advertiser_id                String,
    advertiser_name              String,
    start_date                   Date,
    end_date                     Date,
    spend                        Float32 DEFAULT 0,
    impressions                  Int32   DEFAULT 0,
    clicks                       Int32   DEFAULT 0,
    ctr                          Float32 DEFAULT 0,
    cpc                          Float32 DEFAULT 0,
    cpm                          Float32 DEFAULT 0,
    reach                        Int32   DEFAULT 0,
    frequency                    Float32 DEFAULT 0,
    conversion                   Int32   DEFAULT 0,
    cost_per_conversion          Float32 DEFAULT 0,
    conversion_rate              Float32 DEFAULT 0,
    video_play_actions           Int32   DEFAULT 0,
    profile_visits               Int32   DEFAULT 0,
    likes                        Int32   DEFAULT 0,
    comments                     Int32   DEFAULT 0,
    shares                       Int32   DEFAULT 0,
    follows                      Int32   DEFAULT 0,
    live_views                   Int32   DEFAULT 0,
    purchase                     Int32   DEFAULT 0,
    onsite_shopping              Int32   DEFAULT 0,
    total_onsite_shopping_value  Float32 DEFAULT 0,
    onsite_shopping_roas         Float32 DEFAULT 0,
    cost_per_onsite_shopping     Float32 DEFAULT 0,
    updated_at                   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (advertiser_id, ad_id, stat_time_day)
TTL stat_time_day + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_tta_ad_performance
TO marketing_db.rt_tta_ad_performance
AS SELECT * FROM marketing_db.kafka_rt_tta_ad_performance;


-- ─────────────────────────────────────────────────────────────────────────────
-- 27. processed_dim_tta_advertiser
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_tta_advertiser
(
    advertiser_id   String,
    advertiser_name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_tta_advertiser',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_tta_advertiser
(
    advertiser_id   String,
    advertiser_name String,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY advertiser_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_tta_advertiser
TO marketing_db.rt_dim_tta_advertiser
AS SELECT * FROM marketing_db.kafka_rt_dim_tta_advertiser;


-- ─────────────────────────────────────────────────────────────────────────────
-- 28. processed_dim_tta_ad
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_dim_tta_ad
(
    ad_id         String,
    advertiser_id String,
    campaign_name String,
    adgroup_name  String,
    ad_name       String,
    ad_text       String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_dim_tta_ad',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_dim_tta_ad
(
    ad_id         String,
    advertiser_id String,
    campaign_name String,
    adgroup_name  String,
    ad_name       String,
    ad_text       String,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY ad_id
TTL toDate(updated_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_dim_tta_ad
TO marketing_db.rt_dim_tta_ad
AS SELECT * FROM marketing_db.kafka_rt_dim_tta_ad;


-- ─────────────────────────────────────────────────────────────────────────────
-- 29. processed_fact_tta_ad_daily
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.kafka_rt_fact_tta_ad_daily
(
    date                         Date,
    advertiser_id                String,
    ad_id                        String,
    spend                        Float32,
    impressions                  Int32,
    clicks                       Int32,
    ctr                          Float32,
    cpc                          Float32,
    cpm                          Float32,
    reach                        Int32,
    frequency                    Float32,
    conversion                   Int32,
    cost_per_conversion          Float32,
    conversion_rate              Float32,
    video_play_actions           Int32,
    profile_visits               Int32,
    likes                        Int32,
    comments                     Int32,
    shares                       Int32,
    follows                      Int32,
    live_views                   Int32,
    purchase                     Int32,
    onsite_shopping              Int32,
    total_onsite_shopping_value  Float32,
    onsite_shopping_roas         Float32,
    cost_per_onsite_shopping     Float32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'processed_fact_tta_ad_daily',
    kafka_group_name  = 'ch_rt_consumer',
    kafka_format      = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS marketing_db.rt_fact_tta_ad_daily
(
    date                         Date,
    advertiser_id                String,
    ad_id                        String,
    spend                        Float32 DEFAULT 0,
    impressions                  Int32   DEFAULT 0,
    clicks                       Int32   DEFAULT 0,
    ctr                          Float32 DEFAULT 0,
    cpc                          Float32 DEFAULT 0,
    cpm                          Float32 DEFAULT 0,
    reach                        Int32   DEFAULT 0,
    frequency                    Float32 DEFAULT 0,
    conversion                   Int32   DEFAULT 0,
    cost_per_conversion          Float32 DEFAULT 0,
    conversion_rate              Float32 DEFAULT 0,
    video_play_actions           Int32   DEFAULT 0,
    profile_visits               Int32   DEFAULT 0,
    likes                        Int32   DEFAULT 0,
    comments                     Int32   DEFAULT 0,
    shares                       Int32   DEFAULT 0,
    follows                      Int32   DEFAULT 0,
    live_views                   Int32   DEFAULT 0,
    purchase                     Int32   DEFAULT 0,
    onsite_shopping              Int32   DEFAULT 0,
    total_onsite_shopping_value  Float32 DEFAULT 0,
    onsite_shopping_roas         Float32 DEFAULT 0,
    cost_per_onsite_shopping     Float32 DEFAULT 0,
    updated_at                   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (advertiser_id, ad_id, date)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS marketing_db.mv_rt_fact_tta_ad_daily
TO marketing_db.rt_fact_tta_ad_daily
AS SELECT * FROM marketing_db.kafka_rt_fact_tta_ad_daily;

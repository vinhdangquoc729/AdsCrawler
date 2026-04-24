CREATE TABLE IF NOT EXISTS marketing_db.fad_ad_daily_report
(
    id              String,
    name            Nullable(String),
    adset_id        Nullable(String),
    adset_name      Nullable(String),
    campaign_id     Nullable(String),
    campaign_name   Nullable(String),
    account_id      Nullable(String),
    account_name    Nullable(String),
    date_start      Nullable(Date),
    date_stop       Nullable(Date),

    status           Nullable(String),
    effective_status Nullable(String),
    created_time     Nullable(DateTime),

    spend       Nullable(Float64),
    impressions Nullable(Int32),
    reach       Nullable(Int32),
    clicks      Nullable(Int32),
    ctr         Nullable(Float64),
    cpc         Nullable(Float64),
    cpm         Nullable(Float64),
    frequency   Nullable(Float64),

    messaging_first_reply            Nullable(Int32),
    cost_per_messaging_first_reply   Nullable(Float64),
    new_messaging_connections        Nullable(Int32),
    cost_per_new_messaging           Nullable(Float64),
    leads                            Nullable(Int32),
    cost_leads                       Nullable(Float64),
    purchases                        Nullable(Int32),
    cost_purchases                   Nullable(Float64),
    purchase_value                   Nullable(Float64),
    purchase_roas                    Nullable(Float64),
    website_purchases                Nullable(Int32),
    on_facebook_purchases            Nullable(Int32),
    completed_registration           Nullable(Int32),
    cost_per_completed_registration  Nullable(Float64),
    thru_play                        Nullable(Int32),
    cost_per_thru_play               Nullable(Float64),

    post_comments                    Nullable(Int32),
    link_clicks                      Nullable(Int32),
    cost_per_unique_link_click       Nullable(Float64),
    leads_conversion_value           Nullable(Float64),
    landing_page_views               Nullable(Int32),
    cost_per_landing_page_view       Nullable(Float64),
    adds_to_cart                     Nullable(Int32),
    cost_per_add_to_cart             Nullable(Float64),
    checkouts_initiated              Nullable(Int32),
    cost_per_checkout_initiated      Nullable(Float64),
    page_likes                       Nullable(Int32),
    cost_page_likes                  Nullable(Float64)
)
ENGINE = ReplacingMergeTree()
ORDER BY (account_id, date_start, date_stop, campaign_id, adset_id, id)
SETTINGS allow_nullable_key = 1;


CREATE TABLE IF NOT EXISTS marketing_db.dim_account (
    account_id String,
    account_name String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS marketing_db.dim_campaign (
    campaign_id String,
    account_id String,
    campaign_name String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id;

CREATE TABLE IF NOT EXISTS marketing_db.dim_adset (
    adset_id String,
    campaign_id String,
    adset_name String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY adset_id;

CREATE TABLE IF NOT EXISTS marketing_db.dim_ad (
    ad_id String,
    adset_id String,
    ad_name String,
    status String,
    effective_status String,
    created_time DateTime,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY ad_id;

CREATE TABLE IF NOT EXISTS marketing_db.dim_creative (
    creative_id String,
    creative_title String,
    creative_body String,
    creative_thumbnail_raw_url String,
    creative_link String,
    page_name String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY creative_id;

CREATE TABLE IF NOT EXISTS marketing_db.fact_fb_ad_daily (
    date_start Date,
    account_id String,
    ad_id String,
    
    spend Float32 DEFAULT 0,
    impressions Int32 DEFAULT 0,
    reach Int32 DEFAULT 0,
    clicks Int32 DEFAULT 0,
    ctr Float32 DEFAULT 0,
    cpc Float32 DEFAULT 0,
    cpm Float32 DEFAULT 0,
    frequency Float32 DEFAULT 0,
    
    new_messaging_connections Int32 DEFAULT 0,
    cost_per_new_messaging Float32 DEFAULT 0,
    link_clicks Int32 DEFAULT 0,
    landing_page_views Int32 DEFAULT 0,
    
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date_start) 
ORDER BY (account_id, ad_id, date_start);

CREATE TABLE IF NOT EXISTS marketing_db.fact_fb_ad_creative_daily (
    date_start Date,
    account_id String,
    ad_id String,
    creative_id String,
    
    spend Float32 DEFAULT 0,
    impressions Int32 DEFAULT 0,
    reach Int32 DEFAULT 0,
    clicks Int32 DEFAULT 0,
    new_messaging_connections Int32 DEFAULT 0,
    post_engagements Int32 DEFAULT 0,
    post_reactions Int32 DEFAULT 0,
    post_shares Int32 DEFAULT 0,
    photo_views Int32 DEFAULT 0,
    
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date_start)
ORDER BY (account_id, ad_id, creative_id, date_start);

CREATE TABLE IF NOT EXISTS marketing_db.fact_fb_ad_demographic_daily (
    date_start Date,
    account_id String,
    ad_id String,
    age String,
    gender String,
    
    spend Float32 DEFAULT 0,
    impressions Int32 DEFAULT 0,
    reach Int32 DEFAULT 0,
    clicks Int32 DEFAULT 0,
    inline_link_clicks Int32 DEFAULT 0,
    new_messaging_connections Int32 DEFAULT 0,
    
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date_start)
ORDER BY (account_id, ad_id, age, gender, date_start);

CREATE TABLE IF NOT EXISTS marketing_db.dim_date (
    date Date,
    year Int16,
    quarter Int8,
    month Int8,
    month_name String,
    week Int8,
    day_of_year Int16,
    day_of_month Int8,
    day_of_week Int8,   -- 1=Mon ... 7=Sun
    day_name String,
    is_weekend  UInt8,  -- 1 or 0
    is_holiday  UInt8,  -- public holidays
    holiday_name Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY date;


-- ─────────────────────────────────────────────────────────────────────────────
-- Google Ads: flat denormalized reports  (gad_ prefix, mirrors fad_ for FB)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.gad_campaign_daily_report
(
    campaign_id   String,
    campaign_name Nullable(String),
    date          Date,
    impressions   Int32   DEFAULT 0,
    clicks        Int32   DEFAULT 0,
    cost          Float32 DEFAULT 0,
    all_conversions Int32 DEFAULT 0,
    ctr           Float32 DEFAULT 0,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (campaign_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.gad_ad_group_daily_report
(
    adgroup_id   String,
    adgroup_name Nullable(String),
    date         Date,
    impressions  Int32   DEFAULT 0,
    clicks       Int32   DEFAULT 0,
    cost         Float32 DEFAULT 0,
    all_conversions Int32 DEFAULT 0,
    ctr          Float32 DEFAULT 0,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (adgroup_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.gad_account_daily_report
(
    account_id   String,
    account_name Nullable(String),
    date         Date,
    impressions  Int32   DEFAULT 0,
    clicks       Int32   DEFAULT 0,
    cost         Float32 DEFAULT 0,
    all_conversions Int32 DEFAULT 0,
    ctr          Float32 DEFAULT 0,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.gad_keyword_performance_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       Nullable(String),
    adgroup_name        Nullable(String),
    account_id          Nullable(String),
    account_name        Nullable(String),
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
PARTITION BY toYYYYMM(date)
ORDER BY (adgroup_id, keyword, device, date);

CREATE TABLE IF NOT EXISTS marketing_db.gad_demographic_report
(
    adgroup_id          String,
    date                Date,
    campaign_id         String,
    campaign_name       Nullable(String),
    adgroup_name        Nullable(String),
    account_id          Nullable(String),
    account_name        Nullable(String),
    device              String,
    age_range           String  DEFAULT '',
    gender              String  DEFAULT '',
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
PARTITION BY toYYYYMM(date)
ORDER BY (adgroup_id, age_range, gender, device, date);

CREATE TABLE IF NOT EXISTS marketing_db.gad_ad_asset_daily_report
(
    ad_id            String,
    asset_id         String,
    date             Date,
    campaign_id      Nullable(String),
    campaign_name    Nullable(String),
    adgroup_id       Nullable(String),
    adgroup_name     Nullable(String),
    asset_name       Nullable(String),
    asset_type       Nullable(String),
    asset_text       Nullable(String),
    image_url        Nullable(String),
    asset_performance Nullable(String),
    impressions      Int32   DEFAULT 0,
    clicks           Int32   DEFAULT 0,
    ctr              Float32 DEFAULT 0,
    all_conversions  Int32   DEFAULT 0,
    cost             Float32 DEFAULT 0,
    account_id       Nullable(String),
    account_name     Nullable(String),
    updated_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (ad_id, asset_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.gad_click_type_report
(
    campaign_id     String,
    date            Date,
    click_type      String,
    campaign_name   Nullable(String),
    campaign_status Nullable(String),
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    conversions     Int32   DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    device          String,
    ad_network_type Nullable(String),
    cost            Float32 DEFAULT 0,
    account_id      Nullable(String),
    account_name    Nullable(String),
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (campaign_id, click_type, device, date);


-- ─────────────────────────────────────────────────────────────────────────────
-- Google Ads: snowflake schema  (dim_gg_ / fact_gg_ prefix, mirrors FB dims)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS marketing_db.dim_gg_adgroup
(
    adgroup_id   String,
    campaign_id  String,
    adgroup_name String,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY adgroup_id;

CREATE TABLE IF NOT EXISTS marketing_db.dim_gg_asset
(
    asset_id   String,
    ad_id      String,
    asset_name String,
    asset_type String,
    asset_text String,
    image_url  String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY asset_id;

CREATE TABLE IF NOT EXISTS marketing_db.fact_gg_campaign_daily
(
    date            Date,
    campaign_id     String,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (campaign_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.fact_gg_adgroup_daily
(
    date            Date,
    adgroup_id      String,
    impressions     Int32   DEFAULT 0,
    clicks          Int32   DEFAULT 0,
    cost            Float32 DEFAULT 0,
    all_conversions Int32   DEFAULT 0,
    ctr             Float32 DEFAULT 0,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (adgroup_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.fact_gg_keyword_daily
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
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, adgroup_id, keyword, device, date);

CREATE TABLE IF NOT EXISTS marketing_db.fact_gg_demographic_daily
(
    date                Date,
    account_id          String,
    campaign_id         String,
    adgroup_id          String,
    age_range           String  DEFAULT '',
    gender              String  DEFAULT '',
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
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, adgroup_id, age_range, gender, device, date);

CREATE TABLE IF NOT EXISTS marketing_db.fact_gg_asset_daily
(
    date             Date,
    account_id       String,
    campaign_id      String,
    adgroup_id       String,
    ad_id            String,
    asset_id         String,
    asset_performance String,
    impressions      Int32   DEFAULT 0,
    clicks           Int32   DEFAULT 0,
    cost             Float32 DEFAULT 0,
    all_conversions  Int32   DEFAULT 0,
    ctr              Float32 DEFAULT 0,
    updated_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, adgroup_id, ad_id, asset_id, date);

CREATE TABLE IF NOT EXISTS marketing_db.fact_gg_click_type_daily
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
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, campaign_id, click_type, device, date);
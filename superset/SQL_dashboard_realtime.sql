-- 1. PieChart ~ Filter
-- Cấp dữ liệu: 1 dòng / campaign / day để Superset có thể rollup theo Time Grain (day/week/month/quarter/year).
SELECT
    f.date_start AS date_start,
    dd.year AS year,
    dd.quarter AS quarter,
    dd.month AS month,
    dd.month_name AS month_name,
    dd.week AS week,
    ds.campaign_id AS campaign_id,
    dc.campaign_name AS campaign_name,
    SUM(f.spend) AS total_spend
FROM marketing_db.rt_fact_fb_ad_daily FINAL f
JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
JOIN marketing_db.rt_dim_date FINAL dd ON f.date_start = dd.date
GROUP BY
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    ds.campaign_id,
    dc.campaign_name;


-- 2. BIỂU ĐỒ CHI TIẾT (Trendline, Big Number, Bảng biểu...)
-- CPM: Cost per mile (Tỉ lệ chuyển đổi từ impression -> Spend)
-- CTR: Click through rate (Tỉ lệ chuyển đổi từ impression -> click)
-- CPC: Cost per click (Tỉ lệ chuyển đổi từ click -> Spend)
SELECT 
    f.date_start,
    dc.campaign_name,
    round(SUM(f.spend), 2) AS total_spend,
    SUM(f.new_messaging_connections) AS total_messaging,
    SUM(f.clicks) AS total_clicks,
    SUM(f.impressions) AS total_impressions,
    if(
        SUM(f.new_messaging_connections) > 0,
        round(SUM(f.spend) / SUM(f.new_messaging_connections), 2),
        0.0
    ) AS cost_per_message,
    round((SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS ctr,
    round(SUM(f.spend) / nullIf(SUM(f.clicks), 0), 2) AS cpc,
    round((SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000, 2) AS cpm,
    round((SUM(f.new_messaging_connections) / nullIf(SUM(f.clicks), 0)) * 100, 2) AS message_rate
FROM marketing_db.rt_fact_fb_ad_daily FINAL f
JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
GROUP BY f.date_start, dc.campaign_name
ORDER BY f.date_start ASC;

-- 2.0. Diminishing Returns theo Campaign (tham chiếu mục 3.1 trong docs/mock_data_logic.md)
-- Cấp dữ liệu: 1 dòng / campaign / day để vẽ scatter/bubble chart với X = total_spend, Y = cpm, Series = campaign_name.
-- Giữ thêm total_impressions để có thể tính lại CPM đúng khi Superset aggregate theo filter thời gian.
SELECT
    f.date_start AS date_start,
    dd.year AS year,
    dd.quarter AS quarter,
    dd.month AS month,
    dd.month_name AS month_name,
    dd.week AS week,
    ds.campaign_id AS campaign_id,
    dc.campaign_name AS campaign_name,
    round(SUM(f.spend), 2) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    (SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000 AS cpm
FROM marketing_db.rt_fact_fb_ad_daily FINAL f
JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
JOIN marketing_db.rt_dim_date FINAL dd ON f.date_start = dd.date
GROUP BY
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    ds.campaign_id,
    dc.campaign_name
ORDER BY date_start ASC, total_spend DESC, campaign_name ASC;

-- 2.1. Phân tích seasonality theo Weekend / Holiday (tham chiếu mục 3.2 trong docs/mock_data_logic.md)
-- Dùng dataset này để so sánh CPM, CPC, CTR, message rate giữa ngày thường, cuối tuần và dịp lễ.
SELECT
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    dd.is_holiday,
    concat(dd.year, '-', printf('%02d', dd.month)) AS month_label,
    if(dd.is_weekend = 1, 'Weekend', 'Weekday') AS weekend_bucket,
    if(dd.is_holiday = 1, ifNull(dd.holiday_name, 'Holiday'), 'Non-Holiday') AS holiday_bucket,
    ifNull(dd.holiday_name, 'Non-Holiday') AS holiday_name,
    multiIf(
        dd.is_holiday = 1, concat('Holiday - ', ifNull(dd.holiday_name, 'Unknown')),
        dd.is_weekend = 1, 'Weekend',
        'Weekday'
    ) AS seasonality_bucket,
    f.account_id,
    acc.account_name,
    ds.campaign_id,
    dc.campaign_name,
    round(SUM(f.spend), 2) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.reach) AS total_reach,
    SUM(f.clicks) AS total_clicks,
    SUM(f.link_clicks) AS total_link_clicks,
    SUM(f.landing_page_views) AS total_landing_page_views,
    SUM(f.new_messaging_connections) AS total_messaging,
    if(
        SUM(f.new_messaging_connections) > 0,
        round(SUM(f.spend) / SUM(f.new_messaging_connections), 2),
        0.0
    ) AS cost_per_message,
    round((SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS ctr,
    round(SUM(f.spend) / nullIf(SUM(f.clicks), 0), 2) AS cpc,
    round((SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000, 2) AS cpm,
    round((SUM(f.new_messaging_connections) / nullIf(SUM(f.clicks), 0)) * 100, 2) AS message_rate
FROM marketing_db.rt_fact_fb_ad_daily FINAL f
JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
JOIN marketing_db.rt_dim_date FINAL dd ON f.date_start = dd.date
GROUP BY
    f.date_start,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    dd.is_holiday,
    weekend_bucket,
    holiday_bucket,
    holiday_name,
    seasonality_bucket,
    f.account_id,
    acc.account_name,
    ds.campaign_id,
    dc.campaign_name
ORDER BY f.date_start ASC, seasonality_bucket ASC, dc.campaign_name ASC;

-- Phân tích theo nhân khẩu (Age, gender)
SELECT 
    f.date_start,
    f.account_id,
    acc.account_name,
    s.campaign_id,
    c.campaign_name,
    f.age,
    multiIf(
        f.age = '18-24', 1,
        f.age = '25-34', 2,
        f.age = '35-44', 3,
        f.age = '45-54', 4,
        f.age = '55-64', 5,
        f.age = '65+', 6,
        99
    ) AS age_order,
    f.gender,
    SUM(f.spend) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.reach) AS total_reach,
    SUM(f.clicks) AS total_clicks,
    SUM(f.inline_link_clicks) AS total_link_clicks,
    SUM(f.new_messaging_connections) AS total_messaging,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    (SUM(f.spend) / nullIf(SUM(f.clicks), 0)) AS cpc,
    (SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000 AS cpm,
    (SUM(f.new_messaging_connections) / nullIf(SUM(f.clicks), 0)) * 100 AS message_rate
FROM marketing_db.rt_fact_fb_ad_demographic_daily FINAL f
JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
JOIN marketing_db.rt_dim_ad a ON f.ad_id = a.ad_id
JOIN marketing_db.rt_dim_adset s ON a.adset_id = s.adset_id
JOIN marketing_db.rt_dim_campaign c ON s.campaign_id = c.campaign_id
WHERE f.gender != 'unknown'
GROUP BY 
    f.date_start,
    f.account_id,
    acc.account_name,
    s.campaign_id,
    c.campaign_name,
    f.age, 
    age_order,
    f.gender
ORDER BY f.date_start ASC, age_order ASC, f.gender ASC;

-- Funnel waterfall
-- Dùng fact + dim để Superset filter được theo campaign/adset/account từ mô hình sao.
-- Lưu ý: fact_fb_ad_daily hiện không có leads/purchases, nên funnel này dùng các bước có thật trong fact table.
SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  1 AS step_order,
  'Impressions' AS step,
  toFloat64(ifNull(base.impressions, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.rt_fact_fb_ad_daily FINAL f
    JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
    JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
    JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  2 AS step_order,
  'Clicks' AS step,
  toFloat64(ifNull(base.clicks, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.rt_fact_fb_ad_daily FINAL f
    JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
    JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
    JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  3 AS step_order,
  'Link Clicks' AS step,
  toFloat64(ifNull(base.link_clicks, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.rt_fact_fb_ad_daily FINAL f
    JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
    JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
    JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  4 AS step_order,
  'Landing Page Views' AS step,
  toFloat64(ifNull(base.landing_page_views, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.rt_fact_fb_ad_daily FINAL f
    JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
    JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
    JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
) AS base

UNION ALL

SELECT
  base.date_start,
  base.account_id,
  base.account_name,
  base.campaign_id,
  base.campaign_name,
  base.adset_id,
  base.adset_name,
  base.ad_id,
  5 AS step_order,
  'New Messenger' AS step,
  toFloat64(ifNull(base.new_messaging_connections, 0)) AS step_value
FROM (
    SELECT
        f.date_start AS date_start,
        f.account_id AS account_id,
        acc.account_name AS account_name,
        ds.campaign_id AS campaign_id,
        dc.campaign_name AS campaign_name,
        da.adset_id AS adset_id,
        ds.adset_name AS adset_name,
        f.ad_id AS ad_id,
        f.impressions AS impressions,
        f.clicks AS clicks,
        f.link_clicks AS link_clicks,
        f.landing_page_views AS landing_page_views,
        f.new_messaging_connections AS new_messaging_connections
    FROM marketing_db.rt_fact_fb_ad_daily FINAL f
    JOIN marketing_db.rt_dim_account FINAL acc ON f.account_id = acc.account_id
    JOIN marketing_db.rt_dim_ad FINAL da ON f.ad_id = da.ad_id
    JOIN marketing_db.rt_dim_adset FINAL ds ON da.adset_id = ds.adset_id
    JOIN marketing_db.rt_dim_campaign FINAL dc ON ds.campaign_id = dc.campaign_id
) AS base;

-- =============================================================================
-- GOOGLE ADS DATASETS FOR SUPERSET VISUALIZATION (SNOWFLAKE SCHEMA: DIMS & FACTS)
-- =============================================================================

-- 3. BIỂU ĐỒ GOOGLE ADS TỔNG QUAN CHI TIẾT (Time-Series / Line Chart & Big Number)
-- CPM: Cost per mile (Chi phí trên 1000 lượt hiển thị)
-- CTR: Click through rate (Tỷ lệ nhấp chuột)
-- CPC: Cost per click (Chi phí trên mỗi lượt nhấp)
-- Cost per Conversion: Chi phí trên mỗi lượt chuyển đổi thành công
SELECT 
    f.date,
    dc.campaign_name,
    round(SUM(f.cost), 2) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.all_conversions) AS total_conversions,
    round((SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS ctr,
    round(SUM(f.cost) / nullIf(SUM(f.clicks), 0), 2) AS cpc,
    round((SUM(f.cost) / nullIf(SUM(f.impressions), 0)) * 1000, 2) AS cpm,
    if(
        SUM(f.all_conversions) > 0,
        round(SUM(f.cost) / SUM(f.all_conversions), 2),
        0.0
    ) AS cost_per_conversion
FROM marketing_db.rt_fact_gg_campaign_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
GROUP BY f.date, dc.campaign_name
ORDER BY f.date ASC;


-- 4A. PHÂN TÍCH ĐỘ TUỔI & THIẾT BỊ GOOGLE ADS (Pie Chart / Bar Chart / Heatmap)
-- Dùng fact_gg_age_daily để vẽ tỷ lệ Spend, Impressions, Conversions theo age_range và device.
SELECT 
    f.date,
    dc.campaign_name,
    f.device,
    multiIf(
        f.age_range = 'AGE_RANGE_18_24', '18-24',
        f.age_range = 'AGE_RANGE_25_34', '25-34',
        f.age_range = 'AGE_RANGE_35_44', '35-44',
        f.age_range = 'AGE_RANGE_45_54', '45-54',
        f.age_range = 'AGE_RANGE_55_64', '55-64',
        f.age_range = 'AGE_RANGE_65_UP', '65+',
        f.age_range = 'AGE_RANGE_UNDETERMINED', 'Unknown',
        f.age_range
    ) AS age_range,
    multiIf(
        f.age_range = 'AGE_RANGE_18_24', 1,
        f.age_range = 'AGE_RANGE_25_34', 2,
        f.age_range = 'AGE_RANGE_35_44', 3,
        f.age_range = 'AGE_RANGE_45_54', 4,
        f.age_range = 'AGE_RANGE_55_64', 5,
        f.age_range = 'AGE_RANGE_65_UP', 6,
        99
    ) AS age_order,
    SUM(f.cost) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.all_conversions) AS total_conversions,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    SUM(f.cost) / nullIf(SUM(f.clicks), 0) AS cpc,
    SUM(f.cost) / nullIf(SUM(f.all_conversions), 0) AS cost_per_conversion
FROM marketing_db.rt_fact_gg_age_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
WHERE f.age_range != ''
GROUP BY f.date, dc.campaign_name, f.device, age_range, age_order
ORDER BY f.date ASC, age_order ASC;


-- 4B. PHÂN TÍCH GIỚI TÍNH & THIẾT BỊ GOOGLE ADS (Pie Chart / Bar Chart / Heatmap)
-- Dùng fact_gg_gender_daily để vẽ tỷ lệ Spend, Impressions, Conversions theo gender và device.
SELECT 
    f.date,
    dc.campaign_name,
    f.device,
    f.gender,
    SUM(f.cost) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.all_conversions) AS total_conversions,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    SUM(f.cost) / nullIf(SUM(f.clicks), 0) AS cpc,
    SUM(f.cost) / nullIf(SUM(f.all_conversions), 0) AS cost_per_conversion
FROM marketing_db.rt_fact_gg_gender_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
WHERE f.gender != ''
GROUP BY f.date, dc.campaign_name, f.device, f.gender
ORDER BY f.date ASC, f.gender ASC;


-- 5. HIỆU QUẢ TỪ KHÓA & ĐIỂM CHẤT LƯỢNG (Scatter Plot / Bubble Chart / Table)
-- Vẽ Scatter/Bubble với X = avg_quality_score, Y = cpc, Bubble Size = total_spend, Series = keyword.
-- Giúp nhận diện từ khóa đắt/rẻ và mức độ tối ưu dựa trên Quality Score.
SELECT
    f.date,
    dc.campaign_name,
    dg.adgroup_name,
    f.keyword,
    f.device,
    AVG(f.quality_score) AS avg_quality_score,
    SUM(f.cost) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.conversions) AS total_conversions,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    SUM(f.cost) / nullIf(SUM(f.clicks), 0) AS cpc,
    SUM(f.cost) / nullIf(SUM(f.conversions), 0) AS cost_per_conversion
FROM marketing_db.rt_fact_gg_keyword_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
JOIN marketing_db.rt_dim_gg_adgroup FINAL dg ON f.adgroup_id = dg.adgroup_id
GROUP BY f.date, dc.campaign_name, dg.adgroup_name, f.keyword, f.device
ORDER BY total_spend DESC;


-- 6. PHÂN TÍCH HIỆU QUẢ NỘI DUNG SÁNG TẠO - ASSET (Bar Chart / Pivot Table)
-- Dành cho Creative Analytics: so sánh hiệu quả giữa các Headline, Image, Description và đánh giá Performance (BEST, GOOD, LOW).
SELECT
    f.date,
    dc.campaign_name,
    dg.adgroup_name,
    da.asset_name,
    da.asset_type,
    f.asset_performance,
    SUM(f.cost) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr,
    SUM(f.all_conversions) AS total_conversions
FROM marketing_db.rt_fact_gg_asset_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
JOIN marketing_db.rt_dim_gg_adgroup FINAL dg ON f.adgroup_id = dg.adgroup_id
JOIN marketing_db.rt_dim_gg_asset FINAL da ON f.asset_id = da.asset_id
GROUP BY f.date, dc.campaign_name, dg.adgroup_name, da.asset_name, da.asset_type, f.asset_performance
ORDER BY total_spend DESC;


-- 7. PHÂN TÍCH LOẠI CLICK VÀ MẠNG QUẢNG CÁO (Sunburst Chart / Stacked Bar Chart)
-- So sánh phân bổ và hiệu quả của các loại Click (Headline, Sitelines, Call, v.v...) trên các Ad Networks (Search, Display).
SELECT
    f.date,
    dc.campaign_name,
    f.click_type,
    f.device,
    f.ad_network_type,
    SUM(f.cost) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.conversions) AS total_conversions,
    (SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100 AS ctr
FROM marketing_db.rt_fact_gg_click_type_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
GROUP BY f.date, dc.campaign_name, f.click_type, f.device, f.ad_network_type
ORDER BY total_spend DESC;


-- 8. PieChart ~ Filter cho Google Ads (Tỷ lệ phân bổ ngân sách theo Campaign)
-- Cấp dữ liệu: 1 dòng / campaign / day để Superset có thể rollup theo Time Grain (day/week/month/quarter/year).
SELECT
    f.date AS date,
    dd.year AS year,
    dd.quarter AS quarter,
    dd.month AS month,
    dd.month_name AS month_name,
    dd.week AS week,
    f.campaign_id AS campaign_id,
    dc.campaign_name AS campaign_name,
    SUM(f.cost) AS total_spend
FROM marketing_db.rt_fact_gg_campaign_daily FINAL f
JOIN marketing_db.rt_dim_campaign FINAL dc ON f.campaign_id = dc.campaign_id
JOIN marketing_db.rt_dim_date FINAL dd ON f.date = dd.date
GROUP BY
    f.date,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.week,
    f.campaign_id,
    dc.campaign_name;


-- =============================================================================
-- TIKTOK ADS DATASETS FOR SUPERSET VISUALIZATION (SNOWFLAKE SCHEMA: DIMS & FACTS)
-- =============================================================================

-- 9. TikTok Overview theo Campaign/Adgroup theo ngày (Trendline + Big Number)
SELECT
    f.date,
    toYear(f.date) AS year,
    toQuarter(f.date) AS quarter,
    toMonth(f.date) AS month,
    formatDateTime(f.date, '%Y-%m') AS month_label,
    toISOWeek(f.date) AS week,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    ad.adgroup_name,
    round(SUM(f.spend), 2) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.reach) AS total_reach,
    SUM(f.conversion) AS total_conversions,
    SUM(f.purchase) AS total_purchases,
    SUM(f.onsite_shopping) AS total_onsite_shopping,
    SUM(f.total_onsite_shopping_value) AS total_onsite_shopping_value,
    round((SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS ctr,
    round(SUM(f.spend) / nullIf(SUM(f.clicks), 0), 2) AS cpc,
    round((SUM(f.spend) / nullIf(SUM(f.impressions), 0)) * 1000, 2) AS cpm,
    round(SUM(f.spend) / nullIf(SUM(f.conversion), 0), 2) AS cost_per_conversion,
    round((SUM(f.conversion) / nullIf(SUM(f.clicks), 0)) * 100, 2) AS conversion_rate,
    round(SUM(f.total_onsite_shopping_value) / nullIf(SUM(f.spend), 0), 4) AS shopping_roas
FROM marketing_db.rt_fact_tta_ad_daily FINAL f
JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
GROUP BY
    f.date,
    year,
    quarter,
    month,
    month_label,
    week,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    ad.adgroup_name
ORDER BY f.date ASC, total_spend DESC;


-- 10. TikTok Budget Allocation theo Advertiser/Campaign (Pie/Donut/Treemap)
SELECT
    f.date,
    toYear(f.date) AS year,
    toQuarter(f.date) AS quarter,
    toMonth(f.date) AS month,
    toISOWeek(f.date) AS week,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    round(SUM(f.spend), 2) AS total_spend
FROM marketing_db.rt_fact_tta_ad_daily FINAL f
JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
GROUP BY
    f.date,
    year,
    quarter,
    month,
    week,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name
ORDER BY f.date ASC, total_spend DESC;


-- 11. TikTok Engagement Quality (Bar/Heatmap/Scatter)
SELECT
    f.date,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    ad.adgroup_name,
    f.ad_id,
    ad.ad_name,
    round(SUM(f.spend), 2) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.video_play_actions) AS total_video_plays,
    SUM(f.profile_visits) AS total_profile_visits,
    SUM(f.likes) AS total_likes,
    SUM(f.comments) AS total_comments,
    SUM(f.shares) AS total_shares,
    SUM(f.follows) AS total_follows,
    SUM(f.live_views) AS total_live_views,
    (SUM(f.likes) + SUM(f.comments) + SUM(f.shares)) AS total_social_engagement,
    round(((SUM(f.likes) + SUM(f.comments) + SUM(f.shares)) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS engagement_rate,
    round((SUM(f.video_play_actions) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS video_play_rate,
    round((SUM(f.follows) / nullIf(SUM(f.profile_visits), 0)) * 100, 2) AS follow_rate_from_profile
FROM marketing_db.rt_fact_tta_ad_daily FINAL f
JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
GROUP BY
    f.date,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    ad.adgroup_name,
    f.ad_id,
    ad.ad_name
ORDER BY f.date ASC, total_social_engagement DESC;


-- 12. TikTok Commerce/Conversion Funnel (Waterfall/Funnel Chart)
SELECT
    base.date,
    base.advertiser_id,
    base.advertiser_name,
    base.campaign_name,
    base.adgroup_name,
    base.ad_id,
    base.ad_name,
    1 AS step_order,
    'Impressions' AS step,
    toFloat64(ifNull(base.total_impressions, 0)) AS step_value
FROM (
    SELECT
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks) AS total_clicks,
        SUM(f.conversion) AS total_conversions,
        SUM(f.purchase) AS total_purchases,
        SUM(f.onsite_shopping) AS total_onsite_shopping
    FROM marketing_db.rt_fact_tta_ad_daily FINAL f
    JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
    JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
    GROUP BY
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name
) AS base

UNION ALL

SELECT
    base.date,
    base.advertiser_id,
    base.advertiser_name,
    base.campaign_name,
    base.adgroup_name,
    base.ad_id,
    base.ad_name,
    2 AS step_order,
    'Clicks' AS step,
    toFloat64(ifNull(base.total_clicks, 0)) AS step_value
FROM (
    SELECT
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks) AS total_clicks,
        SUM(f.conversion) AS total_conversions,
        SUM(f.purchase) AS total_purchases,
        SUM(f.onsite_shopping) AS total_onsite_shopping
    FROM marketing_db.rt_fact_tta_ad_daily FINAL f
    JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
    JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
    GROUP BY
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name
) AS base

UNION ALL

SELECT
    base.date,
    base.advertiser_id,
    base.advertiser_name,
    base.campaign_name,
    base.adgroup_name,
    base.ad_id,
    base.ad_name,
    3 AS step_order,
    'Conversions' AS step,
    toFloat64(ifNull(base.total_conversions, 0)) AS step_value
FROM (
    SELECT
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks) AS total_clicks,
        SUM(f.conversion) AS total_conversions,
        SUM(f.purchase) AS total_purchases,
        SUM(f.onsite_shopping) AS total_onsite_shopping
    FROM marketing_db.rt_fact_tta_ad_daily FINAL f
    JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
    JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
    GROUP BY
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name
) AS base

UNION ALL

SELECT
    base.date,
    base.advertiser_id,
    base.advertiser_name,
    base.campaign_name,
    base.adgroup_name,
    base.ad_id,
    base.ad_name,
    4 AS step_order,
    'Purchases' AS step,
    toFloat64(ifNull(base.total_purchases, 0)) AS step_value
FROM (
    SELECT
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks) AS total_clicks,
        SUM(f.conversion) AS total_conversions,
        SUM(f.purchase) AS total_purchases,
        SUM(f.onsite_shopping) AS total_onsite_shopping
    FROM marketing_db.rt_fact_tta_ad_daily FINAL f
    JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
    JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
    GROUP BY
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name
) AS base

UNION ALL

SELECT
    base.date,
    base.advertiser_id,
    base.advertiser_name,
    base.campaign_name,
    base.adgroup_name,
    base.ad_id,
    base.ad_name,
    5 AS step_order,
    'Onsite Shopping' AS step,
    toFloat64(ifNull(base.total_onsite_shopping, 0)) AS step_value
FROM (
    SELECT
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks) AS total_clicks,
        SUM(f.conversion) AS total_conversions,
        SUM(f.purchase) AS total_purchases,
        SUM(f.onsite_shopping) AS total_onsite_shopping
    FROM marketing_db.rt_fact_tta_ad_daily FINAL f
    JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
    JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
    GROUP BY
        f.date,
        f.advertiser_id,
        adv.advertiser_name,
        ad.campaign_name,
        ad.adgroup_name,
        f.ad_id,
        ad.ad_name
) AS base;


-- 13. TikTok Creative Text Performance (Table/Pivot cho ad_text)
SELECT
    f.date,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    ad.adgroup_name,
    ad.ad_name,
    ad.ad_text,
    round(SUM(f.spend), 2) AS total_spend,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks) AS total_clicks,
    SUM(f.conversion) AS total_conversions,
    SUM(f.purchase) AS total_purchases,
    SUM(f.total_onsite_shopping_value) AS total_onsite_shopping_value,
    round((SUM(f.clicks) / nullIf(SUM(f.impressions), 0)) * 100, 2) AS ctr,
    round(SUM(f.spend) / nullIf(SUM(f.clicks), 0), 2) AS cpc,
    round(SUM(f.spend) / nullIf(SUM(f.conversion), 0), 2) AS cost_per_conversion,
    round(SUM(f.total_onsite_shopping_value) / nullIf(SUM(f.spend), 0), 4) AS shopping_roas
FROM marketing_db.rt_fact_tta_ad_daily FINAL f
JOIN marketing_db.rt_dim_tta_ad FINAL ad ON f.ad_id = ad.ad_id
JOIN marketing_db.rt_dim_tta_advertiser FINAL adv ON f.advertiser_id = adv.advertiser_id
GROUP BY
    f.date,
    f.advertiser_id,
    adv.advertiser_name,
    ad.campaign_name,
    ad.adgroup_name,
    ad.ad_name,
    ad.ad_text
ORDER BY f.date ASC, total_spend DESC;

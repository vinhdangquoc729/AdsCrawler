-- 1. PieChart ~ Filter
SELECT 
    dc.campaign_name,
    f.date_start,
    SUM(f.spend) AS total_spend
FROM marketing_db.fact_fb_ad_daily f
JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
GROUP BY dc.campaign_name, f.date_start;


-- 2. BIỂU ĐỒ CHI TIẾT (Trendline, Big Number, Bảng biểu...)
SELECT 
    f.date_start,
    dc.campaign_name,
    SUM(f.spend) AS total_spend,
    SUM(f.new_messaging_connections) AS total_messaging,
    SUM(f.clicks) AS total_clicks,
    SUM(f.impressions) AS total_impressions,
    if(SUM(f.new_messaging_connections) > 0, SUM(f.spend) / SUM(f.new_messaging_connections), 0) AS cost_per_message,
    (SUM(clicks) / nullIf(SUM(impressions), 0)) * 100 AS ctr,
    (SUM(new_messaging_connections) / nullIf(SUM(clicks), 0)) * 100 AS cpm
FROM marketing_db.fact_fb_ad_daily f
JOIN marketing_db.dim_ad da ON f.ad_id = da.ad_id
JOIN marketing_db.dim_adset ds ON da.adset_id = ds.adset_id
JOIN marketing_db.dim_campaign dc ON ds.campaign_id = dc.campaign_id
GROUP BY f.date_start, dc.campaign_name
ORDER BY f.date_start ASC;

-- Phân tích theo nhân khẩu (Age, gender)
SELECT 
    c.campaign_name,
    f.age,
    f.gender,
    SUM(f.spend) AS total_spend,
    SUM(f.new_messaging_connections) AS total_messaging,
    SUM(f.impressions) AS total_impressions
FROM marketing_db.fact_fb_ad_demographic_daily f
JOIN marketing_db.dim_ad a ON f.ad_id = a.ad_id
JOIN marketing_db.dim_adset s ON a.adset_id = s.adset_id
JOIN marketing_db.dim_campaign c ON s.campaign_id = c.campaign_id
WHERE f.gender != 'unknown'
GROUP BY 
    c.campaign_name,
    f.age, 
    f.gender
ORDER BY total_spend DESC


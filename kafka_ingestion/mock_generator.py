# kafka_ingestion/mock_generator.py

import random
import hashlib
import math
from datetime import datetime, timedelta
from minio_client import MinioClient

class MockGenerator:
    """
    High-Fidelity Mock Data Generator for Marketing Analytics.
    Features:
    - Deterministic seeding (Reproducable)
    - Realistic Ad lifecycles (Start/End dates)
    - Seasonality (Weekend CPM spikes + Holiday peaks)
    - Ad Quality Scores & Diminishing Returns (Non-linear CPM)
    - Shared Creative Pool (Creative-level analytics)
    - Strict Waterfall Funnel (Metric integrity)
    - Context-aware Demographics (Targeting biases)
    - XLSX Export
    """

    NAME_POOLS = {
        "campaign": [
            "Chiến dịch Tết Nguyên Đán 2026", "Black Friday - Siêu Sale", "Brand Awareness - Quý 2",
            "Khách hàng mới - Hà Nội & HCM", "Retargeting - Abandoned Cart", "Promotion Mùa Hè Rực Rỡ",
            "Product Launch - New Collection", "Tăng trưởng traffic website"
        ],
        "adset": [
            "Tệp đối tượng Quản lý / CEO", "Sở thích Công nghệ & Gadget", "TP. Hồ Chí Minh - 25-45 tuổi",
            "Lookalike 1% - Khách hàng đã mua", "Interest: Thời trang & Làm đẹp", "Broad Targeting - Toàn quốc",
            "Tệp Custom: Website Visitors (30d)", "Phụ nữ - Quan tâm sức khỏe", "Nam giới - Phụ kiện xe hơi"
        ],
        "ad": [
            "Video Review sản phẩm thực tế", "Banner khuyến mãi - Giảm 50%", "Carousel Slide - Top sản phẩm",
            "Image: Lifestyle người dùng", "Post: Chia sẻ bí quyết sử dụng", "Lead Form: Nhận tư vấn miễn phí",
            "Video Content: Storytelling"
        ]
    }

    def __init__(self, endpoint=None, access_key=None, secret_key=None, enable_xlsx_buffer=False):
        self.minio_client = MinioClient(endpoint, access_key, secret_key)
        self.rng = random.Random()
        self.export_buffer = {}
        self.enable_xlsx_buffer = enable_xlsx_buffer

    def _get_deterministic_id(self, seed_str, prefix="id"):
        h = hashlib.md5(seed_str.encode()).hexdigest()
        return f"{prefix}_{h[:15]}"

    def _get_random_name(self, category, seed_str):
        """Generates a stable name using its own internal RNG based on seed_str."""
        local_rng = random.Random(seed_str)
        pool = self.NAME_POOLS.get(category, ["Default Name"])
        base = local_rng.choice(pool)
        suffix = hashlib.md5(seed_str.encode()).hexdigest()[:3].upper()
        return f"{base} [#{suffix}]"

    def _get_dates_in_range(self, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    def _get_seasonality_multiplier(self, date_str):
        p_date = datetime.strptime(date_str, "%Y-%m-%d")
        day_of_week = p_date.weekday()
        multiplier = 1.0
        if day_of_week >= 4: multiplier = 1.4 # Weekend
        month, day = p_date.month, p_date.day
        if (month == 1 and day >= 15) or (month == 2 and day <= 5): multiplier *= 1.8 # Tet
        return multiplier

    def _analyze_targeting_bias(self, name):
        bias = {}
        if "Phụ nữ" in name: bias["gender"] = "female"
        elif "Nam giới" in name: bias["gender"] = "male"
        if "CEO" in name or "Quản lý" in name: bias["age_range"] = (35, 65)
        elif "18-24" in name: bias["age_range"] = (18, 24)
        return bias

    def _get_age_gender_weights(self, rng, bias=None):
        bias = bias or {}
        ages = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
        genders = ["female", "male"]
        raw_weights = []
        for age in ages:
            age_base = 1.0
            if age in ["25-34", "35-44"]: age_base = 3.0
            elif age == "65+": age_base = 0.3
            if "age_range" in bias:
                p_min = int(age.split('-')[0].replace('+', ''))
                if p_min < bias["age_range"][0] or p_min > bias["age_range"][1]: age_base *= 0.05
            for gender in genders:
                g_mult = 1.0
                if "gender" in bias and bias["gender"] != gender: g_mult = 0.01
                modifier = rng.uniform(0.8, 1.2)
                raw_weights.append((age, gender, age_base * g_mult * modifier))
        total = sum(w[2] for w in raw_weights)
        return [(w[0], w[1], w[2]/max(0.0001, total)) for w in raw_weights]

    def _sum_metrics(self, target, source):
        keys = [
            "spend", "impressions", "reach", "clicks", "messagingFirstReply",
            "newMessagingConnections", "postComments", "linkClicks",
            "landingPageViews", "thruPlay", "videoViewsP25", "videoViewsP50",
            "videoViewsP75", "videoViewsP95", "videoViewsP100", "postSaves",
            "postShares", "photoViews", "postEngagements", "postReactions", "pageLikes"
        ]
        for k in keys: target[k] = target.get(k, 0) + source.get(k, 0)

    def _upload_chunk(self, table_name, data):
        if not data: return
        if self.enable_xlsx_buffer:
            if table_name not in self.export_buffer: self.export_buffer[table_name] = []
            self.export_buffer[table_name].extend(data)
        result = self.minio_client.upload_json(table_name, data)
        # Quiet upload logging to avoid cluttering stdout
        if not result["success"]: print(f"   ... FAILED upload {table_name}: {result['error']}")

    def export_to_xlsx(self, filepath):
        try:
            import pandas as pd
            if not self.export_buffer: return
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for table_name, records in self.export_buffer.items():
                    pd.DataFrame(records).to_excel(writer, sheet_name=table_name[:31], index=False)
            print(f"Mock Generator: Successfully exported to {filepath}")
        except Exception as e: print(f"Export error: {e}")

    def generate_consistent_suite(self, start_date, end_date, user_id="mock_user_admin", options=None):
        options = options or {}
        acc_cnt, cam_cnt = options.get('accountCount', 1), options.get('campaignCount', 2)
        set_per_cam, ad_per_set = options.get('adsetPerCampaign', 2), options.get('adPerAdset', 3)
        base_seed = options.get('seed', 'ads_crawler_premium_v3')

        self.export_buffer = {}
        dates = self._get_dates_in_range(start_date, end_date)
        total_days = len(dates)

        # 1. Global Creative Pool (20 items)
        creative_pool = []
        for p in range(20):
            p_seed = f"{base_seed}_p_{p}"
            creative_pool.append({
                "id": self._get_deterministic_id(p_seed, "cr"),
                "name": f"Visual Asset: {self._get_random_name('ad', p_seed)}"
            })

        # 2. Skeleton Setup
        accounts = []
        total_accounts, total_campaigns, total_adsets, total_ads = 0, 0, 0, 0

        for i in range(acc_cnt):
            acc_seed = f"{base_seed}_acc_{i}"
            acc_id = self._get_deterministic_id(acc_seed, "act")
            acc = {"id": acc_id, "name": f"Account QC #{i+1}", "campaigns": []}
            total_accounts += 1
            for j in range(cam_cnt):
                cam_seed = f"{acc_seed}_cam_{j}"
                cam = {"id": self._get_deterministic_id(cam_seed, "cam"), "name": self._get_random_name("campaign", cam_seed), "adsets": []}
                total_campaigns += 1
                for k in range(set_per_cam):
                    set_seed = f"{cam_seed}_s_{k}"
                    set_name = self._get_random_name("adset", set_seed)

                    # Properties seeding
                    set_prop_rng = random.Random(set_seed)
                    start_idx = set_prop_rng.randint(0, total_days - 1)
                    duration = set_prop_rng.randint(min(max(1, int(total_days * 0.3)), total_days - start_idx), total_days - start_idx)

                    adset = {
                        "id": self._get_deterministic_id(set_seed, "set"), "name": set_name,
                        "daily_budget": set_prop_rng.randint(100000, 1000000), "start_date": dates[start_idx], "end_date": dates[start_idx + duration - 1],
                        "targeting_bias": self._analyze_targeting_bias(set_name), "ads": []
                    }
                    total_adsets += 1
                    for l in range(ad_per_set):
                        ad_seed = set_seed + f"_a_{l}"
                        ad_prop_rng = random.Random(ad_seed)
                        creative = creative_pool[ad_prop_rng.randint(0, len(creative_pool)-1)]

                        ad_start_idx = ad_prop_rng.randint(start_idx, start_idx + duration - 1)
                        ad_dur = ad_prop_rng.randint(1, start_idx + duration - ad_start_idx)
                        adset["ads"].append({
                            "id": self._get_deterministic_id(ad_seed, "ad"), "name": f"Ad: {creative['name']}",
                            "start_date": dates[ad_start_idx], "end_date": dates[ad_start_idx + ad_dur - 1],
                            "quality_multiplier": ad_prop_rng.uniform(0.6, 2.2),
                            "creative_id": creative["id"], "creative_name": creative["name"]
                        })
                        total_ads += 1
                    cam["adsets"].append(adset)
                acc["campaigns"].append(cam)
            accounts.append(acc)

        print("Mock Generator: Skeleton Built! Summary:")
        print(f"   >>> Total Accounts:  {total_accounts}")
        print(f"   >>> Total Campaigns: {total_campaigns}")
        print(f"   >>> Total Ad Sets:   {total_adsets}")
        print(f"   >>> Total Ad Entities: {total_ads}")
        print("   ----------------------------------------")

        ad_perf_map, adset_perf_map, campaign_perf_map = {}, {}, {}
        TABLES = {
            "ad_daily": "fad_ad_daily_report", "age_gender": "fad_age_gender_detailed_report",
            "adset_daily": "fad_ad_set_daily_report", "campaign_daily": "fad_campaign_daily_report",
            "account_daily": "fad_account_daily_report", "ad_perf": "fad_ad_performance_report",
            "adset_perf": "fad_ad_set_performance_report", "campaign_perf": "fad_campaign_overview_report"
        }

        # 3. Daily Waterfall Loop
        for date in dates:
            print(f"Mock Generator: Processing Date: {date}...")
            daily_rng = random.Random(f"{base_seed}_{date}")
            seasonality = self._get_seasonality_multiplier(date)
            day_ad_buffer, day_age_gender_buffer = [], []
            day_adset_map, day_campaign_map, day_account_map = {}, {}, {}

            for acc in accounts:
                for cam in acc["campaigns"]:
                    for adset in cam["adsets"]:
                        if not (adset["start_date"] <= date <= adset["end_date"]): continue
                        for ad in adset["ads"]:
                            if not (ad["start_date"] <= date <= ad["end_date"]): continue

                            # Logarithmic CPM Inflation (Diminishing Returns)
                            spend = round(daily_rng.uniform(0.1, 0.4) * (adset["daily_budget"] / ad_per_set), 2)
                            inflation = 1 + math.log10(max(1, spend / 50000))

                            quality = ad["quality_multiplier"]
                            base_cpm = daily_rng.uniform(40000, 150000)
                            cpm = base_cpm * seasonality * inflation / quality

                            impressions = max(1, int((spend / max(1, cpm)) * 1000))
                            ctr = 0.015 * quality * daily_rng.uniform(0.8, 1.2)
                            clicks = int(impressions * ctr)

                            # Strict Waterfall Funnel
                            link_click_ratio = daily_rng.uniform(0.65, 0.85)
                            link_clicks = int(clicks * link_click_ratio)

                            landing_page_views = int(link_clicks * daily_rng.uniform(0.7, 0.9))
                            messaging_first_reply = int(clicks * daily_rng.uniform(0.05, 0.15) * quality)
                            new_messaging_connections = int(messaging_first_reply * daily_rng.uniform(0.8, 0.95))

                            post_comments = int(clicks * daily_rng.uniform(0.02, 0.1) * quality)
                            post_shares = int(clicks * daily_rng.uniform(0.01, 0.05))
                            post_saves = int(clicks * daily_rng.uniform(0.02, 0.08))
                            post_reactions = int(clicks * daily_rng.uniform(0.1, 0.3) * quality)
                            photo_views = int(clicks * daily_rng.uniform(0.2, 0.5))
                            page_likes = int(clicks * daily_rng.uniform(0.01, 0.04))

                            v25 = int(impressions * daily_rng.uniform(0.08, 0.15) * quality)
                            v50 = int(v25 * daily_rng.uniform(0.3, 0.5))
                            v75 = int(v50 * daily_rng.uniform(0.4, 0.6))
                            v95 = int(v75 * daily_rng.uniform(0.5, 0.7))
                            v100 = int(v95 * daily_rng.uniform(0.7, 0.9))
                            thru_play = int(v25 * daily_rng.uniform(0.5, 0.8))

                            post_engagements = clicks + post_comments + post_shares + post_reactions + post_saves + photo_views

                            ad_data = {
                                "account_id": acc["id"], "account_name": acc["name"], "campaign_id": cam["id"], "campaign_name": cam["name"],
                                "ad_set_id": adset["id"], "ad_set_name": adset["name"], "id": ad["id"], "name": ad["name"],
                                "date_start": date, "date_stop": date, "user_id": user_id, "spend": spend, "impressions": impressions,
                                "reach": int(impressions * 0.95), "clicks": clicks, "linkClicks": link_clicks,
                                "landingPageViews": landing_page_views, "messagingFirstReply": messaging_first_reply,
                                "newMessagingConnections": new_messaging_connections, "postComments": post_comments,
                                "postShares": post_shares, "postSaves": post_saves, "postReactions": post_reactions, "photoViews": photo_views,
                                "pageLikes": page_likes, "postEngagements": post_engagements,
                                "creative_id": ad["creative_id"], "creative_name": ad["creative_name"],
                                "thruPlay": thru_play, "videoViewsP25": v25, "videoViewsP50": v50,
                                "videoViewsP75": v75, "videoViewsP95": v95, "videoViewsP100": v100,
                                "daily_budget": adset["daily_budget"]
                            }
                            day_ad_buffer.append(ad_data)

                            weights = self._get_age_gender_weights(daily_rng, adset["targeting_bias"])
                            dist_keys = [
                                "spend", "impressions", "reach", "clicks", "linkClicks", "landingPageViews",
                                "messagingFirstReply", "newMessagingConnections", "postComments",
                                "postShares", "postSaves", "postReactions", "photoViews",
                                "pageLikes", "postEngagements", "thruPlay",
                                "videoViewsP25", "videoViewsP50", "videoViewsP75", "videoViewsP95", "videoViewsP100"
                            ]
                            remainders = {k: ad_data.get(k, 0) for k in dist_keys}

                            for idx, (age, gender, weight) in enumerate(weights):
                                row_data = {**ad_data, "age": age, "gender": gender}
                                for k in dist_keys:
                                    if idx == len(weights) - 1: val = remainders[k]
                                    else:
                                        total = ad_data.get(k, 0)
                                        val = round(total * weight, 2) if k == "spend" else int(total * weight)
                                        remainders[k] -= val
                                    row_data[k] = val
                                day_age_gender_buffer.append(row_data)

                            for m, target_map in [("id", ad_perf_map), (adset["id"], adset_perf_map), (cam["id"], campaign_perf_map)]:
                                k = ad[m] if m == "id" else m
                                if k not in target_map:
                                    target_map[k] = {**ad_data, "id": k, "name": ad[m] if m == "id" else (adset["name"] if m == adset["id"] else cam["name"]),
                                                     "date_start": ad["start_date"] if m == "id" else (adset["start_date"] if m == adset["id"] else dates[0]),
                                                     "date_stop": ad["end_date"] if m == "id" else (adset["end_date"] if m == adset["id"] else dates[-1])}
                                else: self._sum_metrics(target_map[k], ad_data)

                            for k, t_map in [(adset["id"], day_adset_map), (cam["id"], day_campaign_map), (acc["id"], day_account_map)]:
                                if k not in t_map: t_map[k] = {**ad_data, "id": k, "name": adset["name"] if k == adset["id"] else (cam["name"] if k == cam["id"] else acc["name"])}
                                else: self._sum_metrics(t_map[k], ad_data)

            self._upload_chunk(TABLES["ad_daily"], day_ad_buffer)
            self._upload_chunk(TABLES["age_gender"], day_age_gender_buffer)
            for val in day_adset_map.values(): val["budget_remaining"] = round(val["daily_budget"] - val["spend"], 2)
            self._upload_chunk(TABLES["adset_daily"], list(day_adset_map.values()))
            self._upload_chunk(TABLES["campaign_daily"], list(day_campaign_map.values()))
            self._upload_chunk(TABLES["account_daily"], list(day_account_map.values()))
            print(f"   >>> Records: Ads: {len(day_ad_buffer)}, Demographics: {len(day_age_gender_buffer)}, AdSets: {len(day_adset_map)}")

        print("Mock Generator: Uploading lifecycle performance reports...")
        self._upload_chunk(TABLES["ad_perf"], list(ad_perf_map.values()))
        self._upload_chunk(TABLES["adset_perf"], list(adset_perf_map.values()))
        self._upload_chunk(TABLES["campaign_perf"], list(campaign_perf_map.values()))
        print(f"Mock Generator: Success. [Total Ads Processed: {len(ad_perf_map)}]")

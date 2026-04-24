# ingest/google/mock.py

import random
import hashlib
from datetime import datetime, timedelta
from ..utils.minio_client import MinioClient

class MockGenerator:
    """
    Google Ads Mock Data Generator.
    Deterministic, hierarchical, and realistic metric simulation.
    """

    NAME_POOLS = {
        "campaign": [
            "GG Search / Nam - Quà Tặng người yêu", "GG Display - Retargeting Website",
            "PMax - Toàn quốc - Mùa Hè 2026", "GG Search / Nữ - Trang sức Cara Luna",
            "Brand Awareness - Google Video", "Khuyến mãi 30/4 - Search Ads"
        ],
        "adgroup": [
            "Keyword Quà tặng bạn gái", "Sở thích Trang sức cao cấp", "Tệp Re-marketing 30d",
            "Keyword Dây chuyền bạc", "Đối tượng Quan tâm quà tặng", "Keyword Nhẫn đính hôn"
        ],
        "asset_text": [
            "Quà Tặng Cho Bạn Gái Mới Yêu", "Trang Sức Bạc Cao Cấp Cara Luna", "Miễn Phí Giao Hàng Toàn Quốc",
            "Giảm Giá 20% Cho Đơn Đầu Tiên", "Thiết Kế Tinh Xảo Đẳng Cấp", "Bảo Hành Trọn Đời Sản Phẩm",
            "Món Quà Ý Nghĩa Cho Phái Đẹp", "Cara Luna - Đánh Thức Vẻ Đẹp"
        ],
        "keyword": [
            "tặng quà sinh nhật cho bạn gái", "trang sức bạc cara luna", "quà tặng người yêu",
            "dây chuyền bạc nữ", "nhẫn đính hôn đẹp", "quà tặng 8/3 cho bạn gái",
            "trang sức thiết kế", "bông tai bạc cao cấp"
        ]
    }

    PERFORMANCE_TIERS = ["BEST", "GOOD", "LOW", "PENDING", "LEARNING"]
    AGE_RANGES = ["AGE_RANGE_18_24", "AGE_RANGE_25_34", "AGE_RANGE_35_44", "AGE_RANGE_45_54", "AGE_RANGE_55_64", "AGE_RANGE_65_UP", "AGE_RANGE_UNDETERMINED"]
    GENDERS = ["MALE", "FEMALE", "UNDETERMINED"]
    DEVICES = ["MOBILE", "DESKTOP", "TABLET"]
    CLICK_TYPES = ["URL_CLICKS", "AD_IMAGE", "HEADLINE", "SITELINKS", "CALLS"]
    AD_NETWORK_TYPES = ["SEARCH", "SEARCH_PARTNERS", "DISPLAY"]

    def __init__(self, endpoint=None, access_key=None, secret_key=None, enable_xlsx_buffer=False):
        self.minio_client = MinioClient(endpoint, access_key, secret_key)
        self.rng = random.Random()
        self.export_buffer = {}
        self.enable_xlsx_buffer = enable_xlsx_buffer

    def _get_deterministic_id(self, seed_str, prefix=""):
        h = hashlib.md5(seed_str.encode()).hexdigest()
        # Google IDs are usually large integers, but we can prefix them
        if prefix:
            return f"{prefix}_{h[:12]}"
        return str(int(h[:12], 16))[:12]

    def _get_random_name(self, category, seed_str):
        local_rng = random.Random(seed_str)
        pool = self.NAME_POOLS.get(category, ["Default Name"])
        base = local_rng.choice(pool)
        suffix = hashlib.md5(seed_str.encode()).hexdigest()[:4].upper()
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
        if day_of_week >= 5: multiplier = 1.3 # Weekend spikes
        month, day = p_date.month, p_date.day
        # Special dates (Valentine, 8/3, etc. for gifts)
        if month == 2 and 10 <= day <= 14: multiplier *= 2.5
        if month == 3 and 5 <= day <= 8: multiplier *= 2.0
        return multiplier

    def _sum_metrics(self, target, source):
        keys = ["cost", "impressions", "clicks", "all_conversions"]
        for k in keys:
            val = source.get(k, 0)
            # Ensure val is numeric
            if isinstance(val, str):
                try: val = float(val) if '.' in val else int(val)
                except (ValueError, TypeError): val = 0
            target[k] = target.get(k, 0) + val

        if target.get("impressions", 0) > 0:
            target["ctr"] = target["clicks"] / target["impressions"]
        else:
            target["ctr"] = 0

    def _upload_chunk(self, table_name, data):
        if not data: return
        if self.enable_xlsx_buffer:
            if table_name not in self.export_buffer: self.export_buffer[table_name] = []
            self.export_buffer[table_name].extend(data)
        result = self.minio_client.upload_json(table_name, data)
        if not result["success"]: print(f"   ... FAILED upload {table_name}: {result['error']}")

    def export_to_xlsx(self, filepath):
        try:
            import pandas as pd
            if not self.export_buffer: return
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for table_name, records in self.export_buffer.items():
                    # Sheet names max 31 chars
                    sheet_name = table_name.replace("gad_", "")[:31]
                    pd.DataFrame(records).to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Mock Generator (Google): Exported to {filepath}")
        except Exception as e: print(f"Export error: {e}")

    def generate_consistent_suite(self, start_date, end_date, user_id="admin", options=None):
        options = options or {}
        acc_cnt = options.get('accountCount', 1)
        cam_per_acc = options.get('campaignCount', 2)
        group_per_cam = options.get('adGroupCount', 2)
        ad_per_group = options.get('adCount', 2)
        assets_per_ad = options.get('assetCount', 4)
        base_seed = options.get('seed', 'google_ads_seed_2026')

        self.export_buffer = {}
        dates = self._get_dates_in_range(start_date, end_date)

        # Determine which years are involved in the request
        requested_years = sorted(list(set(int(d.split('-')[0]) for d in dates)))

        # To handle cross-year consistency (campaigns starting in N-1 and ending in N),
        # we ALWAYS generate the skeleton for the requested years AND their previous years.
        years_to_generate = sorted(list(set(requested_years) | set(y - 1 for y in requested_years)))

        # 1. Global Asset Pool
        asset_pool = []
        for i in range(20):
            a_seed = f"{base_seed}_asset_{i}"
            a_rng = random.Random(a_seed)
            a_type = "TEXT" if i < 15 else "IMAGE"
            asset_pool.append({
                "asset_id": self._get_deterministic_id(a_seed, "as"),
                "asset_name": f"Asset {i+1}",
                "asset_type": a_type,
                "asset_text": a_rng.choice(self.NAME_POOLS["asset_text"]) if a_type == "TEXT" else "",
                "image_url": f"https://picsum.photos/seed/{a_seed}/800/600" if a_type == "IMAGE" else "",
                "asset_performance": a_rng.choice(self.PERFORMANCE_TIERS)
            })

        # 2. Skeleton Hierarchy
        accounts = []
        for i in range(acc_cnt):
            acc_seed = f"{base_seed}_acc_{i}"
            acc = {
                "id": self._get_deterministic_id(acc_seed, "9"),
                "name": f"Cara Luna Account #{i+1} ({self._get_deterministic_id(acc_seed)})",
                "campaigns": []
            }

            # For each account, generate entities for each involved year
            for year in years_to_generate:
                year_anchor = datetime(year, 1, 1)
                for j in range(cam_per_acc):
                    cam_seed = f"{acc_seed}_{year}_cam_{j}"
                    cam = {
                        "id": self._get_deterministic_id(cam_seed),
                        "name": self._get_random_name("campaign", cam_seed),
                        "ad_groups": []
                    }
                    for k in range(group_per_cam):
                        grp_seed = f"{cam_seed}_grp_{k}"
                        grp_rng = random.Random(grp_seed)

                        # Lifecycle relative to the YEAR anchor
                        start_offset = grp_rng.randint(-30, 330)
                        duration = grp_rng.randint(60, 200)
                        grp_start = (year_anchor + timedelta(days=start_offset)).strftime("%Y-%m-%d")
                        grp_end = (year_anchor + timedelta(days=start_offset + duration)).strftime("%Y-%m-%d")

                        group = {
                            "id": self._get_deterministic_id(grp_seed),
                            "name": self._get_random_name("adgroup", grp_seed),
                            "budget": grp_rng.randint(200000, 1000000),
                            "start_date": grp_start,
                            "end_date": grp_end,
                            "ads": [],
                            "keywords": []
                        }
                        # Generate keywords for this group
                        for m in range(options.get('keywordCount', 3)):
                            kw_seed = f"{grp_seed}_kw_{m}"
                            kw_rng = random.Random(kw_seed)
                            group["keywords"].append({
                                "keyword": kw_rng.choice(self.NAME_POOLS["keyword"]),
                                "match_type": kw_rng.choice(["BROAD", "PHRASE", "EXACT"]),
                                "keyword_status": "ENABLED"
                            })

                        for l in range(ad_per_group):
                            ad_seed = f"{grp_seed}_ad_{l}"
                            ad_rng = random.Random(ad_seed)
                            ad_assets = ad_rng.sample(asset_pool, assets_per_ad)
                            group["ads"].append({
                                "id": self._get_deterministic_id(ad_seed),
                                "assets": ad_assets,
                                "quality_score": ad_rng.uniform(0.7, 1.5)
                            })
                        cam["ad_groups"].append(group)
                    acc["campaigns"].append(cam)
            accounts.append(acc)

        print(f"Mock Generator (Google): Built hierarchy for years {years_to_generate}.")

        TABLES = {
            "ad_asset": "gad_ad_asset_daily_report",
            "keyword": "gad_keyword_performance_report",
            "age": "gad_age_report",
            "gender": "gad_gender_report",
            "click_type": "gad_click_type_report",
            "ad_group": "gad_ad_group_daily_report",
            "campaign": "gad_campaign_daily_report",
            "account": "gad_account_daily_report"
        }

        # 3. Daily Data Generation
        for date in dates:
            print(f"   Processing Date: {date}...")
            seasonality = self._get_seasonality_multiplier(date)
            day_rng = random.Random(f"{base_seed}_{date}")

            day_asset_buffer = []
            day_keyword_buffer = []
            day_age_buffer = []
            day_gender_buffer = []
            day_click_type_buffer = []
            day_group_map = {}
            day_campaign_map = {}
            day_account_map = {}

            for acc in accounts:
                for cam in acc["campaigns"]:
                    # Click Type Report (Campaign Level breakdown)
                    cam_spend = 0
                    cam_clicks = 0
                    cam_impressions = 0
                    cam_conv = 0

                    for grp in cam["ad_groups"]:
                        if not (grp["start_date"] <= date <= grp["end_date"]): continue
                        for ad in grp["ads"]:
                            # PER-AD DETERMINISTIC SEEDING
                            ad_daily_seed = f"{ad['id']}_{date}"
                            ad_rng = random.Random(ad_daily_seed)

                            # PROGRESSION LOGIC
                            curr_dt = datetime.strptime(date, "%Y-%m-%d")
                            start_dt = datetime.strptime(grp["start_date"], "%Y-%m-%d")
                            days_active = max(0, (curr_dt - start_dt).days)
                            growth_mult = 1.0 + (days_active * 0.004)
                            perf_mult = 1.0 + (min(45, days_active) * 0.0015)

                            # INTRA-DAY REALTIME FACTOR
                            today_str = datetime.now().strftime("%Y-%m-%d")
                            intra_day_factor = 1.0
                            if date == today_str:
                                now = datetime.now()
                                seconds_passed = now.hour * 3600 + now.minute * 60 + now.second
                                intra_day_factor = seconds_passed / 86400.0

                            # Daily metrics for the ad
                            base_spend = ad_rng.uniform(0.1, 0.5) * (grp["budget"] / ad_per_group)
                            daily_spend = base_spend * growth_mult * intra_day_factor
                            base_cpc = ad_rng.uniform(2000, 8000)
                            cpc = base_cpc * seasonality / (ad["quality_score"] * perf_mult)

                            clicks = max(0, int(daily_spend / max(1, cpc)))
                            ctr = ad_rng.uniform(0.05, 0.25)
                            impressions = int(clicks / max(0.001, ctr))
                            conversions = clicks * ad_rng.uniform(0.01, 0.1)
                            all_conv = conversions * ad_rng.uniform(1.1, 1.5)

                            cam_spend += daily_spend
                            cam_clicks += clicks
                            cam_impressions += impressions
                            cam_conv += all_conv

                            # Distribute metrics among assets
                            rem_impressions = impressions
                            rem_clicks = clicks
                            rem_cost = daily_spend
                            rem_conv = all_conv

                            num_assets = len(ad["assets"])
                            for idx, asset in enumerate(ad["assets"]):
                                if idx == num_assets - 1:
                                    a_impressions = rem_impressions
                                    a_clicks = rem_clicks
                                    a_cost = rem_cost
                                    a_conv = rem_conv
                                else:
                                    share = ad_rng.uniform(0.5, 1.5) / num_assets
                                    a_impressions = int(impressions * share)
                                    a_clicks = int(clicks * share)
                                    a_cost = daily_spend * share
                                    a_conv = all_conv * share

                                    rem_impressions -= a_impressions
                                    rem_clicks -= a_clicks
                                    rem_cost -= a_cost
                                    rem_conv -= a_conv

                                row = {
                                    "ad_id": ad["id"], "asset_id": asset["asset_id"], "date": date,
                                    "campaign_id": cam["id"], "campaign_name": cam["name"],
                                    "adgroup_id": grp["id"], "adgroup_name": grp["name"],
                                    "asset_name": asset["asset_name"], "asset_type": asset["asset_type"],
                                    "asset_text": asset["asset_text"], "image_url": asset["image_url"],
                                    "asset_performance": asset["asset_performance"],
                                    "impressions": a_impressions, "clicks": a_clicks,
                                    "ctr": a_clicks / max(1, a_impressions), "all_conversions": a_conv,
                                    "cost": int(a_cost), "account_id": acc["id"], "account_name": acc["name"]
                                }
                                day_asset_buffer.append(row)

                                for k, t_map in [(grp["id"], day_group_map), (cam["id"], day_campaign_map), (acc["id"], day_account_map)]:
                                    if k not in t_map:
                                        t_map[k] = {
                                            "id": k, "name": grp["name"] if k == grp["id"] else (cam["name"] if k == cam["id"] else acc["name"]),
                                            "date": date, "impressions": 0, "clicks": 0, "cost": 0, "all_conversions": 0
                                        }
                                        if k == acc["id"]: t_map[k]["account_id"] = acc["id"]
                                    self._sum_metrics(t_map[k], row)

                        # Keyword/Age/Gender breakdown (Group Level)
                        # We'll use the total group metrics for the day to distribute
                        grp_data = day_group_map.get(grp["id"], {"impressions": 0, "clicks": 0, "cost": 0, "all_conversions": 0})

                        # Distribution helper
                        def distribute_grp(items, key_name, extra_fields=None):
                            res = []
                            rem = {k: grp_data[k] for k in ["impressions", "clicks", "cost", "all_conversions"]}
                            for i, item in enumerate(items):
                                row = {
                                    "adgroup_id": grp["id"], "date": date, "campaign_id": cam["id"], "campaign_name": cam["name"],
                                    "adgroup_name": grp["name"], "account_id": acc["id"], "account_name": acc["name"],
                                    "device": day_rng.choice(self.DEVICES)
                                }
                                if isinstance(item, dict): row.update(item)
                                else: row[key_name] = item

                                if extra_fields: row.update(extra_fields)

                                if i == len(items) - 1:
                                    for k in rem: row[k] = rem[k]
                                else:
                                    # Deterministic share based on group+date
                                    share_rng = random.Random(f"{grp['id']}_{date}_{i}")
                                    share = share_rng.uniform(0.5, 1.5) / len(items)
                                    for k in rem:
                                        val = int(grp_data[k] * share) if k != "cost" else grp_data[k] * share
                                        row[k] = val
                                        rem[k] -= val

                                row["ctr"] = row["clicks"] / max(1, row["impressions"])
                                row["conversions"] = int(row["all_conversions"] * 0.8)
                                row["average_cpc"] = row["cost"] / max(1, row["clicks"])
                                row["cost_per_conversion"] = row["cost"] / max(1, row["conversions"])
                                res.append(row)
                            return res

                        # Keywords
                        day_keyword_buffer.extend(distribute_grp(grp["keywords"], "keyword", {"quality_score": ""}))

                        # Age
                        day_age_buffer.extend(distribute_grp(self.AGE_RANGES, "age_range"))

                        # Gender
                        day_gender_buffer.extend(distribute_grp(self.GENDERS, "gender"))

                    # Click Type
                    for ct_idx, ct in enumerate(self.CLICK_TYPES):
                        ct_rng = random.Random(f"{cam['id']}_{date}_{ct}")
                        share = ct_rng.uniform(0.5, 1.5) / len(self.CLICK_TYPES)
                        day_click_type_buffer.append({
                            "campaign_id": cam["id"], "date": date, "click_type": ct,
                            "campaign_name": cam["name"], "campaign_status": "ENABLED",
                            "impressions": int(cam_impressions * share), "clicks": int(cam_clicks * share),
                            "ctr": (cam_clicks * share) / max(1, cam_impressions * share),
                            "conversions": int(cam_conv * share * 0.8), "all_conversions": cam_conv * share,
                            "device": day_rng.choice(self.DEVICES), "ad_network_type": day_rng.choice(self.AD_NETWORK_TYPES),
                            "cost": int(cam_spend * share), "account_id": acc["id"], "account_name": acc["name"]
                        })

            self._upload_chunk(TABLES["ad_asset"], day_asset_buffer)
            self._upload_chunk(TABLES["keyword"], day_keyword_buffer)
            self._upload_chunk(TABLES["age"], day_age_buffer)
            self._upload_chunk(TABLES["gender"], day_gender_buffer)
            self._upload_chunk(TABLES["click_type"], day_click_type_buffer)
            self._upload_chunk(TABLES["ad_group"], list(day_group_map.values()))
            self._upload_chunk(TABLES["campaign"], list(day_campaign_map.values()))
            self._upload_chunk(TABLES["account"], list(day_account_map.values()))

        print("Mock Generator (Google): All dates processed.")

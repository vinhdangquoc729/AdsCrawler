# ingest/workers/stream_producer.py
# Generates mock ad records in-memory and publishes them directly to Kafka stream topics.
# Facebook -> topic_fb_raw, Google -> topic_google_raw

import json
import os
import random
import argparse
from datetime import datetime, timedelta
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FB_TOPIC = "topic_fb_raw"
GOOGLE_TOPIC = "topic_google_raw"


def _date_range(days):
    end = datetime.now()
    return [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]


def _envelope(platform, report_type, record):
    return {
        "metadata": {
            "platform": platform,
            "report_type": report_type,
            "ingested_at": datetime.utcnow().isoformat(),
            "crawling_type": "mock",
        },
        "data": json.dumps(record),
    }


def generate_facebook_records(days=7, seed=42):
    rng = random.Random(seed)
    records = []
    account_id, campaign_id, adset_id = "act_001", "camp_001", "adset_001"
    ad_id, creative_id = "ad_001", "creative_001"

    for date in _date_range(days):
        spend = round(rng.uniform(50, 500), 2)
        impressions = rng.randint(1000, 50000)
        reach = int(impressions * rng.uniform(0.7, 0.95))
        clicks = int(impressions * rng.uniform(0.01, 0.05))

        records.append(("ad_daily", {
            "id": ad_id, "name": "Mock Ad",
            "campaign_id": campaign_id, "campaign_name": "Mock Campaign",
            "adset_id": adset_id, "adset_name": "Mock Adset",
            "account_id": account_id, "account_name": "Mock Account",
            "date_start": date, "status": "ACTIVE", "effective_status": "ACTIVE",
            "created_time": "2026-01-01T00:00:00+0000",
            "spend": str(spend), "impressions": str(impressions),
            "reach": str(reach), "clicks": str(clicks),
            "ctr": str(round(clicks / max(impressions, 1) * 100, 2)),
            "cpc": str(round(spend / max(clicks, 1), 2)),
            "cpm": str(round(spend / impressions * 1000, 2)),
            "frequency": str(round(impressions / max(reach, 1), 2)),
            "New Messaging Connections": rng.randint(0, 20),
            "Cost per New Messaging": str(round(spend / 10, 2)),
            "Link clicks": rng.randint(0, clicks),
            "Landing page views": rng.randint(0, clicks),
        }))

        records.append(("ad_creative", {
            "id": ad_id, "account_id": account_id, "creative_id": creative_id,
            "creative_title": "Mock Creative", "creative_body": "Mock body",
            "creative_thumbnail_raw_url": "", "creative_link": "https://example.com",
            "page_name": "Mock Page", "date_start": date,
            "spend": str(spend), "impressions": str(impressions),
            "reach": str(reach), "clicks": str(clicks),
            "New Messaging Connections": rng.randint(0, 10),
            "Post engagements": rng.randint(0, 100),
            "Post reactions": rng.randint(0, 50),
            "Post shares": rng.randint(0, 20),
            "Photo views": rng.randint(0, 200),
        }))

        for age in ["18-24", "25-34", "35-44"]:
            for gender in ["male", "female"]:
                seg_imp = int(impressions * rng.uniform(0.1, 0.3))
                records.append(("age_gender", {
                    "id": ad_id, "account_id": account_id,
                    "date_start": date, "age": age, "gender": gender,
                    "spend": str(round(spend * rng.uniform(0.05, 0.2), 2)),
                    "impressions": str(seg_imp),
                    "reach": str(int(seg_imp * 0.9)),
                    "clicks": str(int(seg_imp * 0.02)),
                    "inline_link_clicks": rng.randint(0, 30),
                    "New Messaging Connections": rng.randint(0, 5),
                }))

    return records


def generate_google_records(days=7, seed=42):
    rng = random.Random(seed)
    records = []
    account_id, campaign_id, adgroup_id = "google_acc_001", "gcam_001", "gadg_001"

    for date in _date_range(days):
        impressions = rng.randint(1000, 30000)
        clicks = int(impressions * rng.uniform(0.02, 0.08))
        cost = round(rng.uniform(50, 400), 2)

        records.append(("campaign_daily", {
            "id": campaign_id, "name": "Mock Google Campaign", "date": date,
            "impressions": impressions, "clicks": clicks, "cost": cost,
            "all_conversions": rng.randint(0, 30),
            "ctr": round(clicks / max(impressions, 1) * 100, 2),
        }))

        records.append(("ad_group_daily", {
            "id": adgroup_id, "name": "Mock Ad Group", "date": date,
            "impressions": impressions, "clicks": clicks, "cost": cost,
            "all_conversions": rng.randint(0, 20),
            "ctr": round(clicks / max(impressions, 1) * 100, 2),
        }))

        for keyword in ["mock keyword A", "mock keyword B"]:
            kw_imp = int(impressions * rng.uniform(0.1, 0.4))
            kw_clicks = int(kw_imp * rng.uniform(0.02, 0.1))
            kw_cost = round(cost * rng.uniform(0.1, 0.3), 2)
            records.append(("keyword", {
                "adgroup_id": adgroup_id, "date": date,
                "campaign_id": campaign_id, "campaign_name": "Mock Google Campaign",
                "adgroup_name": "Mock Ad Group",
                "account_id": account_id, "account_name": "Mock Google Account",
                "device": rng.choice(["MOBILE", "DESKTOP", "TABLET"]),
                "keyword": keyword, "quality_score": rng.randint(1, 10),
                "impressions": kw_imp, "clicks": kw_clicks,
                "ctr": round(kw_clicks / max(kw_imp, 1) * 100, 2),
                "conversions": rng.randint(0, 5), "all_conversions": rng.randint(0, 8),
                "average_cpc": round(kw_cost / max(kw_clicks, 1), 2),
                "cost_per_conversion": round(kw_cost / max(rng.randint(1, 5), 1), 2),
                "cost": kw_cost,
            }))

        for age_range in ["AGE_RANGE_18_24", "AGE_RANGE_25_34", "AGE_RANGE_35_44"]:
            seg_imp = int(impressions * rng.uniform(0.1, 0.3))
            seg_clicks = int(seg_imp * rng.uniform(0.02, 0.08))
            seg_cost = round(cost * rng.uniform(0.1, 0.2), 2)
            records.append(("age", {
                "adgroup_id": adgroup_id, "date": date,
                "campaign_id": campaign_id, "campaign_name": "Mock Google Campaign",
                "adgroup_name": "Mock Ad Group",
                "account_id": account_id, "account_name": "Mock Google Account",
                "device": rng.choice(["MOBILE", "DESKTOP", "TABLET"]),
                "age_range": age_range, "impressions": seg_imp, "clicks": seg_clicks,
                "ctr": round(seg_clicks / max(seg_imp, 1) * 100, 2),
                "conversions": rng.randint(0, 3), "all_conversions": rng.randint(0, 5),
                "average_cpc": round(seg_cost / max(seg_clicks, 1), 2),
                "cost_per_conversion": round(seg_cost / max(rng.randint(1, 3), 1), 2),
                "cost": seg_cost,
            }))

        for gender in ["MALE", "FEMALE", "UNDETERMINED"]:
            gen_imp = int(impressions * rng.uniform(0.2, 0.4))
            gen_clicks = int(gen_imp * rng.uniform(0.02, 0.08))
            gen_cost = round(cost * rng.uniform(0.15, 0.35), 2)
            records.append(("gender", {
                "adgroup_id": adgroup_id, "date": date,
                "campaign_id": campaign_id, "campaign_name": "Mock Google Campaign",
                "adgroup_name": "Mock Ad Group",
                "account_id": account_id, "account_name": "Mock Google Account",
                "device": rng.choice(["MOBILE", "DESKTOP", "TABLET"]),
                "gender": gender, "impressions": gen_imp, "clicks": gen_clicks,
                "ctr": round(gen_clicks / max(gen_imp, 1) * 100, 2),
                "conversions": rng.randint(0, 5), "all_conversions": rng.randint(0, 8),
                "average_cpc": round(gen_cost / max(gen_clicks, 1), 2),
                "cost_per_conversion": round(gen_cost / max(rng.randint(1, 5), 1), 2),
                "cost": gen_cost,
            }))

        records.append(("account", {
            "id": "google_acc_001", "name": "Mock Google Account", "date": date,
            "impressions": impressions, "clicks": clicks, "cost": cost,
            "all_conversions": rng.randint(0, 50),
            "account_id": account_id,
            "ctr": round(clicks / max(impressions, 1) * 100, 2),
        }))

        for asset_id, asset_type, asset_text in [
            ("ast_001", "HEADLINE", "Mock Headline"),
            ("ast_002", "DESCRIPTION", "Mock Description"),
            ("ast_003", "IMAGE", ""),
        ]:
            ast_imp = int(impressions * rng.uniform(0.3, 0.7))
            ast_clicks = int(ast_imp * rng.uniform(0.02, 0.08))
            ast_cost = round(cost * rng.uniform(0.1, 0.3), 2)
            records.append(("ad_asset", {
                "ad_id": "gad_001", "asset_id": asset_id, "date": date,
                "campaign_id": campaign_id, "campaign_name": "Mock Google Campaign",
                "adgroup_id": adgroup_id, "adgroup_name": "Mock Ad Group",
                "asset_name": asset_text or "Mock Image",
                "asset_type": asset_type, "asset_text": asset_text,
                "image_url": "https://example.com/img.png" if asset_type == "IMAGE" else "",
                "asset_performance": rng.choice(["BEST", "GOOD", "LOW", "UNSPECIFIED"]),
                "impressions": ast_imp, "clicks": ast_clicks,
                "ctr": round(ast_clicks / max(ast_imp, 1) * 100, 2),
                "all_conversions": rng.randint(0, 10), "cost": ast_cost,
                "account_id": account_id, "account_name": "Mock Google Account",
            }))

        for click_type in ["HEADLINE", "SITELINKS", "CALL"]:
            ct_imp = int(impressions * rng.uniform(0.2, 0.5))
            ct_clicks = int(ct_imp * rng.uniform(0.01, 0.06))
            ct_cost = round(cost * rng.uniform(0.1, 0.4), 2)
            records.append(("click_type", {
                "campaign_id": campaign_id, "date": date,
                "click_type": click_type, "campaign_name": "Mock Google Campaign",
                "campaign_status": "ENABLED",
                "impressions": ct_imp, "clicks": ct_clicks,
                "ctr": round(ct_clicks / max(ct_imp, 1) * 100, 2),
                "conversions": rng.randint(0, 5), "all_conversions": rng.randint(0, 8),
                "device": rng.choice(["MOBILE", "DESKTOP", "TABLET"]),
                "ad_network_type": rng.choice(["SEARCH", "DISPLAY"]),
                "cost": ct_cost,
                "account_id": account_id, "account_name": "Mock Google Account",
            }))

    return records


def publish(platform, days, seed):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topic = FB_TOPIC if platform == "facebook" else GOOGLE_TOPIC

    if platform == "facebook":
        records = generate_facebook_records(days=days, seed=seed)
    else:
        records = generate_google_records(days=days, seed=seed)

    for report_type, record in records:
        msg = _envelope(platform, report_type, record)
        producer.produce(topic, json.dumps(msg).encode())

    producer.flush()
    print(f"[stream_producer] Published {len(records)} records to {topic}")


def main():
    parser = argparse.ArgumentParser(description="Publish mock stream records to Kafka")
    parser.add_argument("--platform", required=True, choices=["facebook", "google"])
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    publish(args.platform, args.days, args.seed)


if __name__ == "__main__":
    main()

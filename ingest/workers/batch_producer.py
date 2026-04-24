# ingest/workers/batch_producer.py

import json
import os
import argparse
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
BATCH_TOPIC = "topic_batch_requests"


def publish_batch_request(platform, mode="mock", start_date=None, end_date=None, days=20, seed=None):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    message = {
        "platform": platform,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "seed": seed,
    }
    producer.produce(BATCH_TOPIC, json.dumps(message).encode())
    producer.flush()
    print(f"[batch_producer] Published to {BATCH_TOPIC}: {message}")


def main():
    parser = argparse.ArgumentParser(description="Publish a batch ingestion request to Kafka")
    parser.add_argument("--platform", required=True, choices=["facebook", "google"])
    parser.add_argument("--mode", default="mock", choices=["mock", "real"])
    parser.add_argument("--start-date", dest="start_date", default=None)
    parser.add_argument("--end-date", dest="end_date", default=None)
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--seed", default=None)
    args = parser.parse_args()

    publish_batch_request(
        platform=args.platform,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        days=args.days,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()

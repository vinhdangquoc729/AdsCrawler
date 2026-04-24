# ingest/workers/batch_consumer.py

import json
import os
import sys
from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
BATCH_TOPIC = "topic_batch_requests"
POLL_TIMEOUT = 10.0
MAX_EMPTY_POLLS = 3


def consume_and_ingest():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "batch_ingestion_worker",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([BATCH_TOPIC])

    processed = 0
    empty_polls = 0

    try:
        while empty_polls < MAX_EMPTY_POLLS:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            if msg is None:
                empty_polls += 1
                print(f"[batch_consumer] No message (poll {empty_polls}/{MAX_EMPTY_POLLS})")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                raise RuntimeError(f"Kafka error: {msg.error()}")

            empty_polls = 0
            request = json.loads(msg.value().decode())
            print(f"[batch_consumer] Processing: {request}")
            _dispatch(request)
            consumer.commit(msg)
            processed += 1
    finally:
        consumer.close()

    print(f"[batch_consumer] Done. Processed {processed} requests.")


def _dispatch(request):
    platform = request.get("platform")
    mode = request.get("mode", "mock")
    options = {k: v for k, v in request.items()
               if k not in ("platform", "mode") and v is not None}

    if platform == "facebook":
        from ingest.facebook.main import run_ingestion
    elif platform == "google":
        from ingest.google.main import run_ingestion
    else:
        print(f"[batch_consumer] Unknown platform: {platform}")
        return

    run_ingestion(
        mode=mode,
        start_date=options.get("start_date"),
        end_date=options.get("end_date"),
        options=options,
    )


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    consume_and_ingest()

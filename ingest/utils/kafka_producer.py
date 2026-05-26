# ingest/utils/kafka_producer.py

import json
import os
from confluent_kafka import Producer


class KafkaJsonProducer:
    """
    Kafka producer that sends JSON records to topic-per-template.
    Each record is sent as an individual JSON message (one message = one record).
    """

    def __init__(self, bootstrap_servers=None):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'
        )
        self._producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'linger.ms': 50,          # batch small messages for throughput
            'batch.num.messages': 500,
            'queue.buffering.max.messages': 100000,
        })
        self._delivery_errors = 0

    def _delivery_callback(self, err, msg):
        if err is not None:
            self._delivery_errors += 1
            print(f"   [KAFKA] Delivery failed for {msg.topic()}: {err}")

    @staticmethod
    def _enrich_partition_fields(record):
        """
        Inject year/month/day partition fields from the record's business date.
        Looks for 'date_start' (Facebook) or 'date' (Google) field.
        These fields are used by Kafka Connect FieldPartitioner to create
        the MinIO directory structure: {topic}/year=YYYY/month=MM/day=DD/
        """
        date_str = record.get("date_start") or record.get("date")
        if date_str and len(date_str) >= 10:
            # date_str format: "YYYY-MM-DD"
            record["year"] = date_str[:4]
            record["month"] = date_str[5:7]
            record["day"] = date_str[8:10]
        return record

    def produce(self, topic, records):
        """
        Send a list of dict records to the given Kafka topic.
        Each dict is serialized as a JSON string and sent as one message.
        Partition fields (year/month/day) are injected automatically.
        """
        if not records:
            return

        import datetime
        for record in records:
            enriched = self._enrich_partition_fields(record.copy())
            value = json.dumps(enriched, ensure_ascii=False).encode('utf-8')
            # Extract logical date for Kafka message timestamp
            # Looks for date_start, date, or stat_time_day
            date_str = enriched.get("date_start") or enriched.get("date") or enriched.get("stat_time_day")
            ts_ms = 0
            if date_str and len(date_str) >= 10:
                try:
                    dt = datetime.datetime.strptime(date_str[:10], "%Y-%m-%d")
                    ts_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    pass

            self._producer.produce(
                topic=topic,
                value=value,
                timestamp=ts_ms if ts_ms > 0 else 0,
                callback=self._delivery_callback,
            )
            # Serve delivery callbacks periodically to avoid buffer overflow
            self._producer.poll(0)

    def flush(self, timeout=30):
        """Flush all buffered messages. Call this at the end of ingestion."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            print(f"   [KAFKA] WARNING: {remaining} messages still in queue after flush")
        if self._delivery_errors > 0:
            print(f"   [KAFKA] WARNING: {self._delivery_errors} delivery errors occurred")
        return self._delivery_errors

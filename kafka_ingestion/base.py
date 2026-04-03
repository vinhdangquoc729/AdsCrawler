# kafka_ingestion/base.py

import json
from datetime import datetime
import os
from kafka import KafkaProducer


class BaseIngestor:
    """Base class for data ingestion. May be extended for different sources."""
    def __init__(self, crawling_type, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.crawling_type = crawling_type

    def send_to_kafka(self, topic, data, platform, report_type):
        """Send data with metadata so downstream consumers can route."""
        payload = {
            "metadata": {
                "platform": platform,
                "report_type": report_type,
                "ingested_at": datetime.now().isoformat(),
                "crawling_type": self.crawling_type
            },
            "data": data
        }
        self.producer.send(topic, value=payload)

    def flush(self):
        self.producer.flush()

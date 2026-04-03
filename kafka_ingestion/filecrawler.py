# kafka_ingestion/filecrawler.py

import os
import json
from base import BaseIngestor

class FileCrawler(BaseIngestor):
    """FileCrawler implementation that reads JSON files and sends records to Kafka."""

    def __init__(self, crawling_type='file', bootstrap_servers=None):
        super().__init__(crawling_type, bootstrap_servers)

    def process_folder(self, folder_name, config_list):
        """
        folder_name: Folder containing JSON files
        config_list: List of dictionaries containing {file_name, topic, platform, report_type}
        """
        for config in config_list:
            file_path = os.path.join(folder_name, config['file_name'])
            if not os.path.exists(file_path):
                print(f"[WARN] File not found: {file_path}")
                continue

            print(f"--- Processing: {config['file_name']} ({config['platform']}) ---")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for record in data:
                    self.send_to_kafka(
                        topic=config['topic'],
                        data=record,
                        platform=config['platform'],
                        report_type=config['report_type']
                    )
            self.flush()
            print(f"--- Completed {config['file_name']} ---\n")

# kafka_ingestion/main.py

from filecrawler import FileCrawler

def main():
    crawler = FileCrawler()
    job_configs = [
        {
            "file_name": "Ad Daily Report.json",
            "topic": "topic_fb_raw",
            "platform": "facebook",
            "report_type": "ad_daily"
        },
        {
            "file_name": "Ad Creative Report.json",
            "topic": "topic_fb_raw",
            "platform": "facebook",
            "report_type": "ad_creative"
        },
        {
            "file_name": "AGE & GENDER_DETAILED_REPORT.json",
            "topic": "topic_fb_raw",
            "platform": "facebook",
            "report_type": "age_gender"
        }
    ]
    crawler.process_folder('sample_data', job_configs)
    print(">>> Ingested all files into Kafka.")

if __name__ == "__main__":
    main()

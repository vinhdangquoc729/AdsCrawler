# kafka_ingestion/run_mock.py

import os
import argparse
from datetime import datetime, timedelta
from mock_generator import MockGenerator

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xlsx", action="store_true",
                        help="Export mock_data_report.xlsx after generation (uses extra RAM)")
    parser.add_argument("--days", type=int, default=20,
                        help="Number of past days to generate (default: 20)")
    args = parser.parse_args()

    end_date   = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    seed = "mkt_bigdata_seed_2026"

    print(f">>> Bắt đầu sinh dữ liệu ảo DETERMINISTIC (Seed: {seed})")
    print(f">>> Đẩy vào MinIO từ {start_date} đến {end_date}...")

    generator = MockGenerator(
        endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9005'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'admin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'password123'),
        enable_xlsx_buffer=args.xlsx,
    )

    options = {
        "accountCount": 2,
        "campaignCount": 5,
        "adsetPerCampaign": 5,
        "adPerAdset": 5,
        "seed": seed
    }

    try:
        generator.generate_consistent_suite(start_date, end_date, options=options)

        if args.xlsx:
            print(">>> Hoàn tất sinh dữ liệu. Đang xuất file Excel...")
            generator.export_to_xlsx("mock_data_report.xlsx")

        print(">>> SUCCESS: Dữ liệu đã sẵn sàng trên MinIO")
    except Exception as e:
        print(f"!!! Lỗi khi sinh dữ liệu: {e}")

if __name__ == "__main__":
    main()

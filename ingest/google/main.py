# ingest/google/main.py

import os
import argparse
from datetime import datetime, timedelta
from .mock import MockGenerator

def run_ingestion(mode="mock", start_date=None, end_date=None, options=None):
    options = options or {}

    # Default date configuration: last 20 days if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        days = options.get('days', 20)
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    if mode == "mock":
        seed = options.get('seed', "google_mkt_seed_2026")
        print(f">>> Bắt đầu sinh dữ liệu ảo Google DETERMINISTIC (Seed: {seed})")
        print(f">>> Đẩy vào MinIO từ {start_date} đến {end_date}...")

        generator = MockGenerator(
            endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9005'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'admin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'password123'),
            enable_xlsx_buffer=options.get('xlsx', False)
        )

        try:
            generator.generate_consistent_suite(start_date, end_date, options={"seed": seed, **options})

            if options.get('xlsx'):
                print(">>> Hoàn tất sinh dữ liệu. Đang xuất file Excel...")
                generator.export_to_xlsx("google_mock_report.xlsx")

            print(">>> SUCCESS: Dữ liệu Google đã sẵn sàng trên MinIO")
        except Exception as e:
            print(f"!!! Lỗi khi sinh dữ liệu Google: {e}")
            import traceback
            traceback.print_exc()

    elif mode == "real":
        print(">>> Bắt đầu lấy dữ liệu THẬT từ Google Ads API (Not implemented yet)...")
        # crawler = GoogleCrawler()
        # crawler.fetch_real_data(start_date, end_date)

    else:
        print(f"!!! Mode '{mode}' không hợp lệ. Sử dụng 'mock' hoặc 'real'.")

def main():
    parser = argparse.ArgumentParser(description="Google Ads Ingestion Tool")
    parser.add_argument("--mode", type=str, default="mock", choices=["mock", "real"], help="Ingestion mode")
    parser.add_argument("--xlsx", action="store_true", default=False, help="Export mock data to Excel")
    parser.add_argument("--days", type=int, default=20, help="Days of data to generate")
    parser.add_argument("--start-date", "--date-start", type=str, dest="start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", "--date-stop", type=str, dest="end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--seed", type=str, default="google_mkt_seed_2026", help="Seed for mock data")

    args = parser.parse_args()

    options = {
        "xlsx": args.xlsx,
        "days": args.days,
        "seed": args.seed,
        "accountCount": 2,
        "campaignCount": 3,
        "adGroupCount": 3,
        "adCount": 2,
        "assetCount": 5
    }

    run_ingestion(mode=args.mode, start_date=args.start_date, end_date=args.end_date, options=options)

if __name__ == "__main__":
    main()

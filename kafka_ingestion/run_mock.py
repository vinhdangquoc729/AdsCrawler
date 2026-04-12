# kafka_ingestion/run_mock.py

import os
from datetime import datetime, timedelta
from mock_generator import MockGenerator

def main():
    # Cấu hình ngày mặc định: 3 ngày gần nhất
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    seed = "mkt_bigdata_seed_2026"

    print(f">>> Bắt đầu sinh dữ liệu ảo DETERMINISTIC (Seed: {seed})")
    print(f">>> Đẩy vào MinIO từ {start_date} đến {end_date}...")

    # Cấu hình MinIO (có thể lấy từ biến môi trường)
    generator = MockGenerator(
        endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9005'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'admin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'password123')
    )

    options = {
        "accountCount": 3,
        "campaignCount": 20,
        "adsetPerCampaign": 20,
        "adPerAdset": 20, 
        "seed": seed
    }

    try:
        generator.generate_consistent_suite(start_date, end_date, options=options)
        print(">>> Hoàn tất sinh dữ liệu. Đang xuất file Excel...")
        
        # Xuất file Excel để kiểm tra
        generator.export_to_xlsx("mock_data_report.xlsx")
        
        print(">>> SUCCESS: Dữ liệu đã sẵn sàng trên MinIO")
    except Exception as e:
        print(f"!!! Lỗi khi sinh dữ liệu: {e}")

if __name__ == "__main__":
    main()

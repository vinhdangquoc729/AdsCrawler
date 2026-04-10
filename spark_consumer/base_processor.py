# spark_consumer/base_processor.py

class BaseDataProcessor:
    """Base class for writing processed data to ClickHouse."""
    def __init__(self, spark_session):
        self.spark = spark_session
        self.jdbc_url = "jdbc:clickhouse://clickhouse:8123/marketing_db?ssl=false&compress=false"
        self.jdbc_props = {
            "user": "admin",
            "password": "password123",
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "isolationLevel": "NONE"
        }

    def write_to_clickhouse(self, df, table_name):
        if df is not None and not df.isEmpty():
            try:
                df.write.jdbc(
                    url=self.jdbc_url,
                    table=table_name,
                    mode="append",
                    properties=self.jdbc_props
                )
                print(f"  [+] Đã ghi thành công vào bảng {table_name} ({df.count()} dòng)")
            except Exception as e:
                print(f"  [!] LỖI GHI BẢNG {table_name}: {str(e)}")

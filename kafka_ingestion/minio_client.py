# kafka_ingestion/minio_client.py

import io
import json
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error

class MinioClient:
    def __init__(self, endpoint=None, access_key=None, secret_key=None, secure=False):
        self.endpoint = endpoint or os.getenv('MINIO_ENDPOINT', 'localhost:9005')
        self.access_key = access_key or os.getenv('MINIO_ACCESS_KEY', 'admin')
        self.secret_key = secret_key or os.getenv('MINIO_SECRET_KEY', 'password123')
        self.secure = secure
        
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        self.bucket_name = os.getenv('MINIO_BUCKET', 'marketing-datalake')

    def ensure_bucket(self):
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                print(f"Bucket '{self.bucket_name}' created.")
            return True
        except S3Error as e:
            print(f"Error ensuring bucket: {e}")
            return False

    def upload_json(self, table_name, data, user_id="admin"):
        """
        Uploads data to MinIO in structured path:
        <tableName>/<YYYY>/<MM>/<DD>/<HH>/<mm>/<userId>_<timestamp>_<uuid>.json
        """
        self.ensure_bucket()
        
        import uuid
        now = datetime.now()
        unique_id = uuid.uuid4().hex[:8]
        
        path = (
            f"{table_name}/"
            f"{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{now.hour:02d}/{now.minute:02d}/"
            f"{user_id}_{int(now.timestamp())}_{unique_id}.json"
        )
        
        try:
            json_data = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            data_stream = io.BytesIO(json_data)
            
            self.client.put_object(
                self.bucket_name,
                path,
                data_stream,
                length=len(json_data),
                content_type='application/json'
            )
            return {"success": True, "path": path}
        except Exception as e:
            print(f"Error uploading to MinIO: {e}")
            return {"success": False, "error": str(e)}

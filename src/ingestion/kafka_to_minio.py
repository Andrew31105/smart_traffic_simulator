
import json
import time
import uuid
import os
import sys
from datetime import datetime
from confluent_kafka import Consumer, KafkaException

import boto3
from botocore.client import Config

# Đảm bảo script tìm thấy module src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.config import settings
from src.utils.logger_utils import get_logger

# Khởi tạo logger để theo dõi hoạt động
logger = get_logger("kafka_to_minio")

class KafkaToMinIO:
    def __init__(self):
        # 1. Khởi tạo MinIO Client (S3 Compatible)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{settings.minio.endpoint}",
            aws_access_key_id=settings.minio.access_key,
            aws_secret_access_key=settings.minio.secret_key,
            config=Config(signature_version='s3v4')
        )
        self.bucket_name = settings.minio.bucket_raw
        
        # 2. Cấu hình Kafka Consumer
        conf = {
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'group.id': settings.kafka.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(conf)
        self.topic = settings.kafka.topic_raw

        # 3. Cấu hình Batching (Lấy từ config hoặc mặc định)
        self.batch_size = 100   # Gom 100 tin nhắn
        self.batch_timeout = 60 # Hoặc quá 60 giây
        
        self.buffer = []
        self.last_flush_time = time.time()

    def _ensure_bucket(self):
        """Đảm bảo bucket tồn tại trước khi ghi."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except:
            logger.info(f"Tạo bucket mới: {self.bucket_name}")
            self.s3_client.create_bucket(Bucket=self.bucket_name)

    def flush(self):
        """Thực hiện 'xả xô' - Đẩy dữ liệu từ RAM lên MinIO."""
        if not self.buffer:
            return

        try:
            now = datetime.now()
            # Tổ chức Path theo Hive Partitioning
            partition_path = now.strftime("year=%Y/month=%m/day=%d/hour=%H")
            
            # Tạo tên file duy nhất: batch_timestamp_uuid.json
            unique_id = uuid.uuid4().hex[:8]
            file_name = f"batch_{int(time.time())}_{unique_id}.json"
            file_key = f"{partition_path}/{file_name}"

            # Chuyển list sang định dạng NDJSON (mỗi dòng 1 JSON)
            ndjson_data = "\n".join([json.dumps(msg) for msg in self.buffer])

            # Upload lên MinIO
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=ndjson_data
            )

            logger.info(f"Đã lưu {len(self.buffer)} bản ghi vào: {file_key}")
            
            # Reset buffer và đồng hồ
            self.buffer = []
            self.last_flush_time = time.time()
            
        except Exception as e:
            logger.error(f"❌ Lỗi khi flush dữ liệu lên MinIO: {e}")

    def run(self):
        """Vòng lặp chính: Nghe Kafka và điều phối việc Flush."""
        self._ensure_bucket()
        self.consumer.subscribe([self.topic])
        logger.info(f"Bắt đầu Ingestion: {self.topic} -> MinIO ({self.bucket_name})")

        try:
            while True:
                # 1. Thò tay lấy tin nhắn (đợi tối đa 1s)
                msg = self.consumer.poll(1.0)

                if msg is not None:
                    if msg.error():
                        logger.error(f"Kafka Error: {msg.error()}")
                    else:
                        # Parse dữ liệu và đưa vào xô (buffer)
                        try:
                            data = json.loads(msg.value().decode('utf-8'))
                            self.buffer.append(data)
                        except json.JSONDecodeError:
                            logger.warning("Nhận được tin nhắn không phải JSON, bỏ qua.")

                # 2. Kiểm tra điều kiện xả xô (Flush)
                current_time = time.time()
                if len(self.buffer) >= self.batch_size or \
                   (current_time - self.last_flush_time >= self.batch_timeout and self.buffer):
                    self.flush()

        except KeyboardInterrupt:
            logger.info("Dừng script theo yêu cầu người dùng...")
        finally:
            # Lưu nốt những gì còn sót lại trước khi tắt
            if self.buffer:
                self.flush()
            self.consumer.close()

if __name__ == "__main__":
    ingestor = KafkaToMinIO()
    ingestor.run()
"""
kafka_to_elasticsearch.py - Đổ dữ liệu đã xử lý từ Kafka vào Elasticsearch.


Consume dữ liệu giao thông đã qua xử lý (output của Flink Speed Layer)
từ Kafka topic "traffic-processed", transform và bulk index vào Elasticsearch
để phục vụ trực quan hoá real-time trên Kibana.


Data flow:
   traffic-raw → [Flink Speed Layer] → traffic-processed → [Module này] → Elasticsearch
"""


import json
import signal
import sys
import os
from datetime import datetime, timezone


from confluent_kafka import Consumer, KafkaError, KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# Thêm project root vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.config import settings
from src.utils.logger_utils import get_logger


logger = get_logger("kafka_to_elasticsearch", log_to_file=True)


# =========================================================
# Cấu hình
# =========================================================
BATCH_SIZE = 50          # Số document gom lại trước khi bulk index
POLL_TIMEOUT = 1.0       # Giây chờ mỗi lần poll Kafka
FLUSH_INTERVAL = 10      # Giây tối đa trước khi flush batch dù chưa đủ BATCH_SIZE


# =========================================================
# Index Mapping cho Elasticsearch
# =========================================================
TRAFFIC_INDEX_MAPPING = {
   "settings": {
       "number_of_shards": 1,
       "number_of_replicas": 0,
       "refresh_interval": "5s",
   },
   "mappings": {
       "properties": {
           # --- Thông tin cảm biến ---
           "sensor_id": {"type": "keyword"},
           "sensor_name": {"type": "keyword"},
           "location": {"type": "geo_point"},


           # --- Metrics tốc độ ---
           "avg_speed": {"type": "float"},
           "min_speed": {"type": "float"},
           "max_speed": {"type": "float"},


           # --- Metrics lưu lượng ---
           "total_vehicle_count": {"type": "integer"},
           "sample_count": {"type": "integer"},


           # --- Tình trạng tắc nghẽn ---
           "is_congested": {"type": "boolean"},
           "congestion_level": {"type": "keyword"},


           # --- Thời gian ---
           "window_start": {"type": "date"},
           "window_end": {"type": "date"},
           "processed_at": {"type": "date"},
           "indexed_at": {"type": "date"},
       }
   },
}




class ElasticsearchSink:
   """
   Consumer Kafka → Elasticsearch.


   Đọc message từ topic "traffic-processed", gom batch,
   rồi bulk index vào Elasticsearch index "traffic-realtime".
   """


   def __init__(self):
       self._running = True


       # --- Elasticsearch client ---
       self.es = Elasticsearch(
           settings.elasticsearch.url,
           request_timeout=30,
           max_retries=3,
           retry_on_timeout=True,
       )
       self.index_name = settings.elasticsearch.index_traffic
       logger.info(f"Kết nối Elasticsearch tại {settings.elasticsearch.url}")


       # --- Kafka consumer ---
       consumer_conf = {
           "bootstrap.servers": settings.kafka.bootstrap_servers,
           "group.id": "es-sink-group",
           "auto.offset.reset": "latest",
           "enable.auto.commit": True,
           "auto.commit.interval.ms": 5000,
       }
       self.consumer = Consumer(consumer_conf)
       logger.info(
           f"Kafka consumer: group=es-sink-group | "
           f"topic={settings.kafka.topic_processed}"
       )


       # --- Buffer cho batch indexing ---
       self._buffer = []
       self._last_flush_time = datetime.now()


   # ---------------------------------------------------------
   # Tạo index nếu chưa tồn tại
   # ---------------------------------------------------------
   def create_index_if_not_exists(self):
       """Tạo Elasticsearch index với mapping phù hợp nếu chưa có."""
       if self.es.indices.exists(index=self.index_name):
           logger.info(f"Index '{self.index_name}' đã tồn tại.")
           return


       self.es.indices.create(index=self.index_name, body=TRAFFIC_INDEX_MAPPING)
       logger.info(
           f"Đã tạo index '{self.index_name}' với mapping "
           f"(geo_point, keyword, float, date...)"
       )


   # ---------------------------------------------------------
   # Transform document
   # ---------------------------------------------------------
   @staticmethod
   def _transform_document(data: dict) -> dict:
       """
       Transform dữ liệu từ Kafka sang format Elasticsearch.


       Chuyển lat/lon thành geo_point để Kibana có thể hiển thị bản đồ,
       và thêm timestamp indexed_at.
       """
       doc = {
           "sensor_id": data["sensor_id"],
           "sensor_name": data["sensor_name"],


           # Chuyển lat/lon sang geo_point format
           "location": {
               "lat": float(data["lat"]),
               "lon": float(data["lon"]),
           },


           # Metrics
           "avg_speed": float(data["avg_speed"]),
           "min_speed": float(data["min_speed"]),
           "max_speed": float(data["max_speed"]),
           "total_vehicle_count": int(data["total_vehicle_count"]),
           "sample_count": int(data["sample_count"]),


           # Tắc nghẽn
           "is_congested": bool(data["is_congested"]),
           "congestion_level": data["congestion_level"],


           # Thời gian
           "window_start": data["window_start"],
           "window_end": data["window_end"],
           "processed_at": data["processed_at"],
           "indexed_at": datetime.now(timezone.utc).isoformat(),
       }
       return doc


   # ---------------------------------------------------------
   # Flush buffer → Elasticsearch
   # ---------------------------------------------------------
   def _flush_buffer(self):
       """Bulk index toàn bộ buffer vào Elasticsearch."""
       if not self._buffer:
           return


       actions = [
           {
               "_index": self.index_name,
               "_source": doc,
           }
           for doc in self._buffer
       ]


       try:
           success, errors = bulk(self.es, actions, raise_on_error=False)
           if errors:
               logger.warning(f"Bulk index có {len(errors)} lỗi: {errors[:3]}")
           else:
               logger.info(
                   f"Bulk index thành công {success} documents "
                   f"vào '{self.index_name}'"
               )
       except Exception as e:
           logger.error(f"Bulk index thất bại: {e}", exc_info=True)
       finally:
           self._buffer.clear()
           self._last_flush_time = datetime.now()


   # ---------------------------------------------------------
   # Kiểm tra cần flush theo thời gian
   # ---------------------------------------------------------
   def _should_time_flush(self) -> bool:
       """Kiểm tra đã quá FLUSH_INTERVAL chưa."""
       elapsed = (datetime.now() - self._last_flush_time).total_seconds()
       return elapsed >= FLUSH_INTERVAL


   # ---------------------------------------------------------
   # Main loop
   # ---------------------------------------------------------
   def run(self):
       """
       Vòng lặp chính: consume Kafka → gom batch → bulk index ES.


       Dừng bằng Ctrl+C hoặc SIGTERM.
       """
       # Đăng ký signal handler để graceful shutdown
       signal.signal(signal.SIGINT, self._signal_handler)
       signal.signal(signal.SIGTERM, self._signal_handler)


       # Kiểm tra kết nối ES
       if not self.es.ping():
           logger.error("Không thể kết nối tới Elasticsearch!")
           raise ConnectionError("Elasticsearch không khả dụng")


       logger.info("Elasticsearch ping OK ✓")


       # Tạo index nếu chưa có
       self.create_index_if_not_exists()


       # Subscribe Kafka topic
       self.consumer.subscribe([settings.kafka.topic_processed])
       logger.info(
           f"Bắt đầu consume từ topic '{settings.kafka.topic_processed}' "
           f"→ ES index '{self.index_name}'"
       )


       try:
           while self._running:
               msg = self.consumer.poll(timeout=POLL_TIMEOUT)


               if msg is None:
                   # Không có message mới, kiểm tra time flush
                   if self._should_time_flush():
                       self._flush_buffer()
                   continue


               if msg.error():
                   if msg.error().code() == KafkaError._PARTITION_EOF:
                       logger.debug(
                           f"Đã đọc hết partition {msg.partition()} "
                           f"offset {msg.offset()}"
                       )
                   else:
                       raise KafkaException(msg.error())
                   continue


               # Parse message
               try:
                   raw_value = msg.value().decode("utf-8")
                   data = json.loads(raw_value)
                   doc = self._transform_document(data)
                   self._buffer.append(doc)


                   logger.debug(
                       f"Buffered: {doc['sensor_name']} | "
                       f"speed={doc['avg_speed']} | "
                       f"congested={doc['is_congested']}"
                   )
               except (json.JSONDecodeError, KeyError) as e:
                   logger.warning(f"Bỏ qua message lỗi: {e}")
                   continue


               # Flush nếu đủ batch size
               if len(self._buffer) >= BATCH_SIZE:
                   self._flush_buffer()


               # Flush theo thời gian
               if self._should_time_flush():
                   self._flush_buffer()


       except KafkaException as e:
           logger.error(f"Kafka consumer lỗi: {e}", exc_info=True)
           raise
       finally:
           # Flush buffer còn lại trước khi thoát
           self._flush_buffer()
           self.consumer.close()
           self.es.close()
           logger.info("Đã đóng kết nối Kafka consumer và Elasticsearch client.")


   # ---------------------------------------------------------
   # Graceful Shutdown
   # ---------------------------------------------------------
   def _signal_handler(self, signum, frame):
       """Xử lý tín hiệu dừng (SIGINT/SIGTERM)."""
       logger.info(f"Nhận tín hiệu {signum}, đang dừng gracefully...")
       self._running = False




# =========================================================
# Entrypoint
# =========================================================
if __name__ == "__main__":
   logger.info("=" * 60)
   logger.info("Khởi động Kafka → Elasticsearch Sink")
   logger.info(f"  ES URL  : {settings.elasticsearch.url}")
   logger.info(f"  ES Index: {settings.elasticsearch.index_traffic}")
   logger.info(f"  Kafka   : {settings.kafka.topic_processed}")
   logger.info(f"  Batch   : {BATCH_SIZE} docs | Flush: {FLUSH_INTERVAL}s")
   logger.info("=" * 60)


   try:
       sink = ElasticsearchSink()
       sink.run()
   except ConnectionError as e:
       logger.error(f"Lỗi kết nối: {e}")
       sys.exit(1)
   except Exception as e:
       logger.error(f"Lỗi không mong đợi: {e}", exc_info=True)
       sys.exit(1)




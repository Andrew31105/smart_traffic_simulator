import json
import time
import os
import sys
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utils.config import settings
from src.utils.logger_utils import get_logger
logger = get_logger("HanoiTrafficProducer")



class TrafficExtractor:
    def __init__(self):
        conf = {
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'client.id': 'traffic_producer',
            'retries': 5,
            'acks': 'all'
        }

        self.producer = Producer(conf)
        self.topic = settings.kafka.topic_raw

        self.api_key = settings.tomtom.api_key
        self.flow_url = settings.tomtom.flow_segment_url.format(base_url=settings.tomtom.base_url)
        self.timeout = settings.tomtom.request_timeout
        self.locations = settings.traffic.locations
        self.fetch_interval = settings.traffic.fetch_interval

        if not self.api_key:
            raise ValueError("Missing TOMTOM_API_KEY in environment")

        logger.info("HanoiTrafficExtractor initialized successfully.")

    def delivery_report(self, err, msg):
        """Callback kiểm tra việc gửi tin nhắn lên Kafka."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Sent to Kafka | Topic: {msg.topic()} | Partition: {msg.partition()}")

    def fetch_flow_segment(self, location_name: str, point: str):
        """Lấy dữ liệu tốc độ theo điểm từ TomTom Flow Segment API."""
        params = {
            "point": point,
            "unit": "KMPH",
            "key": self.api_key,
        }

        try:
            response = requests.get(self.flow_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            logger.error(f"TomTom request failed for {location_name}: {exc}")
            return None
        except ValueError:
            logger.error(f"TomTom returned invalid JSON for {location_name}")
            return None

        flow_segment = data.get("flowSegmentData", {})
        payload = {
            "source": "tomtom",
            "location_name": location_name,
            "point": point,
            "current_speed": flow_segment.get("currentSpeed"),
            "free_flow_speed": flow_segment.get("freeFlowSpeed"),
            "current_travel_time": flow_segment.get("currentTravelTime"),
            "free_flow_travel_time": flow_segment.get("freeFlowTravelTime"),
            "confidence": flow_segment.get("confidence"),
            "road_closure": flow_segment.get("roadClosure"),
            "frc": flow_segment.get("frc"),
            "raw": data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return payload

    def produce_once(self):
        """Lấy dữ liệu cho tất cả điểm cấu hình và gửi vào Kafka."""
        sent_count = 0
        for location_name, point in self.locations.items():
            payload = self.fetch_flow_segment(location_name, point)
            if not payload:
                continue

            try:
                self.producer.produce(
                    self.topic,
                    key=location_name,
                    value=json.dumps(payload).encode("utf-8"),
                    callback=self.delivery_report,
                )
                self.producer.poll(0)
                sent_count += 1
            except BufferError as exc:
                logger.warning(f"Kafka local queue full, flushing and retrying: {exc}")
                self.producer.flush(5)

        self.producer.flush(10)
        logger.info(f"Published {sent_count}/{len(self.locations)} records to Kafka")

    def run(self):
        logger.info(
            f"Starting TomTom producer | topic={self.topic} | interval={self.fetch_interval}s"
        )
        try:
            while True:
                self.produce_once()
                time.sleep(self.fetch_interval)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            self.producer.flush(10)


if __name__ == "__main__":
    extractor = TrafficExtractor()
    extractor.run()

            
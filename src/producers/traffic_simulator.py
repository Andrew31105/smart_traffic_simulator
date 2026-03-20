import json
import time
import random
import sys
import os
from datetime import datetime,timezone
from confluent_kafka import Producer

# Thêm project root vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.config import settings

# Lấy cấu hình từ config.py
KAFKA_BOOTSTRAP_SERVERS = settings.kafka.bootstrap_servers
TOPIC_NAME = settings.kafka.topic_raw

# Danh sách các trạm cảm biến giả lập tại Hà Nội
SENSORS = [
    {"id": "SENSOR_01", "name": "Nga Tu So", "lat": 21.0031, "lon": 105.8201},
    {"id": "SENSOR_02", "name": "Cau Giay", "lat": 21.0360, "lon": 105.7941},
    {"id": "SENSOR_03", "name": "Ba Dinh", "lat": 21.0333, "lon": 105.8333},
    {"id": "SENSOR_04", "name": "Hoan Kiem", "lat": 21.0285, "lon": 105.8542},
    {"id": "SENSOR_05", "name": "Ha Dong", "lat": 20.9723, "lon": 105.7744}
]

conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def generate_traffic_data():
    while True:
        for sensor in SENSORS:
            # Giả lập logic: Tốc độ thường từ 20-60km/h, thỉnh thoảng giảm mạnh (tắc đường)
            is_congested = random.random() < 0.2  # 20% xác suất tắc đường
            speed = random.uniform(5, 15) if is_congested else random.uniform(25, 60)
            
            payload = {
                "sensor_id": sensor["id"],
                "sensor_name": sensor["name"],
                "location": {"lat": sensor["lat"], "lon": sensor["lon"]},
                "current_speed": round(speed, 2),
                "vehicle_count": random.randint(10, 100),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Gửi dữ liệu vào Kafka
            producer.produce(
                TOPIC_NAME, 
                key=sensor["id"], 
                value=json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
        
        producer.flush()
        print(f"Đã gửi {len(SENSORS)} bản tin giao thông lúc {datetime.now()}")
        time.sleep(2) # Gửi dữ liệu mỗi 2 giây

if __name__ == "__main__":
    print("Đang khởi động Traffic Simulator...")
    try:
        generate_traffic_data()
    except KeyboardInterrupt:
        print("\n Đã dừng Traffic Simulator.")
        producer.flush()
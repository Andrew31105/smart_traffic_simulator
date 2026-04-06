
import json
import sys
import os
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage

# Thêm project root vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.config import settings
from src.utils.logger_utils import get_logger

logger = get_logger("flink_speed_layer", log_to_file=True)
CONGESTION_RATIO_THRESHOLD = settings.traffic.congestion_threshold
WINDOW_SIZE_SECONDS = 30

class ParseTrafficData(MapFunction):
    """Parse raw JSON theo schema của TomTom producer."""

    def map(self, value: str):
        try:
            data = json.loads(value)

            location_name = data["location_name"]
            point = data["point"]
            current_speed = float(data["current_speed"])
            free_flow_speed = float(data["free_flow_speed"])

            p_lat, p_lon = point.split(",", 1)
            lat = float(p_lat.strip())
            lon = float(p_lon.strip())

            sensor_id = location_name.strip().upper().replace(" ", "_")

            return json.dumps({
                "sensor_id": sensor_id,
                "sensor_name": location_name,
                "lat": lat,
                "lon": lon,
                "current_speed": current_speed,
                "free_flow_speed": free_flow_speed,
                "timestamp": data.get("timestamp"),
            })
        except Exception as e:
            logger.error(f"Failed to parse TomTom payload: {e}")
            return None
def classify_congestion(avg_speed: float, congestion_ratio: float | None) -> str:
    """Phân loại mức độ tắc nghẽn dựa trên tỉ lệ speed/free-flow."""
    if congestion_ratio is not None:
        if congestion_ratio < 0.4:
            return "SEVERE"
        if congestion_ratio < 0.65:
            return "MODERATE"
        if congestion_ratio < 0.85:
            return "LIGHT"
        return "FREE_FLOW"

    # Fallback theo tốc độ tuyệt đối nếu thiếu free-flow.
    if avg_speed < 10:
        return "SEVERE"       # Tắc nghẽn nghiêm trọng
    elif avg_speed < 20:
        return "MODERATE"     # Tắc nghẽn trung bình
    elif avg_speed < 35:
        return "LIGHT"        # Đông đúc nhẹ
    else:
        return "FREE_FLOW"    # Thông thoáng


class TrafficWindowProcessor(ProcessWindowFunction):
    """
    Xử lý cửa sổ: tính tốc độ trung bình,
    và đánh dấu tắc nghẽn cho mỗi sensor.
    """

    def process(self, key, context, elements):
        records = [json.loads(e) for e in elements]
        if not records:
            return

        sample_count = len(records)
        total_speed = sum(r["current_speed"] for r in records)
        free_flow_values = [r.get("free_flow_speed") for r in records if r.get("free_flow_speed") is not None]

        avg_speed = round(total_speed / sample_count, 2)
        min_speed = round(min(r["current_speed"] for r in records), 2)
        max_speed = round(max(r["current_speed"] for r in records), 2)
        avg_free_flow_speed = round(sum(free_flow_values) / len(free_flow_values), 2) if free_flow_values else None

        congestion_ratio = None
        if avg_free_flow_speed and avg_free_flow_speed > 0:
            congestion_ratio = round(avg_speed / avg_free_flow_speed, 3)

        is_congested = congestion_ratio is not None and congestion_ratio < CONGESTION_RATIO_THRESHOLD

        result = {
            "sensor_id": key,
            "sensor_name": records[0]["sensor_name"],
            "lat": records[0]["lat"],
            "lon": records[0]["lon"],
            "avg_speed": avg_speed,
            "min_speed": min_speed,
            "max_speed": max_speed,
            "avg_free_flow_speed": avg_free_flow_speed,
            "congestion_ratio": congestion_ratio,
            "total_vehicle_count": 0,
            "sample_count": sample_count,
            "is_congested": is_congested,
            "congestion_level": classify_congestion(avg_speed, congestion_ratio),
            "window_start": datetime.fromtimestamp(
                context.window().start / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "window_end": datetime.fromtimestamp(
                context.window().end / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        status = "CONGESTED" if is_congested else "NORMAL"
        logger.info(
            f"[{result['sensor_name']}] {status} | "
            f"Avg: {avg_speed} km/h | Ratio: {congestion_ratio} | "
            f"Window: {result['window_start']} → {result['window_end']}"
        )

        yield json.dumps(result)
def build_pipeline():
    """Xây dựng và chạy Flink streaming pipeline."""

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    # --- Load JAR connector ---
    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_jar = f"file://{os.path.join(current_dir, 'jars', 'flink-sql-connector-kafka-3.0.1-1.18.jar')}"
    env.add_jars(kafka_jar)

    # --- Checkpoint ---
    env.enable_checkpointing(10000)
    env.get_checkpoint_config().set_checkpoint_storage(
        FileSystemCheckpointStorage("file:///tmp/flink-checkpoints")
    )

    logger.info("Đã khởi tạo StreamExecutionEnvironment với JAR và Checkpoint.")

    # --- Kafka Source ---
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka.bootstrap_servers)
        .set_topics(settings.kafka.topic_raw)
        .set_group_id(settings.kafka.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # --- Kafka Sink ---
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(settings.kafka.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(settings.kafka.topic_processed)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # --- Pipeline ---
    # 1) Đọc từ Kafka
    ds = env.from_source(
        kafka_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        "KafkaTrafficSource",
    )

    # 2) Parse JSON
    parsed = (
        ds.map(ParseTrafficData(), output_type=Types.STRING())
          .filter(lambda x: x is not None)
    )

    # 3) Key by sensor_id → Window 30s → Aggregate
    aggregated = (
        parsed
        .key_by(lambda x: json.loads(x)["sensor_id"])
        .window(
            TumblingProcessingTimeWindows.of(
                Time.seconds(WINDOW_SIZE_SECONDS)
            )
        )
        .process(TrafficWindowProcessor(), output_type=Types.STRING())
    )

    # 4) Ghi vào Kafka
    aggregated.sink_to(kafka_sink)

    # 5) Debug console
    aggregated.print()

    logger.info(
        f"Pipeline sẵn sàng | "
        f"Source: {settings.kafka.topic_raw} | "
        f"Sink: {settings.kafka.topic_processed} | "
        f"Window: {WINDOW_SIZE_SECONDS}s"
    )

    env.execute("SmartTrafficSpeedLayer")

if __name__ == "__main__":
    logger.info("Khởi động Flink Speed Layer...")
    try:
        build_pipeline()
    except KeyboardInterrupt:
        logger.info("Đã dừng Flink Speed Layer.")
    except Exception as e:
        logger.error(f"Pipeline lỗi: {e}", exc_info=True)
        raise
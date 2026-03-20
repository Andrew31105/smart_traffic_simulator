"""
config.py - Quản lý cấu hình & Secret
Đọc biến môi trường từ file .env và cung cấp cho toàn bộ dự án.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()


@dataclass(frozen=True)
class TomTomConfig:
    """Cấu hình TomTom API."""
    api_key: str = field(default_factory=lambda: os.getenv("TOMTOM_API_KEY", ""))
    base_url: str = "https://api.tomtom.com"
    flow_segment_url: str = "{base_url}/traffic/services/4/flowSegmentData/absolute/10/json"
    traffic_incidents_url: str = "{base_url}/traffic/services/5/incidentDetails"
    request_timeout: int = 30  # giây


@dataclass(frozen=True)
class KafkaConfig:
    """Cấu hình Kafka."""
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_raw: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_TRAFFIC_RAW", "traffic-raw")
    )
    topic_processed: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_TRAFFIC_PROCESSED", "traffic-processed")
    )
    group_id: str = field(
        default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "traffic-consumer-group")
    )


@dataclass(frozen=True)
class MinIOConfig:
    """Cấu hình MinIO (S3-compatible)."""
    endpoint: str = field(
        default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000")
    )
    access_key: str = field(
        default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    )
    secret_key: str = field(
        default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    )
    bucket_raw: str = field(
        default_factory=lambda: os.getenv("MINIO_BUCKET_RAW", "traffic-raw-data")
    )
    bucket_processed: str = field(
        default_factory=lambda: os.getenv("MINIO_BUCKET_PROCESSED", "traffic-processed-data")
    )
    use_ssl: bool = field(
        default_factory=lambda: os.getenv("MINIO_USE_SSL", "false").lower() == "true"
    )


@dataclass(frozen=True)
class PostgresConfig:
    """Cấu hình PostgreSQL."""
    host: str = field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost")
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432"))
    )
    database: str = field(
        default_factory=lambda: os.getenv("POSTGRES_DB", "traffic_analytics")
    )
    user: str = field(
        default_factory=lambda: os.getenv("POSTGRES_USER", "daoanhaz")
    )
    password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "daoanh123")
    )

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class ElasticsearchConfig:
    """Cấu hình Elasticsearch."""
    host: str = field(
        default_factory=lambda: os.getenv("ES_HOST", "localhost")
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("ES_PORT", "9200"))
    )
    index_traffic: str = field(
        default_factory=lambda: os.getenv("ES_INDEX_TRAFFIC", "traffic-realtime")
    )

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass(frozen=True)
class FlinkConfig:
    """Cấu hình Apache Flink."""
    jobmanager_host: str = field(
        default_factory=lambda: os.getenv("FLINK_JOBMANAGER_HOST", "localhost")
    )
    jobmanager_port: int = field(
        default_factory=lambda: int(os.getenv("FLINK_JOBMANAGER_PORT", "8081"))
    )


@dataclass(frozen=True)
class SparkConfig:
    """Cấu hình Apache Spark."""
    master: str = field(
        default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]")
    )
    app_name: str = field(
        default_factory=lambda: os.getenv("SPARK_APP_NAME", "TrafficBatchProcessor")
    )
    hadoop_aws_package: str = field(
        default_factory=lambda: os.getenv("SPARK_HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.3.4")
    )
    aws_sdk_package: str = field(
        default_factory=lambda: os.getenv("SPARK_AWS_SDK_PACKAGE", "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    )
    postgres_jdbc_package: str = field(
        default_factory=lambda: os.getenv("SPARK_POSTGRES_JDBC_PACKAGE", "org.postgresql:postgresql:42.7.3")
    )


def create_spark_session():
    """Khởi tạo SparkSession với cấu hình S3A để làm việc với MinIO."""
    from pyspark.sql import SparkSession

    minio_endpoint = f"http{'s' if settings.minio.use_ssl else ''}://{settings.minio.endpoint}"
    jars = (
        f"{settings.spark.hadoop_aws_package},"
        f"{settings.spark.aws_sdk_package},"
        f"{settings.spark.postgres_jdbc_package}"
    )

    spark = (
        SparkSession.builder
        .appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.jars.packages", jars)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.minio.use_ssl).lower())
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


class Settings:
    """Singleton quản lý toàn bộ cấu hình."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_configs()
        return cls._instance

    def _init_configs(self):
        self.tomtom = TomTomConfig()
        self.kafka = KafkaConfig()
        self.minio = MinIOConfig()
        self.postgres = PostgresConfig()
        self.elasticsearch = ElasticsearchConfig()
        self.flink = FlinkConfig()
        self.spark = SparkConfig()


# Singleton instance
settings = Settings()

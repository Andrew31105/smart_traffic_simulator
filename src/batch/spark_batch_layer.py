
import os
import sys
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.config import create_spark_session, settings
from src.utils.logger_utils import get_logger


logger = get_logger("spark_batch_layer", log_to_file=True)


def get_spark_session():
    """Lấy SparkSession với cấu hình đã định nghĩa trong src.utils.config."""
    return create_spark_session()


def read_raw_data(spark: SparkSession) -> DataFrame:
    """Đọc dữ liệu thô từ MinIO bucket "traffic-raw-data"."""
    raw_path = f"s3a://{settings.minio.bucket_raw}/*/*/*/*/*.json"
    logger.info(f"Dang doc du lieu tu: {raw_path}")
    df = spark.read.option("recursiveFileLookup", "true").json(raw_path)
    df = df.withColumn("source_file", F.input_file_name())
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """Làm sạch và chuẩn hóa dữ liệu đầu vào cho batch analytics."""
    has_flat_lat = "lat" in df.columns
    has_flat_lon = "lon" in df.columns

    lat_expr = F.col("location.lat") if "location" in df.columns else F.lit(None)
    lon_expr = F.col("location.lon") if "location" in df.columns else F.lit(None)

    if has_flat_lat:
        lat_expr = F.coalesce(lat_expr, F.col("lat"))
    if has_flat_lon:
        lon_expr = F.coalesce(lon_expr, F.col("lon"))

    df_clean = (
        df.withColumn("sensor_id", F.col("sensor_id"))
        .withColumn("sensor_name", F.col("sensor_name"))
        .withColumn("lat", lat_expr.cast("double"))
        .withColumn("lon", lon_expr.cast("double"))
        .withColumn("current_speed", F.col("current_speed").cast("double"))
        .withColumn("vehicle_count", F.col("vehicle_count").cast("integer"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("region", F.coalesce(F.col("sensor_name"), F.lit("UNKNOWN")))
        .withColumn("event_hour", F.date_trunc("hour", F.col("timestamp")))
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .filter(F.col("sensor_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("current_speed").isNotNull())
        .filter(F.col("vehicle_count").isNotNull())
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .filter((F.col("current_speed") >= 0) & (F.col("current_speed") <= 200))
        .filter(F.col("vehicle_count") >= 0)
        .dropDuplicates(["sensor_id", "timestamp", "source_file"])
        .select(
            "sensor_id",
            "sensor_name",
            "region",
            "lat",
            "lon",
            "current_speed",
            "vehicle_count",
            "timestamp",
            "event_hour",
            "event_date",
            "source_file",
        )
    )

    logger.info("Hoan tat ETL transform du lieu raw")
    return df_clean


def calculate_aggregates(df_clean: DataFrame) -> Dict[str, DataFrame]:
    """Tính thống kê theo giờ, ngày, khu vực từ dữ liệu đã làm sạch."""
    hourly_stats = (
        df_clean.groupBy("event_hour", "region")
        .agg(
            F.count("*").alias("sample_count"),
            F.round(F.avg("current_speed"), 2).alias("avg_speed"),
            F.round(F.min("current_speed"), 2).alias("min_speed"),
            F.round(F.max("current_speed"), 2).alias("max_speed"),
            F.sum("vehicle_count").alias("total_vehicle_count"),
        )
        .orderBy("event_hour", "region")
    )

    daily_stats = (
        df_clean.groupBy("event_date", "region")
        .agg(
            F.count("*").alias("sample_count"),
            F.round(F.avg("current_speed"), 2).alias("avg_speed"),
            F.round(F.min("current_speed"), 2).alias("min_speed"),
            F.round(F.max("current_speed"), 2).alias("max_speed"),
            F.sum("vehicle_count").alias("total_vehicle_count"),
        )
        .orderBy("event_date", "region")
    )

    area_stats = (
        df_clean.groupBy("region")
        .agg(
            F.count("*").alias("sample_count"),
            F.round(F.avg("current_speed"), 2).alias("avg_speed"),
            F.round(F.min("current_speed"), 2).alias("min_speed"),
            F.round(F.max("current_speed"), 2).alias("max_speed"),
            F.sum("vehicle_count").alias("total_vehicle_count"),
        )
        .orderBy("region")
    )

    logger.info("Hoan tat tinh toan aggregate theo gio/ngay/khu vuc")
    return {
        "hourly": hourly_stats,
        "daily": daily_stats,
        "area": area_stats,
    }


def write_processed_to_minio(df_clean: DataFrame, aggregates: Dict[str, DataFrame]) -> None:
    """Ghi dữ liệu sạch và aggregate sang MinIO bucket processed dưới dạng Parquet."""
    base_path = f"s3a://{settings.minio.bucket_processed}/batch"

    cleaned_path = f"{base_path}/cleaned"
    df_clean.write.mode("overwrite").partitionBy("event_date", "region").parquet(cleaned_path)
    logger.info(f"Da ghi cleaned parquet vao: {cleaned_path}")

    hourly_path = f"{base_path}/agg_hourly"
    aggregates["hourly"].write.mode("overwrite").partitionBy("event_hour").parquet(hourly_path)
    logger.info(f"Da ghi hourly parquet vao: {hourly_path}")

    daily_path = f"{base_path}/agg_daily"
    aggregates["daily"].write.mode("overwrite").partitionBy("event_date").parquet(daily_path)
    logger.info(f"Da ghi daily parquet vao: {daily_path}")

    area_path = f"{base_path}/agg_area"
    aggregates["area"].write.mode("overwrite").parquet(area_path)
    logger.info(f"Da ghi area parquet vao: {area_path}")


def write_aggregates_to_postgres(aggregates: Dict[str, DataFrame]) -> None:
    """Ghi các bảng aggregate vào PostgreSQL bằng Spark JDBC."""
    jdbc_url = (
        f"jdbc:postgresql://{settings.postgres.host}:"
        f"{settings.postgres.port}/{settings.postgres.database}"
    )
    jdbc_props = {
        "user": settings.postgres.user,
        "password": settings.postgres.password,
        "driver": "org.postgresql.Driver",
    }

    table_map = {
        "hourly": "traffic_agg_hourly",
        "daily": "traffic_agg_daily",
        "area": "traffic_agg_area",
    }

    for key, table_name in table_map.items():
        aggregates[key].write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=jdbc_props,
        )
        logger.info(f"Da ghi aggregate {key} vao PostgreSQL table: {table_name}")


def run_batch_job() -> None:
    """Chạy toàn bộ pipeline batch: read -> transform -> aggregate -> sink."""
    spark = get_spark_session()
    try:
        raw_df = read_raw_data(spark)
        clean_df = transform_data(raw_df)
        aggregates = calculate_aggregates(clean_df)
        write_processed_to_minio(clean_df, aggregates)
        write_aggregates_to_postgres(aggregates)
        logger.info("Batch job hoan tat thanh cong")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_batch_job()

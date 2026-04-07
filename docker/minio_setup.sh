#!/bin/sh
# ============================================
# MinIO Setup Script
# Khởi tạo bucket trên MinIO
# ============================================

set -e

MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin123}

echo "⏳ Đợi MinIO khởi động..."
sleep 10

# Cấu hình alias cho MinIO
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Tạo bucket cho dữ liệu thô (Raw Data - Bronze Layer)
echo "📦 Tạo bucket: traffic-raw-data"
mc mb --ignore-existing local/traffic-raw-data

# Tạo bucket cho dữ liệu đã xử lý (Processed Data - Silver Layer)
echo "📦 Tạo bucket: traffic-processed-data"
mc mb --ignore-existing local/traffic-processed-data

# Tạo bucket cho dữ liệu phân tích (Analytics Data - Gold Layer)
echo "📦 Tạo bucket: traffic-analytics-data"
mc mb --ignore-existing local/traffic-analytics-data

# Thiết lập policy cho phép đọc public (tuỳ chọn)
# mc anonymous set download local/traffic-analytics-data

echo "✅ MinIO setup hoàn tất!"
echo "📊 Danh sách bucket:"
mc ls local/

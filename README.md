# Smart Traffic Analytics

He thong phan tich giao thong theo mo hinh Lambda Architecture:

- Speed Layer (real-time): Kafka -> Flink -> Kafka -> Elasticsearch -> Kibana
- Batch Layer (historical): Kafka -> MinIO (raw) -> Spark -> MinIO (processed) + PostgreSQL

## 1) Kien truc tong quan

```
Traffic Simulator
     |
     v
Kafka topic: traffic-raw
     |                         
     +--> Flink Speed Layer --> Kafka topic: traffic-processed --> Elasticsearch --> Kibana
     |
     +--> Kafka to MinIO (raw JSON)
                 |
                 v
            MinIO bucket: traffic-raw-data
                 |
                 v
            Spark Batch Layer (ETL + aggregate)
                 +--> MinIO bucket: traffic-processed-data (Parquet)
                 +--> PostgreSQL (traffic_agg_hourly / daily / area)
```

## 2) Cong nghe

| Thanh phan | Cong nghe |
|---|---|
| Message Broker | Apache Kafka |
| Streaming | Apache Flink (PyFlink) |
| Batch | Apache Spark (PySpark) |
| Data Lake | MinIO (S3-compatible) |
| Search/Visualization | Elasticsearch + Kibana |
| Relational DB | PostgreSQL |
| Orchestration | Docker Compose |

## 3) Cau truc du an

```
smart-traffic-analytics/
├── docker/
│   ├── docker-compose.yml
│   └── minio_setup.sh
├── src/
│   ├── producers/
│   │   └── traffic_simulator.py
│   ├── ingestion/
│   │   └── kafka_to_minio.py
│   ├── streaming/
│   │   ├── flink_speed_layer.py
│   │   └── kafka_to_elasticsearch.py
│   ├── batch/
│   │   └── spark_batch_layer.py
│   ├── dashboard/
│   │   ├── setup_kibana_dashboard.py
│   │   ├── import_dashboard.py
│   │   ├── kibana_objects.json
│   │   └── DASHBOARD_GUIDE.md
│   └── utils/
│       ├── config.py
│       └── logger_utils.py
├── requirements.txt
└── README.md
```

## 4) Chuan bi moi truong

Yeu cau:

- Docker + Docker Compose
- Python 3.10+

Cai thu vien Python:

```bash
pip install -r requirements.txt
```

Khoi dong ha tang:

```bash
cd docker
docker compose up -d
```

Service ports:

- Kafka: `localhost:9092`
- MinIO API: `localhost:9000`
- MinIO Console: `http://localhost:9001`
- Elasticsearch: `http://localhost:9200`
- Kibana: `http://localhost:5601`
- PostgreSQL: `localhost:5432`

## 5) Cau hinh `.env`

Project doc bien moi truong tai `src/utils/config.py`.

Gia tri mac dinh da du de chay local, nhung ban co the override qua `.env`, vi du:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET_RAW=traffic-raw-data
MINIO_BUCKET_PROCESSED=traffic-processed-data
MINIO_USE_SSL=false

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=traffic_analytics
POSTGRES_USER=traffic_user
POSTGRES_PASSWORD=traffic_pass_123

SPARK_MASTER=local[*]
SPARK_APP_NAME=TrafficBatchProcessor
```

## 6) Cach chay toan bo pipeline

Nen mo 5 terminal rieng:

Terminal 1 - Producer:

```bash
python src/producers/traffic_simulator.py
```

Terminal 2 - Ingestion Kafka -> MinIO:

```bash
python src/ingestion/kafka_to_minio.py
```

Terminal 3 - Speed Layer Flink:

```bash
python src/streaming/flink_speed_layer.py
```

Terminal 4 - Sink Kafka processed -> Elasticsearch:

```bash
python src/streaming/kafka_to_elasticsearch.py
```

Terminal 5 - Batch Layer Spark:

```bash
python src/batch/spark_batch_layer.py
```

## 7) Ket qua mong doi

### 7.1 MinIO

- Raw data NDJSON trong bucket `traffic-raw-data`
- Batch output Parquet trong bucket `traffic-processed-data`:
  - `batch/cleaned`
  - `batch/agg_hourly`
  - `batch/agg_daily`
  - `batch/agg_area`

### 7.2 PostgreSQL

Bang aggregate duoc ghi boi Spark:

- `traffic_agg_hourly`
- `traffic_agg_daily`
- `traffic_agg_area`

### 7.3 Kibana

Sau khi co du lieu Elasticsearch, setup dashboard:

```bash
python src/dashboard/setup_kibana_dashboard.py
```

Truy cap: `http://localhost:5601`

## 8) Logging

- Cac module dung logger chung trong `src/utils/logger_utils.py`
- Log duoc ghi vao thu muc `logs/`

## 9) Troubleshooting nhanh

1. Spark warning `hostname resolves to loopback`
    - Chi la warning local, khong chan pipeline.
2. Spark warning `Unable to load native-hadoop library`
    - Thuong gap tren local, co the bo qua.
3. Batch khong doc duoc MinIO
    - Kiem tra `MINIO_ENDPOINT`, key/secret, bucket raw da ton tai.
4. Batch khong ghi duoc PostgreSQL
    - Kiem tra Postgres container dang chay va thong tin `POSTGRES_*`.
5. Kibana khong co data
    - Kiem tra module `kafka_to_elasticsearch.py` co dang consume topic processed hay khong.

## 10) Lenh huu ich

Dung nhanh trang thai containers:

```bash
cd docker
docker compose ps
```

Xem logs 1 service:

```bash
cd docker
docker compose logs -f postgres
```

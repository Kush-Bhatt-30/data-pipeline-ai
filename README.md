# data-pipeline-ai

An end-to-end, production-style data engineering platform you can run locally with Docker Compose: batch + streaming ingestion (Lambda architecture), raw/processed storage zones, Spark processing, Airflow orchestration, an ML training/prediction layer, and a FastAPI model serving API.

## Architecture

**Sources**
- REST API ingestion: pulls transactions from a REST endpoint (locally we expose a generator at `api:/source/transactions`).
- Batch files: CSV/JSONL batch drops under `storage/local/sample_data/`.
- Kafka streaming: a producer generates transaction events to a topic and a consumer lands them to storage.

**Ingestion Layer (Python)**
- Batch ingestion script writes raw data to an S3-compatible object store (LocalStack) and also writes a **local mirror** for Spark.
- Kafka consumer batches messages and flushes to raw storage on an interval for fault tolerance.

**Storage Layer**
- **Raw zone:** S3-compatible (LocalStack S3) at `s3://dpa-raw/transactions/dt=YYYY-MM-DD/source=.../*.jsonl`.
- **Processed zone:** partitioned Parquet (incremental `dt=` partitions) written to `storage/local/processed/` and optionally uploaded to `s3://dpa-processed/`.
- **Warehouse (mock):** daily aggregate CSV exports under `storage/local/warehouse/` (a stand-in for BigQuery/Snowflake).

**Processing Layer (PySpark)**
- Reads raw JSONL (from the local mirror for a runnable local demo).
- Cleans, validates, deduplicates, enriches (adds timestamps + `dt`).
- Writes partitioned Parquet and BI-friendly aggregates.

**Orchestration (Airflow)**
- A daily DAG runs: bootstrap storage → batch ingest (REST + files) → Spark processing → model training → batch predictions.
- Retries + failure semantics are configured via DAG default args.

**AI Layer (scikit-learn)**
- Feature engineering + model training using a `Pipeline`:
  - Numeric: `amount`, `item_count`
  - Categorical: `payment_method`, `country`
- Label for demo: `is_high_value` derived from `amount >= threshold` (configurable).
- Batch prediction produces `storage/local/bi/predictions/dt=.../predictions.parquet`.

**Serving Layer (FastAPI)**
- `POST /predict` scores a single transaction feature payload.
- `GET /model/meta` reports whether the model artifact is loaded and shows metadata.
- `GET /source/transactions` is a local demo source endpoint for REST ingestion.

## Tech Stack
- Python 3.11 (Docker images)
- Kafka + Zookeeper (Confluent images)
- LocalStack (S3)
- PySpark (Spark processing)
- Apache Airflow (orchestration)
- scikit-learn (ML)
- FastAPI + Uvicorn (serving)
- Parquet (pyarrow) for processed/BI datasets

## Local Setup (Docker Compose)

From the repo root:

```bash
docker compose -f docker/docker-compose.yml up --build
```

Services:
- FastAPI API: `http://localhost:8000`
- Airflow UI: `http://localhost:8081` (user: `airflow`, password: `airflow`)
- LocalStack (S3): `http://localhost:4566`
- Kafka exposed on host: `localhost:29092` (internal: `kafka:9092`)
- Spark master UI: `http://localhost:8080`

## Run The Pipeline

### 1) Streaming path (always-on)
`kafka-producer` and `kafka-consumer` start with Compose:
- Producer emits events into Kafka topic `transactions`
- Consumer lands raw JSONL into:
  - S3 (LocalStack): `s3://dpa-raw/transactions/dt=.../source=kafka/*.jsonl`
  - Local mirror: `storage/local/raw_mirror/...`

### 2) Batch path (Airflow DAG)
1. Open Airflow UI at `http://localhost:8081`
2. Find the DAG `data_pipeline_ai_lambda`
3. Unpause and trigger it (it runs daily, `@daily`, with `catchup=False`)

Outputs:
- Processed Parquet: `storage/local/processed/transactions/dt=YYYY-MM-DD/`
- BI aggregates: `storage/local/bi/daily_metrics/dt=YYYY-MM-DD/`
- Warehouse mock export: `storage/local/warehouse/dt=YYYY-MM-DD/`
- Model artifact: `storage/local/artifacts/model.joblib`
- Batch predictions: `storage/local/bi/predictions/dt=YYYY-MM-DD/predictions.parquet`

## Data Flow (End-to-End)

1. **Ingestion**
   - REST + files ingestion write raw JSONL to S3 raw zone and local mirror.
   - Kafka consumer writes streaming raw JSONL to the same raw zone.
2. **Processing (Spark)**
   - Incremental per-day processing (`dt=...`).
   - Cleans + dedupes + validates, then writes partitioned Parquet.
   - Builds daily aggregates for BI/warehouse.
3. **AI**
   - Train a model from processed Parquet.
   - Generate batch predictions and export to BI folder.
4. **Serving**
   - FastAPI loads the trained model artifact and serves predictions.

## Configuration

Configuration is YAML-driven with environment overrides:
- Base config: `config/config.yaml`
- Override any key via env vars using `DPA__` and `__` nesting:
  - `DPA__S3__ENDPOINT_URL=http://localstack:4566`
  - `DPA__KAFKA__BOOTSTRAP_SERVERS=kafka:9092`
  - `DPA__SPARK__MASTER_URL=local[*]`

## Repo Layout

```
data-pipeline-ai/
  ingestion/        # REST + files batch ingestion, Kafka producer/consumer
  processing/       # PySpark transformations and incremental job
  orchestration/    # Airflow DAG
  storage/          # S3 client + local storage folders
  ai/               # Training + batch prediction
  api/              # FastAPI serving
  config/           # YAML config + loader
  docker/           # Dockerfile + docker-compose + Airflow image
  tests/            # Unit tests (Docker-friendly)
  README.md
```

## Resume-Ready Description

Built a cloud-ready, end-to-end data platform implementing a Lambda architecture with batch + streaming ingestion (Kafka), raw/processed zone storage on S3-compatible object storage (LocalStack), incremental partitioned transformations in PySpark, orchestration with Airflow (retries/failure handling), an ML feature engineering + training/prediction layer (scikit-learn pipelines), and a FastAPI model serving API. Delivered a docker-compose local environment that reproduces the full platform stack for development and demos.

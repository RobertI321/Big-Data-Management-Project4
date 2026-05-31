# RICO Multimodal Pipeline

Scheduled, idempotent, observable Airflow DAG that ingests RICO mobile UI screenshots, embeds them with CLIP and SBERT, extracts structured metadata via an LLM, and stores everything in Postgres+pgvector with full row-level traceability.

## How to run

### Prerequisites

- Docker and Docker Compose v2
- Python 3.11 (inside the Airflow containers, handled automatically)
- Ports 8080 (Airflow UI), 5432 (Postgres), 9000/9001 (MinIO), 11434 (Ollama) available

### Start all services

```bash
docker compose up -d --wait postgres minio ollama
docker compose up -d minio-init ollama-init
docker compose up -d airflow-init
docker compose up -d --wait airflow-webserver airflow-scheduler
```

Or if `make` is installed: `make up`

Wait for all containers to report healthy. Airflow UI will be available at [http://localhost:8080](http://localhost:8080).

### Trigger the DAG

1. Open [http://localhost:8080](http://localhost:8080) and log in with `airflow` / `airflow`
2. Find the `rico_pipeline` DAG and unpause it if needed
3. Click **Trigger DAG** (play button) and set the `LIMIT` parameter (default: 5, demo: 50)
4. Monitor progress in the **Graph** view — all 8 tasks should turn green

### Reset data between runs

```bash
docker compose exec postgres psql -U rico -d rico -c \
  "TRUNCATE TABLE pipeline_runs RESTART IDENTITY CASCADE;"
docker compose exec minio mc alias set local http://minio:9000 minioadmin minioadmin
docker compose exec minio mc rm --recursive --force local/rico-raw/
```

Or if `make` is installed: `make dev-reset`

### Stop services

```bash
docker compose down       # stop, keep volumes
docker compose down -v    # stop and wipe all data
```

## What each metric means

All metrics are persisted to `pipeline_metrics` keyed by `(run_id, metric_name)`.

| Metric | What it measures | Bad value |
|--------|-----------------|-----------|
| `total_run_duration_seconds` | Wall-clock time from pipeline start to load completion | Unusually high compared to previous runs indicates a bottleneck |
| `final_run_status` | 1.0 = success, 0.0 = failed | 0.0 means row-count verification failed |
| `metadata_row_count` | Screens ingested in this run | 0 on a first run means ingestion failed |
| `extraction_payload_non_null_pct` | % of screens where the LLM returned valid JSON | Below 80% suggests the LLM prompt or model needs tuning |
| `confidence_above_threshold_pct` | % of screens where LLM confidence >= 0.5 | Below 50% suggests low-quality extractions |
| `review_queue_pct` | % of screens that failed LLM extraction and were routed to review | Above 20% means the LLM is struggling with the input |
| `embeddings_row_count` | Total embedding rows (image + text) for this run | Should be 2x metadata_row_count |
| `image_vector_dims` | Average dimensionality of CLIP image vectors | Anything other than 512 means the wrong model was loaded |
| `text_vector_dims` | Average dimensionality of SBERT text vectors | Anything other than 384 means the wrong model was loaded |
| `zero_norm_vector_pct` | % of embedding vectors with zero L2 norm | **Any non-zero value signals a silent embedder bug** — the model returned an all-zeros vector, meaning the embedding is meaningless but no error was raised. Investigate the input that produced it. |
| `distinct_app_package_count` | Unique Android app packages in this run | 0 means app_package was never populated |
| `distinct_category_count` | Unique app categories in this run | 0 means category was never populated |
| `eval_recall_at_5` | Recall@5 self-test: fraction of screens that find themselves in top-5 nearest neighbors | Below 0.8 on a self-test suggests embedding quality issues |
| `eval_screens_tested` | Number of screens used in the recall evaluation | Should match metadata_row_count |

## How to interpret an audit failure

The audit task is a circuit breaker that checks for duplicate primary keys in `screens_embeddings` and `screens_metadata`. If duplicates exist, it halts the pipeline immediately.

### Step 1: Identify the failure in Airflow UI

Open the **Graph** view for the failed DAG run. The `audit` task will be red (failed) and `eval_task` will be pink (upstream_failed / skipped).

### Step 2: Read the audit task log

Click the `audit` task, then **Log**. Look for lines containing `AUDIT DUPLICATE` — each one lists the exact duplicate key:

```
[run_id=...] AUDIT DUPLICATE: {"table": "screens_embeddings", "screen_id": 42, "model_name": "SBERT", "model_version": "all-MiniLM-L6-v2", "embedding_kind": "text", "count": 2}
```

### Step 3: Check the audit_results table

```sql
SELECT id, run_id, passed, details, checked_at
FROM audit_results
WHERE passed = false
ORDER BY checked_at DESC;
```

The `details` column contains a JSON object with all duplicate keys and their counts.

### Step 4: Identify the duplicate rows

```sql
-- Find duplicates in screens_embeddings
SELECT screen_id, model_name, model_version, embedding_kind, count(*)
FROM screens_embeddings
GROUP BY screen_id, model_name, model_version, embedding_kind
HAVING count(*) > 1;

-- Find duplicates in screens_metadata
SELECT screen_id, count(*)
FROM screens_metadata
GROUP BY screen_id
HAVING count(*) > 1;
```

### Step 5: Recover

The safest recovery is a full data reset and re-run:

```bash
make dev-reset   # or run the TRUNCATE commands from "Reset data" above
```

Then trigger the DAG again. The `ON CONFLICT DO NOTHING` idempotency guarantees a clean re-run will not produce duplicates.

## Project structure

```
dags/rico_pipeline.py       Thin Airflow DAG — orchestration only, zero business logic
pipeline/__init__.py        Package marker
pipeline/db.py              Postgres connection helper (reads env vars)
pipeline/tracing.py         Pipeline run creation, fingerprinting, git SHA
pipeline/notify.py          Slack webhook notifications (never raises on failure)
pipeline/ingest.py          Stream RICO from HuggingFace, upload to MinIO, insert metadata
pipeline/parse.py           Parse view-hierarchy JSON into text representations
pipeline/embed_image.py     CLIP ViT-B-32 image embeddings (512-d)
pipeline/embed_text.py      SBERT all-MiniLM-L6-v2 text embeddings (384-d)
pipeline/extract.py         Ollama LLM extraction, routes failures to review queue
pipeline/load.py            Row-count verification, pipeline run finalization, metrics
pipeline/audit.py           Duplicate-detection circuit breaker (raises on failure)
pipeline/eval.py            Recall@5 self-test evaluation
pipeline/metrics.py         Observability metrics collection and summary logging
config/01_schema.sql        Database schema (6 tables, pgvector extension)
prompts/extract_v1.txt      LLM prompt for structured extraction
docker-compose.yml          Full stack: Postgres, MinIO, Ollama, Airflow (LocalExecutor)
Makefile                    up/down/clean/reset/dev-reset/logs targets
requirements.txt            Python dependencies installed into Airflow containers
.env                        Environment variables (not committed to git)
```

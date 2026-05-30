# Project Structure

## Current State

```
big_data_project4/
│
├── config/
│   └── 01_schema.sql           ✅ done — all 6 tables defined with run_id + source_fingerprint
│
├── dags/                       ✅ folder exists
│   └── rico_pipeline.py        ✅ done — DAG skeleton, Phase 3
│
├── pipeline/                   ✅ folder exists
│   ├── tracing.py              ✅ done — run_id, fingerprint, git_sha
│   ├── db.py                   ✅ done — connection helper
│   └── (remaining modules)     ❌ not created yet — Phase 5
│
├── prompts/                    ✅ folder exists
│   └── extract_v1.txt          ✅ done — LLM prompt file
│
├── venv/                       ✅ exists, activated
├── docker-compose.yml          ✅ done — postgres, minio, ollama, all healthy
├── Makefile                    ✅ done — up/down/clean/reset targets
├── .env                        ✅ done — credentials, never committed
├── .gitignore                  ✅ done — .env and venv excluded
├── structure.md                ✅ this file
├── week07_notebook.ipynb       ✅ reference only — do not copy code directly
└── test_notebook.ipynb         (empty, ignore)
```

---

## Infrastructure Status

| Service  | URL                   | Status |
|----------|-----------------------|--------|
| Postgres | localhost:5432        | ✅ running |
| MinIO    | localhost:9000        | ✅ running |
| MinIO UI | localhost:9001        | ✅ running |
| Ollama   | localhost:11434       | ✅ running, qwen2.5:3b pulled |

---

## What's Done

- [x] Phase 1 — Schema designed and applied (`config/01_schema.sql`)
  - `pipeline_runs` with `run_id`, model versions, git_sha
  - `audit_results` with FK to pipeline_runs
  - `pipeline_metrics` with FK to pipeline_runs
  - `screens_metadata` with `run_id` + `source_fingerprint`
  - `screens_embeddings` with `run_id` + `source_fingerprint`
  - `screens_review_queue` with `run_id` + `source_fingerprint`
- [x] Phase 2 — Project structure created
  - Folders: `config/`, `dags/`, `pipeline/`, `prompts/`
  - `docker-compose.yml` with all services
  - `Makefile` with make targets
  - `.env` and `.gitignore` in place
  - `prompts/extract_v1.txt` created
  - Infrastructure verified (Ollama responding, Postgres accessible)
- [x] Phase 3 — DAG skeleton
  - `dags/rico_pipeline.py` with all 8 tasks as stubs (including `_start_run`)
  - Dependencies wired: start_run → ingest → [parse, embed_image, embed_text] → extract → load → audit
  - LIMIT param (default 5)
- [x] Phase 4 — Traceability infrastructure (partial)
  - `pipeline/tracing.py` — run_id generation, sha256 fingerprint, git_sha capture
  - `pipeline/db.py` — connection helper from env vars
  - ❌ Wire run_id creation at DAG start, pass via XCom to all tasks — pending Phase 5

---

## What's Next

- [ ] Phase 5 — Implement each module
  - `pipeline/ingest.py` — HuggingFace streaming + MinIO upload + Postgres insert
  - `pipeline/parse.py` — parse_hierarchy + text_representation (reference notebook Section 2)
  - `pipeline/embed_image.py` — CLIP ViT-B-32, 512-d vectors (reference notebook Section 3)
  - `pipeline/embed_text.py` — SBERT all-MiniLM-L6-v2, 384-d vectors (reference notebook Section 4)
  - `pipeline/extract.py` — Ollama qwen2.5:3b, reads prompt from prompts/extract_v1.txt (reference notebook Section 5)
  - `pipeline/load.py` — verify all tasks wrote rows, update pipeline_runs status
  - `pipeline/notify.py` — Slack webhook, wrapped in try/except

- [ ] Phase 6 — Audit (circuit breaker)
  - Create `pipeline/audit.py`
  - Duplicate check on screens_embeddings and screens_metadata
  - Raise exception on failure to halt the pipeline
  - Write result to audit_results table
  - Test it: manually insert duplicate, watch audit fail

- [ ] Phase 7 — Observability
  - Create `pipeline/metrics.py`
  - Per-task duration, row counts, retry counts
  - Data quality metrics on destination tables
  - End-of-run log summary block

- [ ] Phase 8 — Slack notifications
  - Run started message
  - Audit failed message (most important)
  - Run finished message
  - All wrapped in try/except

- [ ] Phase 9 — Idempotency verification
  - Re-run DAG with same LIMIT
  - Confirm zero new rows in destination tables

- [ ] Phase 10 — Final checklist
  - All definition-of-done items checked
  - README written
  - Audit tested on purpose with fake duplicate
-- Runs on first initialization of the postgres data volume.
-- The default database (POSTGRES_DB=rico) is created by the entrypoint
-- before this script runs.

-- Airflow stores its metadata in a separate database on the same instance.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

\c rico

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pipeline_runs(
    id BIGSERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid() UNIQUE,
    dag_run_id VARCHAR(255) NOT NULL,
    started_at TIMESTAMP DEFAULT NULL,
    ended_at TIMESTAMP DEFAULT NULL,
    status VARCHAR(20) NOT NULL,
    limit_param INT DEFAULT NULL,
    git_sha VARCHAR(40) DEFAULT NULL,
    clip_version VARCHAR(20) DEFAULT NULL,
    sbert_version VARCHAR(20) DEFAULT NULL,
    llm_version VARCHAR(20) DEFAULT NULL,
    prompt_version VARCHAR(20) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS audit_results(
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    audit_name VARCHAR(255) NOT NULL,
    passed boolean NOT NULL,
    details JSONB DEFAULT NULL,
    checked_at TIMESTAMP DEFAULT NULL,

    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_metrics(
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value FLOAT NOT NULL,
    recorded_at TIMESTAMP DEFAULT NULL,

    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id           BIGINT PRIMARY KEY,
    app_package         TEXT,
    category            TEXT,
    png_path            TEXT NOT NULL,
    hierarchy_json_path TEXT NOT NULL,
    extraction_payload  JSONB,
    prompt_version      TEXT,
    confidence          DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id              UUID NOT NULL,
    source_fingerprint    TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id      BIGINT NOT NULL,
    model_name     TEXT NOT NULL,
    model_version  TEXT NOT NULL,
    embedding_kind TEXT NOT NULL CHECK (embedding_kind IN ('image', 'text')),
    vector         vector NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id         UUID NOT NULL,
    source_fingerprint TEXT NOT NULL,
    PRIMARY KEY (screen_id, model_name, model_version, embedding_kind),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS screens_review_queue (
    id          BIGSERIAL PRIMARY KEY,
    screen_id   BIGINT NOT NULL,
    reason      TEXT NOT NULL,
    raw_output  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id      UUID NOT NULL,
    source_fingerprint TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);
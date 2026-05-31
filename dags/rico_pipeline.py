"""RICO multimodal pipeline DAG.

Thin orchestration file — every task delegates to a function in the
pipeline/ package.  Zero business logic lives here.

DAG shape:
    ingest → parse → [embed_image, embed_text, extract] → load → audit → eval
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

# ── Default args applied to every task ──────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "rico",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="rico_pipeline",
    schedule=None,
    catchup=False,
    tags=["rico", "multimodal"],
    default_args=DEFAULT_ARGS,
    params={"LIMIT": Param(default=5, type="integer", minimum=1, description="Number of screens to process")},
)
def rico_pipeline():
    """Scheduled, idempotent, observable RICO screen pipeline."""

    # ── 1. ingest ───────────────────────────────────────────────────────────
    @task()
    def ingest(**context) -> str:
        """Create a pipeline run and ingest screens from HuggingFace."""
        from pipeline.db import get_connection
        from pipeline.tracing import create_pipeline_run
        from pipeline.notify import notify_run_started
        from pipeline.ingest import run_ingest

        limit = context["params"]["LIMIT"]
        dag_run_id = context["dag_run"].run_id
        trigger_type = context["dag_run"].run_type

        with get_connection() as conn:
            run_id = create_pipeline_run(conn, dag_run_id, limit)

        notify_run_started(run_id, limit, trigger_type)

        inserted = run_ingest(run_id, limit)
        log.info("[run_id=%s] ingest complete: %d rows inserted", run_id, inserted)
        return str(run_id)

    # ── 2. parse ────────────────────────────────────────────────────────────
    @task()
    def parse(run_id_str: str, **context) -> str:
        """Parse view-hierarchy JSON into text representations."""
        from pipeline.parse import run_parse
        parsed = run_parse(run_id_str)
        log.info("[run_id=%s] parse complete: %d screens", run_id_str, parsed)
        return run_id_str

    # ── 3a. embed_image (parallel) ──────────────────────────────────────────
    @task()
    def embed_image(run_id_str: str, **context) -> str:
        """Embed screen PNGs with CLIP ViT-B-32."""
        from pipeline.embed_image import run_embed_image
        inserted = run_embed_image(run_id_str)
        log.info("[run_id=%s] embed_image complete: %d rows", run_id_str, inserted)
        return run_id_str

    # ── 3b. embed_text (parallel) ───────────────────────────────────────────
    @task()
    def embed_text(run_id_str: str, **context) -> str:
        """Embed parsed text with SBERT all-MiniLM-L6-v2."""
        from pipeline.embed_text import run_embed_text
        inserted = run_embed_text(run_id_str)
        log.info("[run_id=%s] embed_text complete: %d rows", run_id_str, inserted)
        return run_id_str

    # ── 3c. extract (parallel) ──────────────────────────────────────────────
    @task()
    def extract(run_id_str: str, **context) -> str:
        """Extract structured JSON from screens via Ollama LLM."""
        from pipeline.extract import run_extract
        result = run_extract(run_id_str)
        log.info("[run_id=%s] extract complete: %s", run_id_str, result)
        return run_id_str

    # ── 4. load ─────────────────────────────────────────────────────────────
    @task()
    def load(run_id_image: str, run_id_text: str, run_id_extract: str, **context) -> str:
        """Verify row counts and finalise the pipeline run."""
        from pipeline.load import run_load

        # All three inputs carry the same run_id; pick any one.
        run_id_str = run_id_image
        limit = context["params"]["LIMIT"]

        status = run_load(run_id_str, limit)
        log.info("[run_id=%s] load complete: status=%s", run_id_str, status)
        return run_id_str

    # ── 5. audit ────────────────────────────────────────────────────────────
    @task()
    def audit(run_id_str: str, **context) -> str:
        """Run duplicate-detection audit (circuit breaker)."""
        from pipeline.audit import run_audit
        run_audit(run_id_str)
        return run_id_str

    # ── 6. eval ─────────────────────────────────────────────────────────────
    @task()
    def eval_task(run_id_str: str, **context) -> str:
        """Compute recall@k evaluation metrics."""
        from pipeline.eval import run_eval
        result = run_eval(run_id_str)
        log.info("[run_id=%s] eval complete: %s", run_id_str, result)
        return run_id_str

    # ── Wire dependencies ──────────────────────────────────────────────────
    rid = ingest()
    rid_parsed = parse(rid)

    # Three parallel branches — each depends only on parse
    rid_img = embed_image(rid_parsed)
    rid_txt = embed_text(rid_parsed)
    rid_ext = extract(rid_parsed)

    # Load waits for all three parallel branches
    rid_loaded = load(rid_img, rid_txt, rid_ext)

    rid_audited = audit(rid_loaded)
    eval_task(rid_audited)


# Instantiate the DAG
rico_pipeline()

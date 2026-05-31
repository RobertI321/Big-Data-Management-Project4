"""Observability metrics for the RICO pipeline.

Collects pipeline health and data quality metrics after load verification,
persists them to pipeline_metrics, and logs a one-screen summary block.
"""

import logging
from datetime import datetime, timezone
from uuid import UUID

from pipeline.db import get_connection

log = logging.getLogger(__name__)


def _persist_metrics(conn, run_id: UUID, metrics: dict[str, float]) -> None:
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for name, value in metrics.items():
            cur.execute(
                """
                INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, recorded_at)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, name, float(value), now),
            )
    conn.commit()


def collect_and_persist(run_id: str | UUID) -> dict[str, float]:
    """Collect all metrics for a run, persist them, and log the summary."""
    run_id = UUID(str(run_id))
    metrics: dict[str, float] = {}

    with get_connection() as conn:
        with conn.cursor() as cur:
            # ── Pipeline health ────────────────────────────────────────
            cur.execute(
                "SELECT started_at, ended_at, status FROM pipeline_runs WHERE run_id = %s",
                (run_id,),
            )
            row = cur.fetchone()
            started_at, ended_at, status = row if row else (None, None, "unknown")

            now = datetime.now(timezone.utc)
            duration = 0.0
            if started_at:
                sa = started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
                end = ended_at if ended_at else now
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                duration = (end - sa).total_seconds()

            metrics["total_run_duration_seconds"] = duration
            metrics["final_run_status"] = 1.0 if status == "success" else 0.0

            # ── screens_metadata quality ───────────────────────────────
            cur.execute(
                "SELECT count(*) FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            meta_count = cur.fetchone()[0]
            metrics["metadata_row_count"] = meta_count

            if meta_count > 0:
                cur.execute(
                    "SELECT count(*) FROM screens_metadata "
                    "WHERE run_id = %s AND extraction_payload IS NOT NULL",
                    (run_id,),
                )
                extraction_nn = cur.fetchone()[0]
                metrics["extraction_payload_non_null_pct"] = round(
                    100.0 * extraction_nn / meta_count, 1
                )

                cur.execute(
                    "SELECT count(*) FROM screens_metadata "
                    "WHERE run_id = %s AND confidence >= 0.5",
                    (run_id,),
                )
                conf_ok = cur.fetchone()[0]
                metrics["confidence_above_threshold_pct"] = round(
                    100.0 * conf_ok / meta_count, 1
                )

                cur.execute(
                    "SELECT count(*) FROM screens_review_queue WHERE run_id = %s",
                    (run_id,),
                )
                review_count = cur.fetchone()[0]
                metrics["review_queue_pct"] = round(
                    100.0 * review_count / meta_count, 1
                )
            else:
                metrics["extraction_payload_non_null_pct"] = 0.0
                metrics["confidence_above_threshold_pct"] = 0.0
                metrics["review_queue_pct"] = 0.0

            # ── screens_embeddings quality ─────────────────────────────
            cur.execute(
                "SELECT count(*) FROM screens_embeddings WHERE run_id = %s",
                (run_id,),
            )
            emb_count = cur.fetchone()[0]
            metrics["embeddings_row_count"] = emb_count

            cur.execute(
                "SELECT avg(vector_dims(vector)) FROM screens_embeddings "
                "WHERE run_id = %s AND embedding_kind = 'image'",
                (run_id,),
            )
            img_dims = cur.fetchone()[0]
            metrics["image_vector_dims"] = float(img_dims) if img_dims else 0.0
            if img_dims and img_dims != 512:
                log.warning(
                    "[run_id=%s] image vector dims = %.0f (expected 512)",
                    run_id, img_dims,
                )

            cur.execute(
                "SELECT avg(vector_dims(vector)) FROM screens_embeddings "
                "WHERE run_id = %s AND embedding_kind = 'text'",
                (run_id,),
            )
            txt_dims = cur.fetchone()[0]
            metrics["text_vector_dims"] = float(txt_dims) if txt_dims else 0.0
            if txt_dims and txt_dims != 384:
                log.warning(
                    "[run_id=%s] text vector dims = %.0f (expected 384)",
                    run_id, txt_dims,
                )

            # Zero-norm: pgvector <#> returns negative inner product.
            # (v <#> v) = -||v||^2, so it equals 0 iff v is the zero vector.
            cur.execute(
                """
                SELECT count(*)
                FROM screens_embeddings
                WHERE run_id = %s
                  AND (vector <#> vector) = 0
                """,
                (run_id,),
            )
            zero_norm_count = cur.fetchone()[0]
            metrics["zero_norm_vector_pct"] = (
                round(100.0 * zero_norm_count / emb_count, 1) if emb_count > 0 else 0.0
            )

            # ── Sanity checks ─────────────────────────────────────────
            cur.execute(
                "SELECT count(DISTINCT app_package) FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            metrics["distinct_app_package_count"] = cur.fetchone()[0]

            cur.execute(
                "SELECT count(DISTINCT category) FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            metrics["distinct_category_count"] = cur.fetchone()[0]

        # ── Persist ────────────────────────────────────────────────────
        _persist_metrics(conn, run_id, metrics)

    # ── Log summary block ──────────────────────────────────────────────
    img_check = "✓" if metrics["image_vector_dims"] == 512 else "✗"
    txt_check = "✓" if metrics["text_vector_dims"] == 384 else "✗"
    status_str = "success" if metrics["final_run_status"] == 1.0 else "failed"

    summary = (
        f"\n{'=' * 51}\n"
        f"PIPELINE RUN SUMMARY  run_id={run_id}\n"
        f"{'=' * 51}\n"
        f"STATUS          {status_str}\n"
        f"DURATION        {metrics['total_run_duration_seconds']:.1f}s\n"
        f"{'-' * 51}\n"
        f"DATA QUALITY\n"
        f"  metadata rows        {int(metrics['metadata_row_count'])}\n"
        f"  extraction non-null  {metrics['extraction_payload_non_null_pct']:.1f}%\n"
        f"  confidence >= 0.5    {metrics['confidence_above_threshold_pct']:.1f}%\n"
        f"  review queue         {metrics['review_queue_pct']:.1f}%\n"
        f"  embeddings rows      {int(metrics['embeddings_row_count'])}\n"
        f"  image dims           {int(metrics['image_vector_dims'])}  {img_check}\n"
        f"  text dims            {int(metrics['text_vector_dims'])}  {txt_check}\n"
        f"  zero-norm vectors    {metrics['zero_norm_vector_pct']:.1f}%\n"
        f"{'-' * 51}\n"
        f"SANITY\n"
        f"  distinct apps        {int(metrics['distinct_app_package_count'])}\n"
        f"  distinct categories  {int(metrics['distinct_category_count'])}\n"
        f"{'=' * 51}"
    )
    log.info("[run_id=%s] %s", run_id, summary)

    return metrics

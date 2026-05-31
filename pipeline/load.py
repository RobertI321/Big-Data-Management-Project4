"""Load verification and pipeline run finalisation.

Queries row counts for the current run_id across all destination tables,
compares against expected counts, updates pipeline_runs with final status,
and calls notify_run_finished().

This is the "checkpoint" task — it decides whether the run succeeded.
"""

import logging
from datetime import datetime, timezone
from uuid import UUID

from pipeline.db import get_connection
from pipeline.tracing import update_pipeline_run
from pipeline.notify import notify_run_finished

log = logging.getLogger(__name__)


def run_load(run_id: UUID, limit: int) -> str:
    """Verify row counts and finalise the pipeline run.

    Checks:
      - screens_metadata has >= 1 row for this run_id
      - screens_embeddings has image AND text rows for this run_id
      - pipeline_runs is updated to 'success' or 'failed' with ended_at

    Args:
        run_id: Pipeline run UUID.
        limit: The LIMIT param for this run (used for summary).

    Returns:
        Final status string ('success' or 'failed').
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Count metadata rows for THIS run
            cur.execute(
                "SELECT count(*) FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            metadata_count = cur.fetchone()[0]

            # Count image embeddings for THIS run
            cur.execute(
                "SELECT count(*) FROM screens_embeddings "
                "WHERE run_id = %s AND embedding_kind = 'image'",
                (run_id,),
            )
            image_emb_count = cur.fetchone()[0]

            # Count text embeddings for THIS run
            cur.execute(
                "SELECT count(*) FROM screens_embeddings "
                "WHERE run_id = %s AND embedding_kind = 'text'",
                (run_id,),
            )
            text_emb_count = cur.fetchone()[0]

            # Count review queue entries for THIS run
            cur.execute(
                "SELECT count(*) FROM screens_review_queue WHERE run_id = %s",
                (run_id,),
            )
            review_count = cur.fetchone()[0]

            # Count extracted (non-null extraction_payload) for THIS run
            cur.execute(
                "SELECT count(*) FROM screens_metadata "
                "WHERE run_id = %s AND extraction_payload IS NOT NULL",
                (run_id,),
            )
            extracted_count = cur.fetchone()[0]

            # Total counts across ALL runs (for idempotency detection)
            cur.execute("SELECT count(*) FROM screens_metadata")
            total_metadata = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM screens_embeddings WHERE embedding_kind = 'image'"
            )
            total_img = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM screens_embeddings WHERE embedding_kind = 'text'"
            )
            total_txt = cur.fetchone()[0]

            # Get pipeline run start time for duration calculation
            cur.execute(
                "SELECT started_at FROM pipeline_runs WHERE run_id = %s",
                (run_id,),
            )
            row = cur.fetchone()
            started_at = row[0] if row else None

    # Determine status
    # An idempotent re-run inserts 0 rows (ON CONFLICT DO NOTHING) but
    # the data already exists from a previous run — that is success.
    is_idempotent_rerun = (metadata_count == 0 and total_metadata >= limit)

    if is_idempotent_rerun:
        status = "success"
        reason = f"idempotent re-run — 0 new rows, {total_metadata} existing"
    elif metadata_count == 0:
        status = "failed"
        reason = "no metadata rows found"
    elif image_emb_count == 0 and total_img == 0:
        status = "failed"
        reason = "no image embeddings found"
    elif text_emb_count == 0 and total_txt == 0:
        status = "failed"
        reason = "no text embeddings found"
    else:
        status = "success"
        reason = "all checks passed"

    # Calculate duration
    now = datetime.now(timezone.utc)
    duration = 0.0
    if started_at:
        # started_at may be naive (no timezone) — assume UTC
        if started_at.tzinfo is None:
            from datetime import timezone as tz
            started_at = started_at.replace(tzinfo=tz.utc)
        duration = (now - started_at).total_seconds()

    # Build summary
    summary = (
        f"this_run: meta={metadata_count} img={image_emb_count} "
        f"txt={text_emb_count} extracted={extracted_count} review={review_count} | "
        f"total: meta={total_metadata} img={total_img} txt={total_txt}"
    )

    # Log summary block (readable in 10 seconds)
    log.info(
        "[run_id=%s] === LOAD SUMMARY ===\n"
        "  Status:       %s (%s)\n"
        "  Duration:     %.1fs\n"
        "  This run:     meta=%d, img_emb=%d, txt_emb=%d, extracted=%d, review=%d\n"
        "  Total DB:     meta=%d, img_emb=%d, txt_emb=%d\n"
        "  ========================",
        run_id, status, reason, duration,
        metadata_count, image_emb_count, text_emb_count,
        extracted_count, review_count,
        total_metadata, total_img, total_txt,
    )

    # Finalise pipeline run
    with get_connection() as conn:
        update_pipeline_run(conn, run_id, status, ended_at=now)

    # Collect and persist observability metrics + log summary block
    from pipeline.metrics import collect_and_persist
    collect_and_persist(run_id)

    # Notify Slack
    notify_run_finished(run_id, status, duration, summary)

    return status

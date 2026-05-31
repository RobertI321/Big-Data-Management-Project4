"""Audit circuit breaker for the RICO pipeline.

Checks for duplicate keys in screens_metadata and screens_embeddings.
If any duplicates are found, the audit halts the pipeline by raising
AirflowFailException — a log warning is NOT a circuit breaker.
"""

import json
import logging
from datetime import datetime, timezone
from uuid import UUID

from airflow.exceptions import AirflowFailException

from pipeline.db import get_connection
from pipeline.notify import notify_audit_failed

log = logging.getLogger(__name__)


def _find_duplicate_embeddings(cur) -> list[dict]:
    """Find (screen_id, model_name, model_version, embedding_kind) appearing more than once."""
    cur.execute(
        """
        SELECT screen_id, model_name, model_version, embedding_kind, count(*) AS cnt
        FROM screens_embeddings
        GROUP BY screen_id, model_name, model_version, embedding_kind
        HAVING count(*) > 1
        """
    )
    return [
        {
            "table": "screens_embeddings",
            "screen_id": row[0],
            "model_name": row[1],
            "model_version": row[2],
            "embedding_kind": row[3],
            "count": row[4],
        }
        for row in cur.fetchall()
    ]


def _find_duplicate_metadata(cur) -> list[dict]:
    """Find screen_id appearing more than once in screens_metadata."""
    cur.execute(
        """
        SELECT screen_id, count(*) AS cnt
        FROM screens_metadata
        GROUP BY screen_id
        HAVING count(*) > 1
        """
    )
    return [
        {
            "table": "screens_metadata",
            "screen_id": row[0],
            "count": row[1],
        }
        for row in cur.fetchall()
    ]


def run_audit(run_id: str | UUID) -> bool:
    """Run duplicate-detection audit and record the result.

    Returns True if audit passed, raises AirflowFailException if it failed.
    """
    run_id = UUID(str(run_id))

    with get_connection() as conn:
        with conn.cursor() as cur:
            duplicates = _find_duplicate_embeddings(cur) + _find_duplicate_metadata(cur)

        if duplicates:
            for dup in duplicates:
                log.error(
                    "[run_id=%s] AUDIT DUPLICATE: %s",
                    run_id,
                    json.dumps(dup, default=str),
                )

            details = {"duplicate_keys": duplicates, "count": len(duplicates)}

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO audit_results (run_id, audit_name, passed, details, checked_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        "duplicate_key_check",
                        False,
                        json.dumps(details, default=str),
                        datetime.now(timezone.utc),
                    ),
                )
            conn.commit()

            try:
                notify_audit_failed(run_id, duplicates)
            except Exception as exc:
                log.warning("[run_id=%s] Slack notification failed (non-fatal): %s", run_id, exc)

            raise AirflowFailException(
                f"AUDIT FAILED: {len(duplicates)} duplicate key(s) detected — pipeline halted"
            )

        # Audit passed
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_results (run_id, audit_name, passed, details, checked_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    "duplicate_key_check",
                    True,
                    json.dumps({}),
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()

    log.info("[run_id=%s] Audit passed — no duplicate keys found", run_id)
    return True

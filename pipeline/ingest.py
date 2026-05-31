"""Ingest screens from HuggingFace RICO-Screen2Words into MinIO + Postgres.

Streams the dataset, uploads PNG + hierarchy JSON to MinIO, and inserts
metadata rows into screens_metadata with run_id and source_fingerprint.
Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.

Reference: week07_notebook.ipynb Section 1.
"""

import itertools
import logging
import os
from io import BytesIO
from uuid import UUID

import boto3
from datasets import load_dataset

from pipeline.db import get_connection
from pipeline.tracing import sha256_fingerprint

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MinIO / S3 helpers
# ---------------------------------------------------------------------------

def _get_s3_client():
    """Return a boto3 S3 client pointed at the MinIO endpoint."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

INSERT_METADATA_SQL = """
    INSERT INTO screens_metadata
        (screen_id, app_package, category, png_path, hierarchy_json_path,
         run_id, source_fingerprint)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (screen_id) DO NOTHING
"""


def run_ingest(run_id: UUID, limit: int) -> int:
    """Stream RICO screens from HuggingFace, store in MinIO + Postgres.

    For each screen:
      1. Re-encode the PIL image to PNG bytes and upload to MinIO.
      2. Upload the view-hierarchy JSON string to MinIO.
      3. INSERT metadata row into screens_metadata with ON CONFLICT DO NOTHING.

    Args:
        run_id: Pipeline run UUID (attached to every row).
        limit: Maximum number of screens to ingest.

    Returns:
        Number of new rows actually inserted (0 on idempotent re-run).
    """
    bucket = os.environ.get("MINIO_BUCKET", "rico-raw")
    s3 = _get_s3_client()

    log.info("[run_id=%s] Streaming RICO-Screen2Words (limit=%d)", run_id, limit)
    ds = load_dataset(
        "rootsautomation/RICO-Screen2Words",
        split="train",
        streaming=True,
        trust_remote_code=True,
    )

    inserted = 0

    with get_connection() as conn:
        for row in itertools.islice(ds, limit):
            sid = int(row["screenId"])
            app_package = row.get("app_package_name", "")
            category = row.get("category", "")
            hierarchy_json = row.get("view_hierarchy", "{}")

            # --- PNG bytes ---------------------------------------------------
            png_buf = BytesIO()
            row["image"].save(png_buf, format="PNG")
            png_bytes = png_buf.getvalue()

            png_key = f"screens/{sid}.png"
            hier_key = f"screens/{sid}.json"

            # Upload to MinIO (overwrites are fine — same bytes)
            s3.put_object(Bucket=bucket, Key=png_key, Body=png_bytes)
            s3.put_object(Bucket=bucket, Key=hier_key,
                          Body=hierarchy_json.encode("utf-8"))

            # --- Postgres ----------------------------------------------------
            fingerprint = sha256_fingerprint(png_bytes)

            with conn.cursor() as cur:
                cur.execute(INSERT_METADATA_SQL, (
                    sid,
                    app_package,
                    category,
                    png_key,
                    hier_key,
                    run_id,
                    fingerprint,
                ))
                # rowcount is 1 if inserted, 0 if conflict (already exists)
                if cur.rowcount > 0:
                    inserted += 1

            conn.commit()
            log.info("[run_id=%s] Ingested screen %d  png=%dB  fp=%s",
                     run_id, sid, len(png_bytes), fingerprint[:16])

    log.info("[run_id=%s] Ingest complete: %d/%d new rows inserted",
             run_id, inserted, limit)
    return inserted

"""Embed parsed screen text with SBERT all-MiniLM-L6-v2.

For each screen in the current run, fetches the hierarchy JSON from
MinIO, derives the text representation, produces a 384-dimensional
text embedding, and inserts into screens_embeddings.

Reference: week07_notebook.ipynb Section 4.
"""

import logging
import os
from uuid import UUID

import boto3

from pipeline.db import get_connection
from pipeline.parse import parse_hierarchy, text_representation
from pipeline.tracing import sha256_fingerprint

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model loading (cached at module level)
# ---------------------------------------------------------------------------
_sbert_model = None


def _load_sbert():
    """Load SBERT all-MiniLM-L6-v2, caching at module level."""
    global _sbert_model
    if _sbert_model is not None:
        return _sbert_model

    from sentence_transformers import SentenceTransformer
    _sbert_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    log.info("SBERT all-MiniLM-L6-v2 loaded")
    return _sbert_model


# ---------------------------------------------------------------------------
# MinIO helper
# ---------------------------------------------------------------------------

def _get_s3_client():
    """Return a boto3 S3 client pointed at MinIO."""
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

INSERT_EMBEDDING_SQL = """
    INSERT INTO screens_embeddings
        (screen_id, model_name, model_version, embedding_kind,
         vector, run_id, source_fingerprint)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (screen_id, model_name, model_version, embedding_kind)
    DO NOTHING
"""


def run_embed_text(run_id: UUID) -> int:
    """Embed text for all screens in the current run with SBERT.

    For each screen:
      1. Fetch hierarchy JSON from MinIO.
      2. Parse into text representation via parse_hierarchy + text_representation.
      3. Encode with SBERT → 384-d L2-normalized vector.
      4. INSERT into screens_embeddings with ON CONFLICT DO NOTHING.

    The source_fingerprint is the SHA-256 of the text bytes (UTF-8 encoded),
    not the PNG bytes — because the input to the embedder is the text.

    Args:
        run_id: Pipeline run UUID.

    Returns:
        Number of new embedding rows inserted.
    """
    bucket = os.environ.get("MINIO_BUCKET", "rico-raw")
    s3 = _get_s3_client()
    sbert = _load_sbert()

    inserted = 0

    with get_connection() as conn:
        from pgvector.psycopg import register_vector
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, hierarchy_json_path FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        for screen_id, hier_path in screens:
            # Derive text from hierarchy JSON
            raw_json = s3.get_object(Bucket=bucket, Key=hier_path)["Body"].read().decode("utf-8")
            elements = parse_hierarchy(raw_json)
            text_rep = text_representation(elements)

            # Encode
            text_bytes = text_rep.encode("utf-8")
            vec_np = sbert.encode([text_rep], normalize_embeddings=True).astype("float32")[0]
            fingerprint = sha256_fingerprint(text_bytes)

            with conn.cursor() as cur:
                cur.execute(INSERT_EMBEDDING_SQL, (
                    screen_id,
                    "SBERT",
                    "all-MiniLM-L6-v2",
                    "text",
                    vec_np,
                    run_id,
                    fingerprint,
                ))
                if cur.rowcount > 0:
                    inserted += 1
            conn.commit()

            log.info("[run_id=%s] Embedded text screen %d: dim=%d, text=%d chars, fp=%s",
                     run_id, screen_id, len(vec_np), len(text_rep), fingerprint[:16])

    log.info("[run_id=%s] Text embedding complete: %d/%d new rows",
             run_id, inserted, len(screens))
    return inserted

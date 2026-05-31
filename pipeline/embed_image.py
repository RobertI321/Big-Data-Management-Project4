"""Embed screen PNGs with CLIP ViT-B-32.

Fetches PNG bytes from MinIO for each screen in the current run,
produces a 512-dimensional image embedding, and inserts into
screens_embeddings with ON CONFLICT DO NOTHING for idempotency.

Reference: week07_notebook.ipynb Section 3.
"""

import logging
import os
from io import BytesIO
from uuid import UUID

import boto3
import numpy as np
import torch
from PIL import Image

from pipeline.db import get_connection
from pipeline.tracing import sha256_fingerprint

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model loading (cached at module level for the task lifetime)
# ---------------------------------------------------------------------------
_clip_model = None
_clip_preprocess = None


def _load_clip():
    """Load CLIP ViT-B-32 model and preprocessing transform.

    Caches at module level so repeated calls within the same task
    don't re-download weights.
    """
    global _clip_model, _clip_preprocess
    if _clip_model is not None:
        return _clip_model, _clip_preprocess

    import open_clip
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32", pretrained="laion2b_s34b_b79k"
    )
    model.eval()
    _clip_model = model
    _clip_preprocess = preprocess
    log.info("CLIP ViT-B-32 loaded")
    return model, preprocess


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


def run_embed_image(run_id: UUID) -> int:
    """Embed PNGs for all screens in the current run with CLIP.

    For each screen:
      1. Fetch PNG bytes from MinIO.
      2. Preprocess and encode with CLIP ViT-B-32 → 512-d vector.
      3. L2-normalize the vector.
      4. INSERT into screens_embeddings with ON CONFLICT DO NOTHING.

    Args:
        run_id: Pipeline run UUID.

    Returns:
        Number of new embedding rows inserted.
    """
    bucket = os.environ.get("MINIO_BUCKET", "rico-raw")
    s3 = _get_s3_client()
    model, preprocess = _load_clip()

    inserted = 0

    with get_connection() as conn:
        # Register pgvector type for psycopg
        from pgvector.psycopg import register_vector
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, png_path FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        for screen_id, png_path in screens:
            # Fetch PNG bytes
            png_bytes = s3.get_object(Bucket=bucket, Key=png_path)["Body"].read()

            # Preprocess and embed
            img = Image.open(BytesIO(png_bytes)).convert("RGB")
            img_tensor = preprocess(img).unsqueeze(0)

            with torch.no_grad():
                vec = model.encode_image(img_tensor)
                vec = vec / vec.norm(dim=-1, keepdim=True)

            vec_np = vec.cpu().numpy().astype("float32")[0]
            fingerprint = sha256_fingerprint(png_bytes)

            with conn.cursor() as cur:
                cur.execute(INSERT_EMBEDDING_SQL, (
                    screen_id,
                    "CLIP",
                    "ViT-B-32",
                    "image",
                    vec_np,
                    run_id,
                    fingerprint,
                ))
                if cur.rowcount > 0:
                    inserted += 1
            conn.commit()

            log.info("[run_id=%s] Embedded image screen %d: dim=%d, fp=%s",
                     run_id, screen_id, len(vec_np), fingerprint[:16])

    log.info("[run_id=%s] Image embedding complete: %d/%d new rows",
             run_id, inserted, len(screens))
    return inserted

"""Recall@k evaluation using a self-test holdout.

For each screen in the current run, uses its text description as a query,
embeds it with SBERT, and searches screens_embeddings for the k nearest
neighbors via pgvector cosine distance. A hit is when the correct screen_id
appears in the top-k results. Computes recall@5 and persists to pipeline_metrics.

The self-test will produce artificially high recall (close to 1.0) because
the query vectors come from the same data that was indexed. This is expected
and acceptable per Project4.pdf §4.
"""

import logging
import os
from datetime import datetime, timezone
from uuid import UUID

import boto3

from pipeline.db import get_connection
from pipeline.parse import parse_hierarchy, text_representation

log = logging.getLogger(__name__)

K = 5

_sbert_model = None


def _load_sbert():
    global _sbert_model
    if _sbert_model is not None:
        return _sbert_model
    from sentence_transformers import SentenceTransformer
    _sbert_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    log.info("SBERT all-MiniLM-L6-v2 loaded for eval")
    return _sbert_model


def _get_s3_client():
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def run_eval(run_id: str | UUID) -> dict[str, float]:
    """Run recall@5 self-test evaluation and persist results."""
    run_id = UUID(str(run_id))
    bucket = os.environ.get("MINIO_BUCKET", "rico-raw")
    s3 = _get_s3_client()
    sbert = _load_sbert()

    with get_connection() as conn:
        from pgvector.psycopg import register_vector
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, hierarchy_json_path FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        hits = 0
        tested = 0

        for screen_id, hier_path in screens:
            raw_json = s3.get_object(Bucket=bucket, Key=hier_path)["Body"].read().decode("utf-8")
            elements = parse_hierarchy(raw_json)
            text_rep = text_representation(elements)

            if not text_rep.strip():
                log.warning("[run_id=%s] eval: screen %d has empty text, skipping", run_id, screen_id)
                continue

            query_vec = sbert.encode([text_rep], normalize_embeddings=True).astype("float32")[0]

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT screen_id
                    FROM screens_embeddings
                    WHERE embedding_kind = 'text'
                    ORDER BY vector <=> %s::vector
                    LIMIT %s
                    """,
                    (query_vec.tolist(), K),
                )
                top_k_ids = [row[0] for row in cur.fetchall()]

            tested += 1
            if screen_id in top_k_ids:
                hits += 1
                log.debug("[run_id=%s] eval: screen %d FOUND in top-%d", run_id, screen_id, K)
            else:
                log.info("[run_id=%s] eval: screen %d NOT in top-%d (got %s)",
                         run_id, screen_id, K, top_k_ids)

        recall = hits / tested if tested > 0 else 0.0

        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            for name, value in [
                ("eval_recall_at_5", recall),
                ("eval_screens_tested", float(tested)),
            ]:
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, recorded_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (run_id, name, value, now),
                )
        conn.commit()

    log.info(
        "[run_id=%s] Eval complete: recall@%d = %.2f (%d/%d screens)",
        run_id, K, recall, hits, tested,
    )
    return {"eval_recall_at_5": recall, "eval_screens_tested": float(tested)}

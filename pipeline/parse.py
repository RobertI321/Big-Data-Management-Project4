"""Parse RICO view-hierarchy JSON into flat text representations.

Fetches hierarchy JSON from MinIO for each screen in the current run,
extracts visible-text elements via iterative DFS, and produces a
reading-order text representation that downstream tasks use for
text embedding and LLM extraction.

Reference: week07_notebook.ipynb Section 2.
"""

import json
import logging
import os
from uuid import UUID

import boto3

from pipeline.db import get_connection

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MinIO / S3 helper
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
# Parsing logic (from notebook Section 2)
# ---------------------------------------------------------------------------

def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    """Iterative DFS over a RICO view-hierarchy JSON.

    Returns a list of (element_type, text, bounds) tuples for nodes
    that have a non-empty text or class attribute.

    Args:
        raw_json: The raw JSON string of the Android view hierarchy.

    Returns:
        List of (element_type, text, (x1, y1, x2, y2)) tuples.
    """
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None

    elements: list[tuple[str, str, tuple[int, int, int, int]]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements


def text_representation(elements: list[tuple[str, str, tuple[int, int, int, int]]]) -> str:
    """Concatenate element texts in reading order (top-to-bottom, left-to-right).

    Args:
        elements: Output of parse_hierarchy().

    Returns:
        Space-joined string of visible texts sorted by (y_top, x_left).
    """
    with_text = [e for e in elements if e[1]]
    in_order = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _, text, _ in in_order)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

UPDATE_PARSED_SQL = """
    UPDATE screens_metadata
       SET updated_at = NOW()
     WHERE screen_id = %s AND run_id = %s
"""


def run_parse(run_id: UUID) -> int:
    """Parse hierarchy JSON for every screen in the current run.

    Fetches hierarchy JSON from MinIO, runs parse_hierarchy +
    text_representation, and stores the text in a temporary format
    that downstream tasks (embed_text, extract) can read from the
    hierarchy JSON in MinIO.

    Note: The parsed text is not stored in a dedicated column since
    the schema doesn't have one. Downstream tasks re-derive it from
    the hierarchy JSON in MinIO (same as the notebook pattern).
    The UPDATE here touches updated_at to mark the row as parsed.

    Args:
        run_id: Pipeline run UUID.

    Returns:
        Number of screens parsed.
    """
    bucket = os.environ.get("MINIO_BUCKET", "rico-raw")
    s3 = _get_s3_client()
    parsed_count = 0

    with get_connection() as conn:
        # Fetch all screens for this run
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, hierarchy_json_path FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        for screen_id, hier_path in screens:
            raw_json = s3.get_object(Bucket=bucket, Key=hier_path)["Body"].read().decode("utf-8")
            elements = parse_hierarchy(raw_json)
            text_rep = text_representation(elements)

            with conn.cursor() as cur:
                cur.execute(UPDATE_PARSED_SQL, (screen_id, run_id))
            conn.commit()

            parsed_count += 1
            log.info("[run_id=%s] Parsed screen %d: %d elements, %d chars text",
                     run_id, screen_id, len(elements), len(text_rep))

    log.info("[run_id=%s] Parse complete: %d screens parsed", run_id, parsed_count)
    return parsed_count

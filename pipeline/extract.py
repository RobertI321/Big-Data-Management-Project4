"""LLM extraction via Ollama — schema-on-ask pattern.

For each screen in the current run, reads the prompt template from
prompts/extract_v1.txt, fills in the parsed text, calls Ollama, and
either updates screens_metadata with the extraction or routes the
failure to screens_review_queue.

Reference: week07_notebook.ipynb Section 5.
"""

import json
import logging
import os
from uuid import UUID

import boto3
import requests as http_requests

from pipeline.db import get_connection
from pipeline.parse import parse_hierarchy, text_representation
from pipeline.tracing import sha256_fingerprint

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt loading
# ---------------------------------------------------------------------------

def _load_prompt_template() -> str:
    """Read the prompt template from prompts/extract_v1.txt.

    Returns:
        The raw template string with a {hierarchy_text} placeholder.
    """
    prompt_path = os.environ.get(
        "PROMPT_PATH",
        os.path.join(os.path.dirname(__file__), "..", "prompts", "extract_v1.txt"),
    )
    with open(prompt_path) as f:
        return f.read()


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
# Ollama call
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str) -> str:
    """Send a prompt to Ollama and return the raw response text.

    Args:
        prompt: The fully rendered prompt string.

    Returns:
        The model's raw response text.

    Raises:
        requests.HTTPError: on non-2xx status from Ollama.
    """
    ollama_url = os.environ.get("OLLAMA_URL", "http://ollama:11434")
    model = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")

    resp = http_requests.post(
        f"{ollama_url}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["response"]


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

UPDATE_EXTRACTION_SQL = """
    UPDATE screens_metadata
       SET extraction_payload = %s::jsonb,
           prompt_version     = %s,
           confidence         = %s,
           updated_at         = NOW()
     WHERE screen_id = %s AND run_id = %s
"""

INSERT_REVIEW_QUEUE_SQL = """
    INSERT INTO screens_review_queue
        (screen_id, reason, raw_output, run_id, source_fingerprint)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_extract(run_id: UUID) -> dict:
    """Run LLM extraction for all screens in the current run.

    For each screen:
      1. Derive text from hierarchy JSON (same as embed_text).
      2. Fill the prompt template and call Ollama.
      3. On valid JSON: update screens_metadata with payload + confidence.
      4. On parse failure: insert into screens_review_queue.

    Args:
        run_id: Pipeline run UUID.

    Returns:
        Dict with keys 'extracted' and 'review_queue' (counts).
    """
    bucket = os.environ.get("MINIO_BUCKET", "rico-raw")
    s3 = _get_s3_client()
    prompt_template = _load_prompt_template()

    extracted = 0
    review_queued = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, hierarchy_json_path, source_fingerprint "
                "FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        for screen_id, hier_path, fingerprint in screens:
            # Derive text
            raw_json = s3.get_object(Bucket=bucket, Key=hier_path)["Body"].read().decode("utf-8")
            elements = parse_hierarchy(raw_json)
            text_rep = text_representation(elements)

            # Fill prompt and call Ollama
            prompt = prompt_template.replace("{hierarchy_text}", text_rep)
            try:
                raw_response = _call_ollama(prompt)
                payload = json.loads(raw_response)

                confidence = float(payload.get("confidence", 0.0))
                # Strip confidence from stored payload (it has its own column)
                body = {k: v for k, v in payload.items() if k != "confidence"}

                with conn.cursor() as cur:
                    cur.execute(UPDATE_EXTRACTION_SQL, (
                        json.dumps(body),
                        "v1",
                        confidence,
                        screen_id,
                        run_id,
                    ))
                conn.commit()
                extracted += 1
                log.info("[run_id=%s] Extracted screen %d: confidence=%.2f",
                         run_id, screen_id, confidence)

            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                # Route to review queue
                raw_out = raw_response if "raw_response" in dir() else str(exc)
                with conn.cursor() as cur:
                    cur.execute(INSERT_REVIEW_QUEUE_SQL, (
                        screen_id,
                        f"JSONDecodeError: {exc}",
                        raw_out[:2000],
                        run_id,
                        fingerprint,
                    ))
                conn.commit()
                review_queued += 1
                log.warning("[run_id=%s] Screen %d → review queue: %s",
                            run_id, screen_id, exc)

            except Exception as exc:
                # Ollama HTTP error or timeout — also route to review queue
                with conn.cursor() as cur:
                    cur.execute(INSERT_REVIEW_QUEUE_SQL, (
                        screen_id,
                        f"OllamaError: {exc}",
                        str(exc)[:2000],
                        run_id,
                        fingerprint,
                    ))
                conn.commit()
                review_queued += 1
                log.warning("[run_id=%s] Screen %d → review queue (Ollama error): %s",
                            run_id, screen_id, exc)

    log.info("[run_id=%s] Extraction complete: %d extracted, %d → review queue",
             run_id, extracted, review_queued)
    return {"extracted": extracted, "review_queue": review_queued}

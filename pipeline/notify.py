"""Slack notification helpers for the RICO pipeline.

All functions follow the same safety contract: a failure to post to Slack
must NEVER raise an exception or fail the pipeline. Every outbound call
is wrapped in try/except, logs a warning on failure, and returns silently.
"""

import json
import logging
import os
from uuid import UUID

import requests

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base function
# ---------------------------------------------------------------------------

def post_to_slack(message: str) -> None:
    """Post a plain-text message to the configured Slack webhook.

    Reads SLACK_WEBHOOK_URL from the environment. If the variable is empty
    or unset, the call is skipped with a debug log. If the HTTP request
    fails for any reason, a warning is logged and the function returns
    silently — it never raises.

    Args:
        message: The text payload to send to Slack.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        log.debug("SLACK_WEBHOOK_URL not set, skipping notification")
        return

    try:
        resp = requests.post(
            webhook_url,
            json={"text": message},
            timeout=5,
        )
        if resp.status_code != 200:
            log.warning("Slack returned HTTP %s: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("Slack notification failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# Typed message helpers
# ---------------------------------------------------------------------------

def notify_run_started(
    run_id: UUID,
    limit: int,
    trigger_type: str = "manual",
) -> None:
    """Notify Slack that a pipeline run has started.

    Args:
        run_id: The UUID for this pipeline run.
        limit: Number of screens being processed.
        trigger_type: How the run was triggered ('manual' or 'scheduled').
    """
    message = (
        f":rocket: *Pipeline run started*\n"
        f"  run_id: `{run_id}`\n"
        f"  LIMIT: {limit}\n"
        f"  trigger: {trigger_type}"
    )
    post_to_slack(message)
    log.info("[run_id=%s] Slack: run_started (limit=%d, trigger=%s)",
             run_id, limit, trigger_type)


def notify_audit_failed(
    run_id: UUID,
    duplicate_keys: list | dict,
) -> None:
    """Notify Slack that the audit circuit breaker has fired.

    This is the most important notification — an on-call engineer should
    be able to tell from this message alone whether to investigate now
    or wait until morning.

    Args:
        run_id: The UUID of the failed pipeline run.
        duplicate_keys: The duplicate keys detected by the audit.
    """
    details = json.dumps(duplicate_keys, indent=2, default=str)
    # Truncate if too long for Slack
    if len(details) > 1500:
        details = details[:1500] + "\n... (truncated)"

    message = (
        f":rotating_light: *AUDIT FAILED — pipeline halted*\n"
        f"  run_id: `{run_id}`\n"
        f"  Duplicate keys detected:\n```\n{details}\n```\n"
        f"  Action: check Airflow task logs for full details."
    )
    post_to_slack(message)
    log.info("[run_id=%s] Slack: audit_failed", run_id)


def notify_run_finished(
    run_id: UUID,
    status: str,
    duration_seconds: float,
    summary: str = "",
) -> None:
    """Notify Slack that a pipeline run has completed.

    Args:
        run_id: The UUID of the completed pipeline run.
        status: Final status ('success', 'failed', or 'audit_failed').
        duration_seconds: Total wall-clock time of the run in seconds.
        summary: One-line health + data quality summary from metrics.
    """
    emoji = {
        "success": ":white_check_mark:",
        "failed": ":x:",
        "audit_failed": ":rotating_light:",
    }.get(status, ":question:")

    duration_str = f"{duration_seconds:.1f}s"

    message = (
        f"{emoji} *Pipeline run finished*\n"
        f"  run_id: `{run_id}`\n"
        f"  status: *{status}*\n"
        f"  duration: {duration_str}"
    )
    if summary:
        message += f"\n  summary: {summary}"

    post_to_slack(message)
    log.info("[run_id=%s] Slack: run_finished (status=%s, duration=%s)",
             run_id, status, duration_str)

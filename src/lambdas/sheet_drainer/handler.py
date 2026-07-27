"""
sheet_drainer — AWS Lambda
==========================================================================
Drains EVERY tab of the DataMoon Google Sheet into Amazon S3, then clears
the drained rows from each tab — so the sheet starts each day empty and
ready for the next batch.

THE NO-DATA-LOSS RULE
---------------------
For each tab we follow a strict order:

    1. READ    all data rows from the tab
    2. WRITE   those rows to S3 (compressed JSON Lines)
    3. VERIFY  the S3 object really exists, right size and row count
    4. DELETE  those exact rows from the tab  <-- ONLY after verify passes

Because we delete ONLY after S3 confirms the write, a crash at any point
can never lose data: if we die before step 4, the rows are still in the
sheet and simply get re-read next run (they may briefly appear twice in S3,
which is fine — the ETL step deduplicates).

Row 1 of each tab is treated as a header and is kept in place; only the
data rows beneath it are removed.

This file runs both:
  * inside AWS Lambda (handler = lambda_handler), and
  * locally for testing (python handler.py --dry-run).

Configuration comes from environment variables (set at deploy time):
    S3_BUCKET            target bucket name
    S3_RAW_PREFIX        e.g. "raw"
    SHEET_ID             the Google Sheet ID
    GOOGLE_SECRET_NAME   Secrets Manager name holding the service-account JSON
    SKIP_TABS            optional comma-separated tab names to leave alone
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Configuration (env vars, with safe local-test defaults) ----------------
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_RAW_PREFIX = os.environ.get("S3_RAW_PREFIX", "raw")
SHEET_ID = os.environ.get("SHEET_ID", "")
GOOGLE_SECRET_NAME = os.environ.get("GOOGLE_SECRET_NAME", "datamoon/google-service-account")
SKIP_TABS = {t.strip() for t in os.environ.get("SKIP_TABS", "").split(",") if t.strip()}
# Step 2. Set to "" to disable the hand-off.
NORMALIZER_FUNCTION = os.environ.get("NORMALIZER_FUNCTION", "datamoon-normalizer")

# The first row of each tab is assumed to be a header row we keep.
HEADER_ROWS = 1


# ==========================================================================
# Google Sheets access
# ==========================================================================
def _get_sheets_service():
    """Build an authenticated Google Sheets API client.

    Credentials (a service-account JSON) are pulled from AWS Secrets Manager,
    never stored in the repo. The service account must have Editor access to
    the sheet (share the sheet with the service account's email).
    """
    import boto3
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    sm = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=GOOGLE_SECRET_NAME)
    creds_info = json.loads(secret["SecretString"])

    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _list_tabs(service) -> list[dict]:
    """Return [{title, gid}] for every tab in the spreadsheet, in sheet order."""
    meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    return [
        {"title": s["properties"]["title"], "gid": s["properties"]["sheetId"]}
        for s in meta.get("sheets", [])
    ]


def _quote_tab(title: str) -> str:
    """A1-notation range for a whole tab. Titles with spaces/commas need quoting,
    and a literal single quote inside the title is escaped by doubling it."""
    return "'" + title.replace("'", "''") + "'"


def _read_tab(service, title: str) -> tuple[list[str], list[list]]:
    """Read a tab's header and all data rows beneath it."""
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=_quote_tab(title))
        .execute()
    )
    values = resp.get("values", [])
    if not values:
        return [], []
    return values[0], values[1:]


def _delete_rows(service, gid: int, first_row_1based: int, last_row_1based: int) -> None:
    """Delete rows [first, last] (1-based, inclusive) from one tab.

    We delete by absolute row index so we remove exactly the rows we already
    saved to S3 — never more.
    """
    if last_row_1based < first_row_1based:
        return
    request = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": gid,
                        "dimension": "ROWS",
                        # API is 0-based, end-exclusive:
                        "startIndex": first_row_1based - 1,
                        "endIndex": last_row_1based,
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body=request).execute()


# ==========================================================================
# S3 write + verify
# ==========================================================================
def _rows_to_jsonl_gz(header: list[str], rows: list[list], batch_id: str, tab: str) -> bytes:
    """Turn rows into gzip-compressed JSON Lines (one JSON object per row)."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for row in rows:
            # Pad short rows so every column lines up with the header.
            padded = row + [""] * (len(header) - len(row))
            record = dict(zip(header, padded))
            record["_batch_id"] = batch_id
            record["_source_tab"] = tab
            gz.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
    return buf.getvalue()


def _slug(title: str) -> str:
    """Filesystem/S3-safe version of a tab name, for use in the object key."""
    s = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-").lower()
    return s or "sheet"


def _s3_key(now: datetime, tab: str, batch_id: str) -> str:
    """Partitioned key: raw/dt=YYYY-MM-DD/sheet=<slug>/<batch_id>.jsonl.gz"""
    return f"{S3_RAW_PREFIX}/dt={now:%Y-%m-%d}/sheet={_slug(tab)}/{batch_id}.jsonl.gz"


def _write_and_verify(body: bytes, key: str, expected_rows: int) -> None:
    """Write to S3, then VERIFY the object exists with the right size/row count.

    Raises if verification fails, which means we will NOT delete from the
    sheet — the safe outcome.
    """
    import boto3

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
        # Record the row count as metadata so verification is explicit.
        Metadata={"rows": str(expected_rows)},
    )
    head = s3.head_object(Bucket=S3_BUCKET, Key=key)
    if head["ContentLength"] != len(body):
        raise RuntimeError(
            f"S3 verify failed: size mismatch for {key} "
            f"({head['ContentLength']} != {len(body)})"
        )
    if int(head["Metadata"].get("rows", -1)) != expected_rows:
        raise RuntimeError(f"S3 verify failed: row-count metadata mismatch for {key}")
    logger.info("Verified s3://%s/%s (%d rows, %d bytes)", S3_BUCKET, key, expected_rows, len(body))


# ==========================================================================
# Hand-off to step 2
# ==========================================================================
def _trigger_normalizer(now: datetime, summary: dict) -> None:
    """Kick off the normalizer for the day we just wrote into.

    Failure to trigger is logged but never fatal: the rows are already safely
    in S3, and the normalizer can be re-run by hand for any date.
    """
    import boto3

    try:
        boto3.client("lambda").invoke(
            FunctionName=NORMALIZER_FUNCTION,
            InvocationType="Event",  # fire-and-forget
            Payload=json.dumps({
                "source_dt": f"{now:%Y-%m-%d}",
                "triggered_by": summary["batch_id"],
            }).encode("utf-8"),
        )
        logger.info("Triggered %s for dt=%s", NORMALIZER_FUNCTION, f"{now:%Y-%m-%d}")
    except Exception:
        logger.exception("Could not trigger %s — run it manually", NORMALIZER_FUNCTION)


# ==========================================================================
# Lambda entry point
# ==========================================================================
def lambda_handler(event, context):
    """Drain every tab. EventBridge Scheduler calls this daily at 9:30am ET."""
    now = datetime.now(timezone.utc)
    batch_id = uuid.uuid4().hex

    service = _get_sheets_service()
    tabs = _list_tabs(service)
    logger.info("Found %d tabs: %s", len(tabs), [t["title"] for t in tabs])

    results = []
    total = 0
    failures = []

    for tab in tabs:
        title, gid = tab["title"], tab["gid"]
        if title in SKIP_TABS:
            logger.info("Skipping tab '%s' (in SKIP_TABS)", title)
            continue
        try:
            header, rows = _read_tab(service, title)
            if not rows:
                logger.info("Tab '%s' has no data rows — nothing to drain.", title)
                results.append({"tab": title, "rows": 0})
                continue

            key = _s3_key(now, title, f"{batch_id}-{_slug(title)}")
            body = _rows_to_jsonl_gz(header, rows, batch_id, title)

            # STEP 2 + 3: write and verify BEFORE touching the sheet.
            _write_and_verify(body, key, expected_rows=len(rows))

            # STEP 4: only now is it safe to delete the rows we just saved.
            _delete_rows(service, gid, HEADER_ROWS + 1, HEADER_ROWS + len(rows))

            logger.info("Drained %d rows from '%s' -> s3://%s/%s", len(rows), title, S3_BUCKET, key)
            results.append({"tab": title, "rows": len(rows), "s3_key": key})
            total += len(rows)
        except Exception as exc:  # keep draining the other tabs
            logger.exception("Tab '%s' failed to drain", title)
            failures.append({"tab": title, "error": str(exc)})
            results.append({"tab": title, "error": str(exc)})

    summary = {"batch_id": batch_id, "total_rows": total, "tabs": results}

    # Hand off to STEP 2 (normalize + dedup). Async, so a slow normalize run
    # can't time out the drain, and read-only on raw/ so it can be re-run.
    if total and NORMALIZER_FUNCTION:
        _trigger_normalizer(now, summary)

    if failures:
        # Surface the failure so the invocation is marked failed and alarms fire.
        # Tabs that succeeded are already safely in S3 and cleared.
        raise RuntimeError(f"{len(failures)} tab(s) failed: {json.dumps(summary)}")
    return summary


# ==========================================================================
# Local testing:  python handler.py --dry-run
# ==========================================================================
if __name__ == "__main__":
    import sys

    if "--dry-run" in sys.argv:
        # Exercise the pure (no-AWS, no-Google) transform logic on fake data.
        fake_header = ["email", "phone", "name"]
        fake_rows = [
            ["Alice@Example.com ", "(212) 555-0101", "Alice"],
            ["bob@example.com", "212-555-0102", "Bob"],
            ["carol@example.com"],  # short row — should be padded
        ]
        tab = "Quick Business & Loans"
        blob = _rows_to_jsonl_gz(fake_header, fake_rows, "test-batch", tab)
        print("Compressed bytes:", len(blob))
        print("Decoded records:")
        for line in gzip.decompress(blob).decode("utf-8").splitlines():
            print("  ", line)
        print("Range notation:", _quote_tab("Business Loans,Line Of Credit's"))
        print("Example S3 key:", _s3_key(datetime.now(timezone.utc), tab, "test-batch"))
    else:
        print("Run with --dry-run for a local, no-AWS test.")

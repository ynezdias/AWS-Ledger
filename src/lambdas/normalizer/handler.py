"""
normalizer — AWS Lambda   (STEP 2 of the pipeline)
==========================================================================
Reads a day's RAW files from S3, cleans + normalizes + internally
deduplicates them, and writes ONE combined CSV into the
"Normalized DataMoon/" folder of the same bucket.

READ-ONLY ON RAW
----------------
This job NEVER writes to, moves, or deletes anything under raw/. The raw
drop from the Google Sheet stays exactly as it landed. Everything this job
produces goes to a separate prefix.

WHAT IT DOES  (mirrors "STEP 1 - NORMALIZE & INTERNAL DEDUP" in the
DataMoon quality_report.txt files — no rows dropped for missing fields)
    1. READ      every raw/dt=<DT>/ object (all sheet tabs) as JSON Lines
    2. CLEAN     trim whitespace, collapse blanks/placeholders to empty
    3. NORMALIZE emails lowercased, phones -> +1XXXXXXXXXX, state upper,
                 zip -> 5 digits, names title-cased
    4. KEY       compute the THREE independent match keys
                 (email / phone / name+address)
    5. DEDUP     merge duplicates WITHIN this batch, keeping the most
                 complete row. Incomplete rows are KEPT and flagged, never
                 dropped. Rows with no key at all are never merged away.
    6. WRITE     one CSV -> "Normalized DataMoon/dt=<DT>/normalized_<DT>.csv"
                 plus a reconciliation report under .../_reports/

Every original source column is preserved in the output. Normalized values
are ADDED as new `norm_*` columns rather than overwriting the originals, so
nothing you received is lost.

Configuration (environment variables):
    S3_BUCKET            bucket holding both raw/ and the output folder
    S3_RAW_PREFIX        e.g. "raw"
    S3_NORMALIZED_PREFIX e.g. "Normalized DataMoon"
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_RAW_PREFIX = os.environ.get("S3_RAW_PREFIX", "raw")
S3_NORMALIZED_PREFIX = os.environ.get("S3_NORMALIZED_PREFIX", "Normalized DataMoon")

# Provenance columns the drainer adds; kept but listed last.
META_COLS = ["_batch_id", "_source_tab"]

# Canonical DataMoon column order (58-col full export). Any column not in this
# list still gets written — it is appended, sorted, after the known ones.
CANONICAL_ORDER = [
    "sha256_lc_hem", "score_category", "first_name", "last_name",
    "personal_emails", "personal_phone", "mobile_phone",
    "additional_personal_emails", "personal_emails_validation_status",
    "personal_emails_last_seen", "linkedin_url", "personal_address",
    "personal_address_2", "personal_city", "personal_state", "personal_zip",
    "personal_zip4", "contact_country", "gender", "age_range", "married",
    "children", "income_range", "net_worth", "homeowner", "job_title",
    "job_title_normalized", "job_title_last_updated", "seniority_level",
    "seniority_level_2", "department", "department_2", "business_email",
    "programmatic_business_emails", "business_email_validation_status",
    "business_email_last_seen", "direct_number", "professional_address",
    "professional_address_2", "professional_city", "professional_state",
    "professional_zip", "professional_zip4", "company_name", "company_domain",
    "company_phone", "company_sic", "company_naics", "company_address",
    "company_city", "company_state", "company_zip", "company_country",
    "company_revenue", "company_employee_count", "primary_industry",
    "company_linkedin_url", "company_last_updated", "last_updated",
]

# Columns this job derives. Appended after the source columns.
DERIVED_COLS = [
    "norm_email", "norm_personal_phone", "norm_mobile_phone",
    "norm_first_name", "norm_last_name", "norm_address", "norm_city",
    "norm_state", "norm_zip",
    "email_key", "phone_key", "nameaddr_key",
    "is_complete", "has_valid_phone", "has_valid_email",
    "duplicates_merged", "merged_from_rows", "source_dt", "row_id",
]

# Values that mean "empty" in DataMoon exports.
NULLISH = {"", "n/a", "na", "null", "none", "-", "unknown", "#n/a"}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ==========================================================================
# Field-level cleaning
# ==========================================================================
def clean(value) -> str:
    """Trim, collapse inner whitespace, and turn placeholder junk into ''."""
    if value is None:
        return ""
    s = str(value).replace(" ", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return "" if s.lower() in NULLISH else s


def norm_email(value: str) -> str:
    """First address from a possibly multi-valued field, lowercased.

    DataMoon packs several addresses into personal_emails separated by
    commas/semicolons/spaces. Anything that isn't shaped like an email is
    discarded rather than carried forward as noise.
    """
    if not value:
        return ""
    first = re.split(r"[;,\s]+", value)[0].strip().lower()
    return first if EMAIL_RE.match(first) else ""


def norm_phone(value: str) -> str:
    """US phone -> +1XXXXXXXXXX, or '' if it isn't a usable 10-digit number."""
    if not value:
        return ""
    d = re.sub(r"\D", "", value)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) != 10:
        return ""
    # Area code and exchange code can't start with 0 or 1 in the NANP.
    if d[0] in "01" or d[3] in "01":
        return ""
    return "+1" + d


def norm_zip(value: str) -> str:
    """5-digit ZIP, zero-padded. ZIP+4 is truncated to the base 5."""
    if not value:
        return ""
    d = re.sub(r"\D", "", value)
    if len(d) > 5:
        d = d[:5]
    return d.zfill(5) if d else ""


def norm_name(value: str) -> str:
    """Title-case a person name. Letters after an apostrophe or hyphen also
    capitalize, so o'brien -> O'Brien and mary-jane -> Mary-Jane."""
    if not value:
        return ""
    return re.sub(r"[A-Za-z]+", lambda m: m.group(0).capitalize(), value.lower())


def norm_address(value: str) -> str:
    """Upper-case, punctuation-stripped street address for match-key use."""
    if not value:
        return ""
    s = re.sub(r"[^A-Za-z0-9 ]", " ", value.upper())
    return re.sub(r"\s+", " ", s).strip()


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ==========================================================================
# Row transformation
# ==========================================================================
def normalize_row(raw: dict, source_dt: str, row_id: int) -> dict:
    """Copy the source row untouched, then append derived/normalized fields."""
    out = {k: clean(v) for k, v in raw.items()}

    email = norm_email(out.get("personal_emails", "")) or norm_email(
        out.get("business_email", "")
    )
    pphone = norm_phone(out.get("personal_phone", ""))
    mphone = norm_phone(out.get("mobile_phone", ""))
    first = norm_name(out.get("first_name", ""))
    last = norm_name(out.get("last_name", ""))
    addr = norm_address(out.get("personal_address", ""))
    city = norm_name(out.get("personal_city", ""))
    state = out.get("personal_state", "").upper()[:2]
    zipc = norm_zip(out.get("personal_zip", ""))

    # --- The three INDEPENDENT match keys ---------------------------------
    # email_key: prefer DataMoon's own hashed email so our keys line up with
    # the AWS lead pool; otherwise hash our normalized address.
    hem = out.get("sha256_lc_hem", "").lower()
    email_key = hem if hem else (sha256(email) if email else "")
    # phone_key: personal phone first, mobile as fallback. Last 10 digits.
    phone = pphone or mphone
    phone_key = phone[2:] if phone else ""
    # nameaddr_key: fallback only, needs a last name AND a street address.
    nameaddr_key = sha256(f"{last}|{addr}|{zipc}") if (last and addr) else ""

    out.update({
        "norm_email": email,
        "norm_personal_phone": pphone,
        "norm_mobile_phone": mphone,
        "norm_first_name": first,
        "norm_last_name": last,
        "norm_address": addr,
        "norm_city": city,
        "norm_state": state,
        "norm_zip": zipc,
        "email_key": email_key,
        "phone_key": phone_key,
        "nameaddr_key": nameaddr_key,
        # "complete" = name AND address AND email, matching the quality reports.
        "is_complete": str(bool(last and addr and email_key)).lower(),
        "has_valid_phone": str(bool(phone)).lower(),
        "has_valid_email": str(bool(email_key)).lower(),
        "duplicates_merged": "0",
        "merged_from_rows": "",
        "source_dt": source_dt,
        "row_id": str(row_id),
    })
    return out


def completeness_score(row: dict) -> int:
    """Higher = keep this row when merging duplicates."""
    return (
        (row["is_complete"] == "true") * 8
        + bool(row["email_key"]) * 4
        + bool(row["phone_key"]) * 2
        + bool(row["nameaddr_key"])
        + sum(1 for v in row.values() if v) // 10  # tie-break: more filled fields
    )


def internal_dedup(rows: list[dict]) -> tuple[list[dict], int]:
    """Merge duplicates within the batch. Returns (unique_rows, dup_count).

    Two rows are the same person if they share ANY of the three keys — email
    OR phone, with name+address as the fallback — exactly the rule in the
    quality_report.txt files. That means matching is TRANSITIVE: row A keyed
    only by email and row B keyed only by phone still collapse together if
    some row C carries both. A simple "first available key" lookup would miss
    that, so we group with union-find instead.

    Rows with NO key at all are always kept — we never silently drop a record
    for missing fields.

    Within a group the most complete row wins, and any field it left blank is
    back-filled from its duplicates, so merging never loses a value that was
    present somewhere in the batch.
    """
    parent = list(range(len(rows)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    # Link every row to the first row that shared any one of its keys.
    seen: dict[tuple[str, str], int] = {}
    keyless: list[dict] = []
    for i, row in enumerate(rows):
        keys = [(kind, row[kind]) for kind in ("email_key", "phone_key", "nameaddr_key")
                if row[kind]]
        if not keys:
            keyless.append(row)
            continue
        for k in keys:
            if k in seen:
                union(seen[k], i)
            else:
                seen[k] = i

    groups: dict[int, list[int]] = {}
    for i, row in enumerate(rows):
        if any(row[k] for k in ("email_key", "phone_key", "nameaddr_key")):
            groups.setdefault(find(i), []).append(i)

    unique: list[dict] = []
    dup_count = 0
    for members in groups.values():
        ranked = sorted(members, key=lambda i: completeness_score(rows[i]), reverse=True)
        winner = rows[ranked[0]]
        dup_count += len(members) - 1

        for other_idx in ranked[1:]:
            other = rows[other_idx]
            for field, value in other.items():
                if value and not winner.get(field):
                    winner[field] = value

        # Back-filling can add an email/address the winner lacked, so the
        # completeness flags have to be recomputed rather than inherited.
        winner["has_valid_email"] = str(bool(winner["email_key"])).lower()
        winner["has_valid_phone"] = str(bool(winner["phone_key"])).lower()
        winner["is_complete"] = str(bool(
            winner["norm_last_name"] and winner["norm_address"] and winner["email_key"]
        )).lower()

        winner["duplicates_merged"] = str(len(members) - 1)
        winner["merged_from_rows"] = ",".join(str(rows[i]["row_id"]) for i in sorted(members))
        tabs = sorted({rows[i].get("_source_tab", "") for i in members} - {""})
        if tabs:
            winner["_source_tab"] = " | ".join(tabs)
        unique.append(winner)

    for row in keyless:
        row["merged_from_rows"] = row["row_id"]

    unique.extend(keyless)
    unique.sort(key=lambda r: int(r["row_id"]))
    return unique, dup_count


# ==========================================================================
# S3 read (raw is opened read-only and never modified)
# ==========================================================================
def read_raw_day(s3, source_dt: str) -> tuple[list[dict], list[str]]:
    """Load every JSON Lines object under raw/dt=<DT>/ across all sheet tabs."""
    prefix = f"{S3_RAW_PREFIX}/dt={source_dt}/"
    rows: list[dict] = []
    keys: list[str] = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl.gz"):
                continue
            body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            text = gzip.decompress(body).decode("utf-8")
            for line in text.splitlines():
                if line.strip():
                    rows.append(json.loads(line))
            keys.append(key)
    return rows, keys


# ==========================================================================
# Output
# ==========================================================================
def build_header(rows: list[dict]) -> list[str]:
    """Canonical columns first, then any unexpected ones, then derived+meta."""
    seen = set().union(*(r.keys() for r in rows)) if rows else set()
    source = [c for c in CANONICAL_ORDER if c in seen]
    extra = sorted(seen - set(CANONICAL_ORDER) - set(DERIVED_COLS) - set(META_COLS))
    return source + extra + DERIVED_COLS + [c for c in META_COLS if c in seen]


def rows_to_csv(rows: list[dict], header: list[str]) -> bytes:
    """One combined CSV for the whole day, UTF-8 with a BOM so Excel opens it."""
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=header, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({h: row.get(h, "") for h in header})
    return buf.getvalue().encode("utf-8-sig")


def build_report(source_dt: str, source_keys: list[str], rows_in: int,
                 dup_count: int, unique: list[dict], out_key: str) -> str:
    """Reconciliation report in the same shape as the quality_report.txt files."""
    complete = sum(1 for r in unique if r["is_complete"] == "true")
    no_phone = sum(1 for r in unique if r["has_valid_phone"] == "false")
    no_email = sum(1 for r in unique if r["has_valid_email"] == "false")
    balanced = "BALANCED" if rows_in == dup_count + len(unique) else "MISMATCH"
    sources = "\n".join(f"    s3://{S3_BUCKET}/{k}" for k in source_keys) or "    (none)"

    return f"""DataMoon - Normalization & Dedup Report
Generated: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC
Source dt: {source_dt}
Source files (read-only, unmodified):
{sources}
================================================================

STEP 1 - NORMALIZE & INTERNAL DEDUP (no rows dropped for missing fields)
  Rows in:                                  {rows_in}
  Removed - internal duplicates:            {dup_count}
  Unique records:                           {len(unique)}
  Flagged incomplete (missing name/addr/email): {len(unique) - complete}
  Rows with no valid personal phone:        {no_phone}
  Rows with no valid email:                 {no_email}

  Match rule: personal email OR personal phone, fallback name+street address.
  Matching is transitive, so duplicates collapse across sheet tabs and across
  both DataMoon export layouts. When rows merge, the most complete row is kept
  and its blank fields are back-filled from its duplicates -- see the
  merged_from_rows column for which source rows went into each record.

OUTPUT
  s3://{S3_BUCKET}/{out_key}

RECONCILIATION
  {rows_in} in = {dup_count} internal-dup + {len(unique)} unique
  Sum of buckets: {dup_count + len(unique)}  ({balanced})

NEXT STEP
  Overlap analysis against the AWS lead pool (STEP 2 in the DataMoon
  quality reports) has NOT been applied yet.
"""


# ==========================================================================
# Lambda entry point
# ==========================================================================
def lambda_handler(event, context):
    """Normalize one day's raw drop. Safe to re-run — output is overwritten."""
    import boto3  # provided by the Lambda runtime; imported here so the
                  # --dry-run path works on a machine without it installed.

    event = event or {}
    source_dt = event.get("source_dt") or f"{datetime.now(timezone.utc):%Y-%m-%d}"

    s3 = boto3.client("s3")
    raw_rows, source_keys = read_raw_day(s3, source_dt)
    logger.info("Read %d raw rows from %d object(s) for dt=%s",
                len(raw_rows), len(source_keys), source_dt)

    if not raw_rows:
        logger.info("Nothing to normalize for dt=%s", source_dt)
        return {"source_dt": source_dt, "rows_in": 0, "unique": 0}

    normalized = [normalize_row(r, source_dt, i) for i, r in enumerate(raw_rows)]
    unique, dup_count = internal_dedup(normalized)

    header = build_header(unique)
    out_key = f"{S3_NORMALIZED_PREFIX}/dt={source_dt}/normalized_{source_dt}.csv"
    body = rows_to_csv(unique, header)

    s3.put_object(
        Bucket=S3_BUCKET, Key=out_key, Body=body,
        ContentType="text/csv; charset=utf-8",
        Metadata={"rows": str(len(unique)), "rows-in": str(len(raw_rows)),
                  "internal-dups": str(dup_count)},
    )

    report_key = f"{S3_NORMALIZED_PREFIX}/_reports/dt={source_dt}/quality_report.txt"
    s3.put_object(
        Bucket=S3_BUCKET, Key=report_key,
        Body=build_report(source_dt, source_keys, len(raw_rows),
                          dup_count, unique, out_key).encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )

    result = {
        "source_dt": source_dt,
        "source_objects": len(source_keys),
        "rows_in": len(raw_rows),
        "internal_duplicates": dup_count,
        "unique": len(unique),
        "complete": sum(1 for r in unique if r["is_complete"] == "true"),
        "output": f"s3://{S3_BUCKET}/{out_key}",
        "report": f"s3://{S3_BUCKET}/{report_key}",
    }
    logger.info("Normalized: %s", json.dumps(result))
    return result


# ==========================================================================
# Local testing:  python handler.py --dry-run
# ==========================================================================
if __name__ == "__main__":
    import sys

    if "--dry-run" in sys.argv:
        sample = [
            # Same person twice: second row is richer, so it should win.
            {"sha256_lc_hem": "ABC123", "first_name": "  jOHN ", "last_name": "o'BRIEN",
             "personal_emails": "John.OBrien@Example.com, alt@x.com",
             "personal_phone": "(212) 555-0143", "personal_address": "12 Main St.",
             "personal_city": "new york", "personal_state": "ny", "personal_zip": "1001",
             "_source_tab": "MerchantCash"},
            {"sha256_lc_hem": "abc123", "first_name": "John", "last_name": "O'Brien",
             "personal_emails": "john.obrien@example.com", "personal_phone": "1-212-555-0143",
             "mobile_phone": "917.555.0199", "personal_address": "12 MAIN ST",
             "personal_city": "New York", "personal_state": "NY", "personal_zip": "10012-3456",
             "company_name": "Acme", "_source_tab": "MerchantCash"},
            # No keys at all -> must survive dedup, flagged incomplete.
            {"first_name": "Mystery", "personal_phone": "n/a", "_source_tab": "Quick Business"},
        ]
        norm = [normalize_row(r, "2026-07-24", i) for i, r in enumerate(sample)]
        uniq, dups = internal_dedup(norm)
        print(f"rows_in={len(sample)} dups={dups} unique={len(uniq)}")
        for r in uniq:
            print("  ", {k: r[k] for k in
                         ("row_id", "norm_first_name", "norm_last_name", "norm_email",
                          "norm_personal_phone", "norm_zip", "phone_key", "is_complete",
                          "duplicates_merged")})
        print("\nCSV preview:")
        print(rows_to_csv(uniq, build_header(uniq)).decode("utf-8-sig")[:400])
    else:
        print("Run with --dry-run for a local, no-AWS test.")

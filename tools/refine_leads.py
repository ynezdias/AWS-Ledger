"""
STEP 4 — Refinement / validation layer
==========================================================================
Reads the batch's NET-NEW leads (in `datamoon_leads` but NOT in
`lead_overlaps`), runs quality checks on each, and writes the results to a
SEPARATE table `refined_leads`.

Design guarantees:
  * READ-ONLY on raw data. This script only SELECTs from `datamoon_leads`
    and `lead_overlaps`. It NEVER updates or deletes a raw row.
  * NO DATA LOSS. Every net-new lead lands in `refined_leads`. Rows that
    fail a check are KEPT and FLAGGED (never dropped) — `flags` records why.
  * Idempotent. Re-running for the same source_dt replaces that day's
    refined rows (DELETE by source_dt, then re-insert).

Checks per lead
---------------
Per-field validity:
  NO_EMAIL / EMAIL_INVALID / DISPOSABLE_EMAIL   (email column + email_norm)
  NO_PHONE / PHONE_INVALID                       (phone_e164)
  NO_NAME                                        (first_name + last_name)
  NO_COMPANY                                     (company_name)

Cross-field consistency ("do they match?"):
  EMAIL_COMPANY_MISMATCH  business-email domain != company_domain
  EMAIL_NAME_MISMATCH     email local-part contains neither first nor last name

FREE_EMAIL (gmail/yahoo/...) is recorded as an informational note, NOT a
defect — it's normal for B2C loan leads and does not flag the row.

refined_status = 'clean' when there are no flags, else 'flagged'.

Run:  DM_SECRET='<secretstring>' python tools/refine_leads.py [source_dt]
      (see tools/run_refine.ps1 — it fetches the secret and invokes this)
"""

import os
import re
import sys
import json
import pg8000

# --------------------------------------------------------------------------
# Validation helpers
# --------------------------------------------------------------------------
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$")
PHONE_E164_RE = re.compile(r"^\+\d{8,15}$")

DISPOSABLE_DOMAINS = {
    "mailinator.com", "tempmail.com", "guerrillamail.com", "10minutemail.com",
    "trashmail.com", "yopmail.com", "getnada.com", "throwawaymail.com",
}
FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "live.com", "msn.com", "comcast.net", "att.net",
    "verizon.net", "sbcglobal.net", "me.com", "ymail.com", "protonmail.com",
}


def _clean(v):
    return (v or "").strip()


def _domain_core(host: str) -> str:
    """Reduce a hostname to its registrable core, e.g. mail.acme.co -> acme."""
    host = host.lower().strip().lstrip("@")
    host = re.sub(r"^www\.", "", host)
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        return parts[-2]
    return parts[0] if parts else ""


def refine(row: dict) -> dict:
    """Run all checks on one lead. Returns the refined-row payload. Never drops."""
    flags: list[str] = []
    notes: list[str] = []

    email = _clean(row.get("email")).lower()
    email_norm = _clean(row.get("email_norm")).lower() or email
    phone_e164 = _clean(row.get("phone_e164"))
    first = _clean(row.get("first_name"))
    last = _clean(row.get("last_name"))
    company = _clean(row.get("company_name"))
    company_domain = _clean(row.get("company_domain")).lower()

    # ---- email validity ----
    email_domain = ""
    if not email_norm:
        email_valid = False
        flags.append("NO_EMAIL")
    elif not EMAIL_RE.match(email_norm):
        email_valid = False
        flags.append("EMAIL_INVALID")
    else:
        email_domain = email_norm.split("@", 1)[1]
        if email_domain in DISPOSABLE_DOMAINS:
            email_valid = False
            flags.append("DISPOSABLE_EMAIL")
        else:
            email_valid = True
            if email_domain in FREE_EMAIL_DOMAINS:
                notes.append("FREE_EMAIL")

    # ---- phone validity ----
    phone_valid = bool(phone_e164) and bool(PHONE_E164_RE.match(phone_e164))
    if not phone_e164:
        flags.append("NO_PHONE")
    elif not phone_valid:
        flags.append("PHONE_INVALID")

    # ---- name / company presence ----
    name_present = bool(first or last)
    if not name_present:
        flags.append("NO_NAME")
    company_present = bool(company)
    if not company_present:
        flags.append("NO_COMPANY")

    # ---- cross-field: email domain vs company ----
    # Only meaningful for a business (non-free) email with something to compare.
    email_company_match = None
    if email_valid and email_domain and email_domain not in FREE_EMAIL_DOMAINS:
        target = company_domain or company
        if target:
            e_core = _domain_core(email_domain)
            c_core = _domain_core(company_domain) if company_domain else \
                re.sub(r"[^a-z0-9]", "", company.lower())
            if e_core and c_core and (e_core in c_core or c_core in e_core):
                email_company_match = True
            else:
                email_company_match = False
                flags.append("EMAIL_COMPANY_MISMATCH")

    # ---- cross-field: email local-part vs person name ----
    email_name_match = None
    if email_valid and name_present:
        local = email_norm.split("@", 1)[0].lower()
        hit = False
        for part in (first.lower(), last.lower()):
            if len(part) >= 3 and part in local:
                hit = True
                break
        email_name_match = hit
        if not hit:
            flags.append("EMAIL_NAME_MISMATCH")

    status = "clean" if not flags else "flagged"

    return {
        "row_id": row.get("row_id"),
        "source_dt": row.get("source_dt"),
        "first_name": first,
        "last_name": last,
        "company_name": company,
        "job_title": _clean(row.get("job_title")),
        "email": email,
        "phone": _clean(row.get("phone")),
        "email_norm": email_norm,
        "phone_e164": phone_e164,
        "email_valid": email_valid,
        "phone_valid": phone_valid,
        "name_present": name_present,
        "company_present": company_present,
        "email_company_match": email_company_match,
        "email_name_match": email_name_match,
        "flags": flags,
        "notes": notes,
        "flag_count": len(flags),
        "refined_status": status,
    }


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS refined_leads (
    row_id               text PRIMARY KEY,
    source_dt            text,
    first_name           text,
    last_name            text,
    company_name         text,
    job_title            text,
    email                text,
    phone                text,
    email_norm           text,
    phone_e164           text,
    email_valid          boolean,
    phone_valid          boolean,
    name_present         boolean,
    company_present      boolean,
    email_company_match  boolean,   -- NULL = not applicable (free/no email)
    email_name_match     boolean,   -- NULL = not applicable
    flags                text[],
    notes                text[],
    flag_count           integer,
    refined_status       text,      -- 'clean' | 'flagged'
    refined_at           timestamptz DEFAULT now()
);
"""

# Net-new = in datamoon_leads but NOT in lead_overlaps (by row_id).
SELECT_NETNEW_SQL = """
SELECT l.*
FROM datamoon_leads l
LEFT JOIN lead_overlaps o ON o.row_id = l.row_id
WHERE l.source_dt = %s AND o.row_id IS NULL;
"""

# pg8000's legacy paramstyle is positional %s, so we bind in this column order.
INSERT_COLS = [
    "row_id", "source_dt", "first_name", "last_name", "company_name", "job_title",
    "email", "phone", "email_norm", "phone_e164",
    "email_valid", "phone_valid", "name_present", "company_present",
    "email_company_match", "email_name_match", "flags", "notes", "flag_count", "refined_status",
]
INSERT_HEAD = f"INSERT INTO refined_leads ({', '.join(INSERT_COLS)}) VALUES "
_ROW_PH = "(" + ", ".join(["%s"] * len(INSERT_COLS)) + ")"


def run(source_dt: str, conn):
    def log(m):
        print(m, flush=True)

    cur = conn.cursor()
    # Don't wait forever on a lock held by a crashed prior run.
    cur.execute("SET lock_timeout = '15s'; SET statement_timeout = '120s';")
    log("create table...")
    cur.execute(CREATE_SQL)

    # Idempotent: clear this day's refined rows before re-inserting.
    log("delete existing...")
    cur.execute("DELETE FROM refined_leads WHERE source_dt = %s;", (source_dt,))

    log("select net-new...")
    cur.execute(SELECT_NETNEW_SQL, (source_dt,))
    colnames = [d[0] for d in cur.description]
    rows = [dict(zip(colnames, r)) for r in cur.fetchall()]
    log(f"fetched {len(rows)} rows; refining + inserting...")

    refined = [refine(r) for r in rows]
    # Chunked multi-row INSERT to keep remote round-trips low.
    CHUNK = 200
    for i in range(0, len(refined), CHUNK):
        batch = refined[i:i + CHUNK]
        sql = INSERT_HEAD + ", ".join([_ROW_PH] * len(batch)) + ";"
        params = [rec[c] for rec in batch for c in INSERT_COLS]
        cur.execute(sql, params)
    conn.commit()

    # ---- report ----
    total = len(refined)
    clean = sum(1 for r in refined if r["refined_status"] == "clean")
    flagged = total - clean
    flag_tally: dict[str, int] = {}
    for r in refined:
        for f in r["flags"]:
            flag_tally[f] = flag_tally.get(f, 0) + 1

    lines = [
        "STEP 4 — Refinement Report",
        "=" * 50,
        f"source_dt:                 {source_dt}",
        f"Net-new leads refined:     {total:,}",
        f"  clean (no flags):        {clean:,}  ({clean/total*100:.1f}%)" if total else "  clean: 0",
        f"  flagged (kept):          {flagged:,}  ({flagged/total*100:.1f}%)" if total else "  flagged: 0",
        "",
        "Flag breakdown (a lead can have several):",
    ]
    for f, n in sorted(flag_tally.items(), key=lambda x: -x[1]):
        lines.append(f"  {f:<24} {n:,}")
    if not flag_tally:
        lines.append("  (none)")
    lines += ["", "Written to RDS table: refined_leads  (raw tables untouched)"]
    report = "\n".join(lines)
    print(report)
    return report


if __name__ == "__main__":
    source_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-07-24"
    c = json.loads(os.environ["DM_SECRET"])
    conn = pg8000.connect(
        user=c["username"], password=c["password"], host=c["host"],
        port=c["port"], database=c["dbname"], ssl_context=True,
    )
    try:
        run(source_dt, conn)
    finally:
        conn.close()

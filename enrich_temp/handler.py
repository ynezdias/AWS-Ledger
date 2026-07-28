"""
TEMPORARY Step 6 (Enrich) Lambda for the DataMoon pipeline.

Two modes in one function:

  SEED   invoke with {"action": "seed", "limit": N, "source_dt": "YYYY-MM-DD"?}
         - READ-ONLY connection to RDS `datamoon` (session forced read-only)
         - pulls net-new CLEAN leads from datamoon_refined for the latest
           (or given) source_dt
         - dedupes by company-domain key (business-email domain when not a
           free provider, else normalized company name)
         - skips domains already in the DynamoDB cache
         - enqueues up to `limit` domains to SQS
         - returns stats only; NOTHING is written to RDS, ever

  WORK   SQS event (batch size 1). Per message:
         - re-checks the DynamoDB cache (idempotent on retry)
         - calls the Claude API (web search enabled, strict JSON schema)
         - writes the result to the DynamoDB cache and to S3 under
           s3://datamoon-raw-data/Enrichment/dt=<dt>/<domain_key>.json
         - raises on transient failure so SQS redrives (maxReceiveCount=3 -> DLQ)
RQVtT4 Q
RDS is only ever SELECTed from. No tables are created, altered, or written.
"""
import json
import os
import re
import time
import datetime as dt

import boto3
import pg8000

REGION = os.environ.get("AWS_REGION", "us-east-2")
DM_SECRET_ID = os.environ.get("DM_SECRET_ID", "datamoon/postgres")
ANTHROPIC_SECRET_ID = os.environ.get("ANTHROPIC_SECRET_ID", "lead-pool/anthropic-api-key")
QUEUE_URL = os.environ.get("QUEUE_URL", "")
CACHE_TABLE = os.environ.get("CACHE_TABLE", "datamoon-enrich-cache-temp")
OUT_BUCKET = os.environ.get("OUT_BUCKET", "datamoon-raw-data")
OUT_PREFIX = os.environ.get("OUT_PREFIX", "Enrichment")
MODEL = os.environ.get("MODEL", "claude-opus-5")
MAX_ENQUEUE_DEFAULT = int(os.environ.get("MAX_ENQUEUE", "25"))

_sm = boto3.client("secretsmanager", region_name=REGION)
_sqs = boto3.client("sqs", region_name=REGION)
_ddb = boto3.client("dynamodb", region_name=REGION)
_s3 = boto3.client("s3", region_name=REGION)

FREE_MAIL = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com",
    "msn.com", "live.com", "comcast.net", "att.net", "verizon.net", "sbcglobal.net",
    "ymail.com", "protonmail.com", "proton.me", "me.com", "mail.com", "gmx.com",
    "bellsouth.net", "cox.net", "charter.net", "earthlink.net", "yahoo.co.uk",
    "hotmail.co.uk", "rocketmail.com", "optonline.net", "juno.com", "netzero.net",
}

ENRICH_SCHEMA = {
    "type": "object",
    "properties": {
        "website": {"type": ["string", "null"], "description": "Official company website URL, or null if not found"},
        "industry": {"type": ["string", "null"], "description": "Primary industry, short phrase"},
        "employee_range": {
            "type": ["string", "null"],
            "description": "One of: 1-10, 11-50, 51-200, 201-500, 501-1000, 1001-5000, 5001+",
        },
        "summary": {"type": ["string", "null"], "description": "One-line description of what the company does"},
        "recent_signals": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Recent notable events: funding, hiring, expansion, news. Empty if none found.",
        },
        "confidence": {
            "type": "object",
            "properties": {
                "website": {"type": "number"},
                "industry": {"type": "number"},
                "employee_range": {"type": "number"},
                "overall": {"type": "number"},
            },
            "required": ["website", "industry", "employee_range", "overall"],
            "additionalProperties": False,
        },
        "source_urls": {
            "type": "array",
            "items": {"type": "string"},
            "description": "URLs of sources actually used for these answers",
        },
        "match_is_ambiguous": {
            "type": "boolean",
            "description": "True if multiple distinct companies matched and identification is uncertain",
        },
    },
    "required": ["website", "industry", "employee_range", "summary",
                 "recent_signals", "confidence", "source_urls", "match_is_ambiguous"],
    "additionalProperties": False,
}

_secret_cache = {}


def _secret(sid):
    if sid not in _secret_cache:
        _secret_cache[sid] = _sm.get_secret_value(SecretId=sid)["SecretString"]
    return _secret_cache[sid]


# ---------------------------------------------------------------- domain key
def domain_key(email_norm, company_name):
    """(key, kind): business-email domain, else normalized company name."""
    em = (email_norm or "").strip().lower()
    if "@" in em:
        dom = em.split("@", 1)[1]
        if dom and dom not in FREE_MAIL and "." in dom:
            return dom, "domain"
    co = re.sub(r"[^a-z0-9]+", " ", (company_name or "").lower()).strip()
    co = re.sub(r"\b(llc|inc|corp|corporation|co|ltd|lp|llp|pllc|pc|dba)\b", "", co).strip()
    co = re.sub(r"\s+", " ", co)
    if co:
        return "co:" + co.replace(" ", "-"), "company_name"
    return None, None


# ---------------------------------------------------------------- seed mode
def seed(event):
    limit = int(event.get("limit", MAX_ENQUEUE_DEFAULT))
    creds = json.loads(_secret(DM_SECRET_ID))
    conn = pg8000.connect(
        user=creds["username"], password=creds["password"],
        host=creds.get("host", "datamoon.c364acm8wlnv.us-east-2.rds.amazonaws.com"),
        port=int(creds.get("port", 5432)), database=creds.get("dbname", "datamoon"),
        timeout=30, ssl_context=True,
    )
    try:
        cur = conn.cursor()
        # Hard guarantee: this session cannot write.
        cur.execute("SET default_transaction_read_only = on")
        conn.commit()

        src_dt = event.get("source_dt")
        if not src_dt:
            cur.execute("SELECT max(source_dt) FROM datamoon_refined")
            src_dt = cur.fetchone()[0]

        cur.execute(
            "SELECT first_name, last_name, company_name, job_title, email, email_norm "
            "FROM datamoon_refined WHERE refined_status='clean' AND source_dt=%s",
            (src_dt,),
        )
        groups = {}  # key -> {kind, company_name, contact, title, leads}
        n_rows = 0
        for fn, ln, co, title, email, em_norm in cur:
            n_rows += 1
            key, kind = domain_key(em_norm or email, co)
            if not key:
                continue
            g = groups.get(key)
            if g is None:
                groups[key] = {
                    "kind": kind,
                    "company_name": (co or "").strip(),
                    "contact": f"{(fn or '').strip()} {(ln or '').strip()}".strip(),
                    "title": (title or "").strip(),
                    "leads": 1,
                }
            else:
                g["leads"] += 1
                if not g["company_name"] and co:
                    g["company_name"] = co.strip()
    finally:
        conn.close()

    # Skip domains already cached
    keys = list(groups.keys())
    cached = set()
    for i in range(0, len(keys), 100):
        chunk = keys[i:i + 100]
        resp = _ddb.batch_get_item(RequestItems={CACHE_TABLE: {
            "Keys": [{"domain_key": {"S": k}} for k in chunk],
            "ProjectionExpression": "domain_key",
        }})
        for item in resp.get("Responses", {}).get(CACHE_TABLE, []):
            cached.add(item["domain_key"]["S"])

    to_send = [k for k in keys if k not in cached][:limit]
    sent = 0
    for i in range(0, len(to_send), 10):
        entries = []
        for j, k in enumerate(to_send[i:i + 10]):
            g = groups[k]
            entries.append({
                "Id": str(j),
                "MessageBody": json.dumps({
                    "domain_key": k, "kind": g["kind"], "company_name": g["company_name"],
                    "contact": g["contact"], "title": g["title"],
                    "lead_count": g["leads"], "source_dt": str(src_dt),
                }),
            })
        _sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
        sent += len(entries)

    return {
        "source_dt": str(src_dt),
        "clean_leads_read": n_rows,
        "distinct_domain_keys": len(keys),
        "by_kind": {
            "email_domain": sum(1 for g in groups.values() if g["kind"] == "domain"),
            "company_name_only": sum(1 for g in groups.values() if g["kind"] == "company_name"),
        },
        "already_cached": len(cached),
        "enqueued": sent,
        "limit": limit,
    }


# ---------------------------------------------------------------- work mode
def _call_claude(msg):
    import anthropic
    client = anthropic.Anthropic(api_key=_secret(ANTHROPIC_SECRET_ID).strip())

    ident = msg["company_name"] or msg["domain_key"]
    parts = [f"Company name: {ident}"]
    if msg["kind"] == "domain":
        parts.append(f"Known business email domain: {msg['domain_key']}")
    if msg.get("contact"):
        who = msg["contact"] + (f", {msg['title']}" if msg.get("title") else "")
        parts.append(f"A known contact there: {who}")
    prompt = (
        "Research this company using web search and fill in the requested fields. "
        "It is likely a US small/medium business (lead source: business-lending leads).\n\n"
        + "\n".join(parts)
        + "\n\nRules: use web search before answering; if you cannot confidently identify "
        "the company, set fields to null, set match_is_ambiguous accordingly, and use low "
        "confidence scores (0-1). recent_signals should only contain things found in "
        "sources, never guesses. source_urls must be URLs you actually consulted."
    )

    messages = [{"role": "user", "content": prompt}]
    for _ in range(4):  # pause_turn continuation guard
        resp = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            output_config={"format": {"type": "json_schema", "schema": ENRICH_SCHEMA}},
            tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 5}],
            messages=messages,
        )
        if resp.stop_reason == "pause_turn":
            messages = [{"role": "user", "content": prompt},
                        {"role": "assistant", "content": resp.content}]
            continue
        break

    if resp.stop_reason == "refusal":
        return {"error": "refusal"}, resp.usage
    text = next((b.text for b in resp.content if b.type == "text"), None)
    if text is None:
        return {"error": "no_text_block", "stop_reason": resp.stop_reason}, resp.usage
    try:
        return json.loads(text), resp.usage
    except json.JSONDecodeError:
        return {"error": "bad_json", "raw": text[:2000]}, resp.usage


def work(record):
    msg = json.loads(record["body"])
    key = msg["domain_key"]

    # Cache hit -> done (idempotent on SQS retry/duplicate delivery)
    got = _ddb.get_item(TableName=CACHE_TABLE, Key={"domain_key": {"S": key}})
    if "Item" in got:
        print(f"cache hit, skipping: {key}")
        return

    t0 = time.time()
    result, usage = _call_claude(msg)
    elapsed = round(time.time() - t0, 1)

    item = {
        "domain_key": key,
        "source_dt": msg.get("source_dt", ""),
        "company_name": msg.get("company_name", ""),
        "lead_count": msg.get("lead_count", 0),
        "model": MODEL,
        "enriched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "usage": {
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "web_searches": getattr(getattr(usage, "server_tool_use", None), "web_search_requests", None),
        },
        "result": result,
    }

    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", key)[:180]
    s3_key = f"{OUT_PREFIX}/dt={msg.get('source_dt', 'unknown')}/{safe}.json"
    _s3.put_object(Bucket=OUT_BUCKET, Key=s3_key,
                   Body=json.dumps(item, indent=2).encode(),
                   ContentType="application/json")

    _ddb.put_item(TableName=CACHE_TABLE, Item={
        "domain_key": {"S": key},
        "enriched_at": {"S": item["enriched_at"]},
        "s3_key": {"S": s3_key},
        "ok": {"BOOL": "error" not in result},
        "payload": {"S": json.dumps(result)[:35000]},
    })
    print(f"enriched {key} in {elapsed}s -> s3://{OUT_BUCKET}/{s3_key}")


def handler(event, context):
    if isinstance(event, dict) and event.get("action") == "seed":
        out = seed(event)
        print(json.dumps(out))
        return out
    for record in event.get("Records", []):
        work(record)
    return {"processed": len(event.get("Records", []))}

"""
Recurring DataMoon leads that the pool has ALREADY been worked.

Question this answers:
    "Which leads are arriving in our DataMoon feed AGAIN, and of those, which
     ones have we actually touched recently in the lead pool?"

Sources
    datamoon.public.overlap_history   -> every DataMoon match key that has ever
                                         overlapped the pool, with how many
                                         times it arrived and when.
    leadpool.public.lead_emails       -> match key -> pool lead_id
    leadpool.public.lead_phones
    leadpool.public.leads             -> issue / shop / fund / SF signals
    leadpool.public.lead_activity     -> AI, TextBolt, email, task, opportunity

Outputs (written to the repo root)
    recurring_worked_leads.csv
    RECURRING_WORKED_LEADS_REPORT.txt

Run:  python tools/recurring_worked_leads.py
"""

import csv
import os
from datetime import datetime, timezone, date

import pg8000.native

# --- config -----------------------------------------------------------------

DM = dict(
    user="datamoon_admin",
    host="datamoon.c364acm8wlnv.us-east-2.rds.amazonaws.com",
    port=5432,
    database="datamoon",
)
LP = dict(
    user="leadpool_admin",
    host="lead-pool.c364acm8wlnv.us-east-2.rds.amazonaws.com",
    port=5432,
    database="leadpool",
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_OUT = os.path.join(ROOT, "recurring_worked_leads.csv")
TXT_OUT = os.path.join(ROOT, "RECURRING_WORKED_LEADS_REPORT.txt")

NOW = datetime.now(timezone.utc)

# sf_synced_at is deliberately NOT a work signal: 2.9M rows carry it and the max
# is "today", i.e. it is a bulk Salesforce sync stamp, not a human touching the
# lead. It is carried as a column for context only.
WORK_FIELDS = [
    ("last_issued_at", "issued to a rep"),
    ("last_shopped_at", "shopped to a funder"),
    ("last_funded_at", "funded"),
    ("ai_last", "AI outreach"),
    ("tb_last", "TextBolt SMS"),
    ("email_out_last", "outbound email"),
    ("email_in_last", "inbound email reply"),
    ("task_last", "rep task logged"),
]


def secret(env_name, fallback_key):
    """Password comes from env (set by the runner) so it is never hard-coded."""
    v = os.environ.get(env_name)
    if not v:
        raise SystemExit(
            "Missing env var %s. Run via tools/run_recurring.ps1, which pulls "
            "the password out of Secrets Manager." % env_name
        )
    return v


def as_dt(v):
    """last_funded_at is a DATE; everything else is timestamptz."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    return None


def days_since(v):
    d = as_dt(v)
    return None if d is None else (NOW - d).days


def recency_band(days):
    if days is None:
        return "never_worked"
    if days <= 7:
        return "0-7d"
    if days <= 30:
        return "8-30d"
    if days <= 90:
        return "31-90d"
    if days <= 180:
        return "91-180d"
    return "180d+"


# --- 1. pull the recurrence history from datamoon ---------------------------

print("[1/4] reading overlap_history from datamoon ...")
dm = pg8000.native.Connection(password=secret("DM_PW", "datamoon"), ssl_context=True, **DM)
hist = dm.run(
    """
    select match_key, key_type, company_name, first_name, last_name,
           overlap_count, first_overlap_at, last_overlap_at
    from overlap_history
    """
)
dm.close()
print("      %d match keys with a pool overlap" % len(hist))

by_key = {r[0]: r for r in hist}
keys = list(by_key.keys())


# --- 2. resolve keys -> pool leads, with work signals ------------------------

print("[2/4] resolving %d keys against the lead pool ..." % len(keys))
lp = pg8000.native.Connection(password=secret("LP_PW", "leadpool"), ssl_context=True, **LP)
lp.run("set statement_timeout = '600s'")
lp.run("create temp table dm_keys (k text primary key)")

CHUNK = 1000
for i in range(0, len(keys), CHUNK):
    batch = keys[i : i + CHUNK]
    vals = ",".join("(:k%d)" % j for j in range(len(batch)))
    params = {("k%d" % j): k for j, k in enumerate(batch)}
    lp.run(
        "insert into dm_keys(k) values " + vals + " on conflict do nothing", **params
    )
lp.run("analyze dm_keys")

rows = lp.run(
    """
    with matched as (
        select e.lead_id, e.email_norm as k, 'email' as key_type
          from lead_emails e join dm_keys d on d.k = e.email_norm
        union
        select p.lead_id, p.phone_e164 as k, 'phone' as key_type
          from lead_phones p join dm_keys d on d.k = p.phone_e164
    )
    select m.lead_id::text,
           array_agg(distinct m.k)                as keys,
           array_agg(distinct m.key_type)         as key_types,
           l.company, l.first_name, l.last_name,
           l.city, l.state,
           l.status, l.sf_status, l.lead_source,
           l.do_not_issue,
           l.issue_count, l.last_issued_at, l.last_brand,
           l.shopped_count, l.last_shopped_at, l.last_shop_outcome,
           l.deal_count, l.converted_count, l.funded_total, l.last_funded_at,
           l.no_offer_count, l.dead_file_count,
           l.dup_arrival_count, l.created_at, l.sf_synced_at,
           a.ai_last, a.tb_last, a.tb_replied,
           a.email_out_last, a.email_in_last, a.task_last,
           a.n_opp, a.opp_stages
      from matched m
      join leads l         on l.id = m.lead_id
      left join lead_activity a on a.lead_id = m.lead_id::text
     group by m.lead_id, l.company, l.first_name, l.last_name, l.city, l.state,
              l.status, l.sf_status, l.lead_source, l.do_not_issue,
              l.issue_count, l.last_issued_at, l.last_brand,
              l.shopped_count, l.last_shopped_at, l.last_shop_outcome,
              l.deal_count, l.converted_count, l.funded_total, l.last_funded_at,
              l.no_offer_count, l.dead_file_count, l.dup_arrival_count,
              l.created_at, l.sf_synced_at,
              a.ai_last, a.tb_last, a.tb_replied, a.email_out_last,
              a.email_in_last, a.task_last, a.n_opp, a.opp_stages
    """
)
lp.close()
print("      %d distinct pool leads matched" % len(rows))

COLS = [
    "lead_id", "keys", "key_types", "company", "first_name", "last_name",
    "city", "state", "status", "sf_status", "lead_source", "do_not_issue",
    "issue_count", "last_issued_at", "last_brand",
    "shopped_count", "last_shopped_at", "last_shop_outcome",
    "deal_count", "converted_count", "funded_total", "last_funded_at",
    "no_offer_count", "dead_file_count", "dup_arrival_count", "created_at",
    "sf_synced_at", "ai_last", "tb_last", "tb_replied",
    "email_out_last", "email_in_last", "task_last", "n_opp", "opp_stages",
]


# --- 3. score + build the human-readable reason -----------------------------

print("[3/4] scoring and writing reasons ...")
out = []
for raw in rows:
    r = dict(zip(COLS, raw))

    # recurrence facts from the DataMoon side (take the strongest key)
    hs = [by_key[k] for k in r["keys"] if k in by_key]
    arrivals = max((h[5] or 1) for h in hs) if hs else 1
    first_seen = min((h[6] for h in hs if h[6]), default=None)
    last_seen = max((h[7] for h in hs if h[7]), default=None)

    # most recent genuine work touch
    touches = []
    for field, label in WORK_FIELDS:
        d = as_dt(r.get(field))
        if d is not None:
            touches.append((d, label))
    touches.sort(reverse=True)
    last_worked = touches[0][0] if touches else None
    dsw = days_since(last_worked)

    # ---- reasoning -------------------------------------------------------
    why = []

    if arrivals > 1:
        why.append(
            "arrived %dx in DataMoon (first %s, again %s)"
            % (arrivals, first_seen.date() if first_seen else "?",
               last_seen.date() if last_seen else "?")
        )
    else:
        why.append(
            "arrived in DataMoon %s" % (last_seen.date() if last_seen else "?")
        )

    why.append("matched pool lead on " + "+".join(sorted(r["key_types"] or [])))

    if r["dup_arrival_count"] and r["dup_arrival_count"] > 1:
        why.append("pool already logged %d duplicate arrivals" % r["dup_arrival_count"])

    if touches:
        why.append(
            "last worked %dd ago (%s)" % (dsw, touches[0][1])
        )
        extra = [lbl for _, lbl in touches[1:4]]
        if extra:
            why.append("also: " + ", ".join(extra))
    else:
        why.append("NO work signal in pool - never issued, shopped or contacted")

    if r["issue_count"]:
        why.append("issued %dx%s" % (r["issue_count"],
                                     " (last brand %s)" % r["last_brand"] if r["last_brand"] else ""))
    if r["last_shop_outcome"]:
        why.append("shop outcome: %s" % r["last_shop_outcome"])
    if r["deal_count"]:
        why.append("%d deal(s)" % r["deal_count"])
    if r["funded_total"]:
        why.append("funded $%s" % r["funded_total"])
    if r["no_offer_count"]:
        why.append("%d no-offer" % r["no_offer_count"])
    if r["dead_file_count"]:
        why.append("%d dead-file" % r["dead_file_count"])
    if r["tb_replied"]:
        why.append("REPLIED to SMS")
    if r["email_in_last"]:
        why.append("replied by email")
    if r["n_opp"]:
        why.append("%d opportunity (%s)" % (r["n_opp"], r["opp_stages"]))
    if r["do_not_issue"]:
        why.append("DO_NOT_ISSUE flag set")
    if r["sf_status"]:
        why.append("SF status: %s" % r["sf_status"])

    # ---- verdict ---------------------------------------------------------
    if r["do_not_issue"]:
        verdict = "SUPPRESS - do_not_issue"
    elif r["deal_count"] or r["funded_total"]:
        verdict = "SUPPRESS - already a deal/funded"
    elif dsw is not None and dsw <= 30:
        verdict = "SUPPRESS - worked in last 30d"
    elif dsw is not None and dsw <= 90:
        verdict = "HOLD - worked in last 90d"
    elif dsw is not None:
        verdict = "RE-ENGAGE - cold, last worked %dd ago" % dsw
    else:
        verdict = "RELEASE - in pool but never worked"

    out.append(
        {
            "lead_id": r["lead_id"],
            "company": r["company"],
            "first_name": r["first_name"] or (hs[0][3] if hs else None),
            "last_name": r["last_name"] or (hs[0][4] if hs else None),
            "city": r["city"],
            "state": r["state"],
            "match_keys": "; ".join(r["keys"] or []),
            "matched_on": "+".join(sorted(r["key_types"] or [])),
            "datamoon_arrivals": arrivals,
            "datamoon_first_seen": first_seen.date().isoformat() if first_seen else "",
            "datamoon_last_seen": last_seen.date().isoformat() if last_seen else "",
            "last_worked_at": last_worked.isoformat() if last_worked else "",
            "days_since_last_worked": "" if dsw is None else dsw,
            "recency_band": recency_band(dsw),
            "work_channels": ", ".join(lbl for _, lbl in touches),
            "issue_count": r["issue_count"] or 0,
            "shopped_count": r["shopped_count"] or 0,
            "last_shop_outcome": r["last_shop_outcome"] or "",
            "deal_count": r["deal_count"] or 0,
            "funded_total": r["funded_total"] or "",
            "no_offer_count": r["no_offer_count"] or 0,
            "dead_file_count": r["dead_file_count"] or 0,
            "do_not_issue": bool(r["do_not_issue"]),
            "sf_status": r["sf_status"] or "",
            "pool_created_at": r["created_at"].date().isoformat() if r["created_at"] else "",
            "verdict": verdict,
            "reason": " | ".join(why),
        }
    )

# most-recently-worked first; never-worked at the bottom
out.sort(key=lambda x: (x["days_since_last_worked"] == "", x["days_since_last_worked"]))


# --- 4. write outputs -------------------------------------------------------

print("[4/4] writing %s" % CSV_OUT)
with open(CSV_OUT, "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
    w.writeheader()
    w.writerows(out)


def tally(key):
    d = {}
    for r in out:
        d[r[key]] = d.get(r[key], 0) + 1
    return sorted(d.items(), key=lambda kv: -kv[1])


worked = [r for r in out if r["recency_band"] != "never_worked"]
recur = [r for r in out if r["datamoon_arrivals"] > 1]

lines = []
A = lines.append
A("RECURRING DATAMOON LEADS vs LEAD POOL - work-history brief")
A("generated %s UTC" % NOW.strftime("%Y-%m-%d %H:%M"))
A("=" * 72)
A("")
A("SCOPE")
A("  DataMoon match keys that have ever overlapped the pool : %d" % len(hist))
A("  Distinct pool leads those keys resolve to              : %d" % len(out))
A("  ...of which have a real work signal                    : %d" % len(worked))
A("  ...of which arrived in DataMoon more than once         : %d" % len(recur))
A("")
A("HOW RECENTLY THE POOL LEAD WAS WORKED")
for k, v in sorted(tally("recency_band"), key=lambda kv: kv[1], reverse=True):
    A("  %-14s %8d" % (k, v))
A("")
A("VERDICT")
for k, v in tally("verdict"):
    A("  %-42s %7d" % (k, v))
A("")
A("MATCHED ON")
for k, v in tally("matched_on"):
    A("  %-14s %8d" % (k, v))
A("")
A("SF STATUS (top)")
for k, v in tally("sf_status")[:12]:
    A("  %-28s %7d" % (k or "(none)", v))
A("")
A("WHAT 'WORKED' MEANS HERE")
A("  A lead counts as worked if ANY of these carry a timestamp:")
for f, lbl in WORK_FIELDS:
    A("    %-18s %s" % (f, lbl))
A("  sf_synced_at is EXCLUDED on purpose - 2.9M leads carry it and it maxes")
A("  out at today, so it is a bulk Salesforce sync stamp, not a human touch.")
A("")
A("OUTPUT")
A("  %s" % os.path.basename(CSV_OUT))
A("  one row per pool lead, sorted most-recently-worked first;")
A("  the 'reason' column spells out why each row is in the list.")

with open(TXT_OUT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")

print("\n".join(lines))
print("\nwrote %s" % TXT_OUT)

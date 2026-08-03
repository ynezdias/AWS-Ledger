"""
STEP 6 — Overlapping analysis: who should we re-contact?

Runs AFTER the daily store (datamoon_leads / datamoon_refined / lead_overlaps /
S3 export are all written and verified). Scores EVERY entity in lead_overlaps
behaviorally and tiers it T1..T4. Nothing is dropped: repliers, opt-outs,
deals and cool-off leads keep their rows and are marked in contact_status.

Writes:
  lead_overlaps.tier       new column — every key's current tier, refreshed daily
  t1_t2                    CURRENT-STATE table of T1+T2 entities (reach out now).
                           Fully rebuilt each run; leads move in/out as their
                           signals change day to day.
  t3_t4                    CURRENT-STATE table of T3+T4 entities (stored for
                           later; promoted automatically when signals appear).
  recontacting_overlaps    per-cycle-date SNAPSHOT of contactable T1/T2 with a
                           phone (history of what each day's run recommended)
  upleads_list             per-cycle-date SNAPSHOT of contactable T1/T2 without
                           a phone (email-only; enrich later)

Scoring (see prompt_overlap_analysis.txt for the method and its caveats):
  +40 funding-themed source file   (INFERRED FROM VENDOR FILE NAME ONLY —
                                    confirm with vendor whether these segments
                                    are intent-based or modeled lookalikes)
  +15 reappeared on 2+ distinct days
  +15 present in 2+ different source files
  +15 arrived again in the cycle date's own data
  +10 matched pool on BOTH email and phone
  +10 decision-maker title
  +5  validated contact data
  -30 worked by a rep in the last 30 days
Tiers: T1 >=70, T2 >=50, T3 >=35, T4 below.

contact_status values: ok | replied_interested | replied_later | replied_other |
opted_out (DO NOT CONTACT) | deal_or_do_not_issue | cooloff_recently_worked.
Repliers are worked through the reply track (interested_recent_ranked CSVs);
opted_out must never be contacted regardless of tier.

Usage:  python tools/overlap_analysis.py 2026-07-29     (needs DM_SECRET set)
"""
import os, sys, re, csv, glob, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import pipeline_store as S

ROOT = os.path.dirname(HERE)
LOCAL_ROOT = os.path.join(ROOT, "DataMoon")

INTENT_TOKENS = ("quick business", "quick_business", "merchant_cash", "b2c")
DM_TITLES = ("owner", "founder", "ceo", "president", "principal", "partner", "chief")
DM_SENIORITY = ("cxo", "owner", "founder", "director", "vp")

TABLE_COLS = [
    "cycle_date", "match_key", "rank", "tier", "score",
    "first_name", "last_name", "contact_no", "email", "company_name",
    "key_type", "matched_on", "overlap_hits", "distinct_days",
    "first_seen", "last_seen", "source_files", "n_sources",
    "intent_source", "active_in_cycle", "job_title", "state",
    "recently_worked", "contact_status", "reason",
]
DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    cycle_date       date NOT NULL,
    match_key        text NOT NULL,
    rank             integer,
    tier             text,
    score            integer,
    first_name       text,
    last_name        text,
    contact_no       text,
    email            text,
    company_name     text,
    key_type         text,
    matched_on       text,
    overlap_hits     integer,
    distinct_days    integer,
    first_seen       date,
    last_seen        date,
    source_files     text,
    n_sources        integer,
    intent_source    boolean,
    active_in_cycle  boolean,
    job_title        text,
    state            text,
    recently_worked  text,
    contact_status   text,
    reason           text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cycle_date, match_key)
);
"""
# t1_t2 / t3_t4 are CURRENT STATE: one row per match_key, replaced every run.
DDL_STATE = DDL.replace("PRIMARY KEY (cycle_date, match_key)", "PRIMARY KEY (match_key)")

def _digits10(p):
    d = re.sub(r"\D", "", p or "")
    return d[-10:] if len(d) >= 10 else None

# ---------------------------------------------------------------- inputs
def load_repliers(progress=print):
    """key -> reply verdict for everyone who ever replied via TextTorrent.
    Uses the NEWEST retarget_replied_* export found; a manual snapshot the
    cycle does not refresh — drop in newer exports to keep this current."""
    dirs = sorted(glob.glob(os.path.join(LOCAL_ROOT, "retarget_replied_*")))
    out = {}
    if not dirs:
        progress("  WARNING: no retarget_replied_* export found — reply status OFF")
        return out
    d = dirs[-1]
    progress(f"  replier export: {os.path.basename(d)} "
             f"(a manual snapshot — drop in a newer one to keep statuses fresh)")
    for name, forced in (("retarget_companies.csv", None),
                         ("excluded_do_not_contact.csv", "opted_out")):
        p = os.path.join(d, name)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                v = forced or (r.get("verdict", "") or "replied_other")
                for ph in re.split(r"[;,]", r.get("phones_replied", "") or ""):
                    k = _digits10(ph)
                    if k: out.setdefault(k, v)
                for em in re.split(r"[;,]", r.get("emails", "") or ""):
                    em = em.strip().lower()
                    if em: out.setdefault(em, v)
    return out

def load_worked(progress=print):
    p = os.path.join(ROOT, "recurring_worked_leads.csv")
    worked = {}
    if not os.path.exists(p):
        progress("  WARNING: recurring_worked_leads.csv not found — cool-off/deal status OFF")
        return worked
    with open(p, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            info = {"verdict": r["verdict"], "days": r["days_since_last_worked"],
                    "dni": r["do_not_issue"].strip().upper() == "TRUE"}
            for k in re.split(r"[;,]", r.get("match_keys", "") or ""):
                k = k.strip().lower()
                key = k if "@" in k else _digits10(k)
                if key: worked.setdefault(key, info)
    return worked

def load_cycle_enrich(src_dt, progress=print):
    """Contact quality + activity from the cycle date's own overlapping.csv."""
    p = os.path.join(LOCAL_ROOT, f"cycle_{src_dt}", "overlapping.csv")
    enrich = {}
    if not os.path.exists(p):
        progress(f"  WARNING: {p} not found — active-in-cycle/title/mobile signals OFF")
        return enrich
    with open(p, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            title = r.get("job_title", "") or ""
            senior = (r.get("seniority_level", "") or "").lower()
            info = {
                "mobile": (r.get("mobile_phone", "") or r.get("direct_number", "") or "").strip(),
                "title": title,
                "dm": any(t in title.lower() for t in DM_TITLES) or senior in DM_SENIORITY,
                "valid": "valid" in ((r.get("business_email_validation_status", "") or "")
                                     + (r.get("personal_emails_validation_status", "") or "")).lower(),
                "state": r.get("personal_state", "") or r.get("company_state", ""),
            }
            for em in (r.get("email_norm", "") or "").split(";"):
                em = em.strip().lower()
                if em: enrich.setdefault(em, info)
            for ph in (r.get("phone_e164", "") or "").split(";"):
                k = _digits10(ph)
                if k: enrich.setdefault(k, info)
    return enrich

# ---------------------------------------------------------------- scoring
def score_row(row, en, wk, src_dt):
    """row: dict from lead_overlaps. Returns (score, why-list, derived fields)."""
    srcs = [s.strip().lower() for s in (row["source_files"] or "").split(";") if s.strip()]
    n_src = len(set(srcs))
    intent = any(any(t in s for t in INTENT_TOKENS) for s in srcs)
    ddays = row["distinct_days"] or 0
    strong = row["matched_on"] in ("email+phone", "phone+email")
    active = bool(en) and str(row["last_seen"]) == src_dt
    recently_worked = bool(wk and "worked in last 30d" in wk["verdict"])

    score, why = 0, []
    if intent:
        score += 40; why.append("came through a funding-themed source (inferred from file name; vendor to confirm intent vs lookalike)")
    if ddays >= 2:
        score += 15; why.append(f"reappeared on {ddays} different days ({row['seen_dates_text'] or ''})")
    if n_src >= 2:
        score += 15; why.append(f"corroborated by {n_src} different source files")
    if active:
        score += 15; why.append(f"arrived again in the {src_dt} cycle — active right now")
    if strong:
        score += 10; why.append("matched pool on BOTH email and phone (identity certain)")
    if en and en.get("dm"):
        score += 10; why.append(f"decision maker ({en.get('title','')})")
    if en and (en.get("valid") or en.get("mobile")):
        score += 5; why.append("contact data validated (mobile / verified email)")
    if recently_worked:
        score -= 30; why.append(f"CAUTION: a rep worked them {wk['days']}d ago — cool-off")
    return score, why, intent, n_src, active, recently_worked

def tier_of(score):
    if score >= 70: return "T1"
    if score >= 50: return "T2"
    if score >= 35: return "T3"
    return "T4"

# ------------------------------------------- tier every lead by data quality
# Every row in datamoon_leads / datamoon_refined gets a tier. Two evidence
# levels, applied in order:
#   1. QUALITY (all rows): how good and workable is the record itself —
#      funding-themed source, valid phone AND email, complete record,
#      decision-maker title. This is what reps can act on for leads that
#      have no overlap history.
#   2. BEHAVIOR (overlap rows only): the lead_overlaps tier overwrites the
#      quality tier where it exists — observed behavior beats record quality.
# Idempotent: fully recomputed on every run.
LEADS_QUALITY_SQL = """
UPDATE datamoon_leads SET tier = CASE
  WHEN source_file ~* 'quick.?business|merchant_cash|b2c'
       AND has_valid_phone = 'true' AND has_valid_email = 'true'
       AND is_complete = 'true'
       AND (job_title ~* 'owner|founder|ceo|president|principal|partner|chief'
            OR lower(coalesce(seniority,'')) IN ('cxo','owner','founder','director','vp'))
    THEN 'T1'
  WHEN (source_file ~* 'quick.?business|merchant_cash|b2c'
        AND has_valid_phone = 'true' AND has_valid_email = 'true')
    OR (source_file ~* 'quick.?business|merchant_cash|b2c'
        AND (has_valid_phone = 'true' OR has_valid_email = 'true')
        AND (job_title ~* 'owner|founder|ceo|president|principal|partner|chief'
             OR lower(coalesce(seniority,'')) IN ('cxo','owner','founder','director','vp')))
    OR (has_valid_phone = 'true' AND has_valid_email = 'true' AND is_complete = 'true'
        AND (job_title ~* 'owner|founder|ceo|president|principal|partner|chief'
             OR lower(coalesce(seniority,'')) IN ('cxo','owner','founder','director','vp')))
    THEN 'T2'
  WHEN (source_file ~* 'quick.?business|merchant_cash|b2c'
        AND (has_valid_phone = 'true' OR has_valid_email = 'true'))
    OR (has_valid_phone = 'true' AND has_valid_email = 'true' AND is_complete = 'true')
    THEN 'T3'
  ELSE 'T4'
END
"""
REFINED_QUALITY_SQL = """
UPDATE datamoon_refined SET tier = CASE
  WHEN refined_status = 'clean' AND source_file ~* 'quick.?business|merchant_cash|b2c'
       AND email_valid AND phone_valid
    THEN 'T1'
  WHEN refined_status = 'clean'
       AND ((source_file ~* 'quick.?business|merchant_cash|b2c'
             AND (email_valid OR phone_valid))
            OR (email_valid AND phone_valid))
    THEN 'T2'
  WHEN (refined_status = 'clean' AND (email_valid OR phone_valid))
    OR (source_file ~* 'quick.?business|merchant_cash|b2c'
        AND email_valid AND phone_valid)
    THEN 'T3'
  ELSE 'T4'
END
"""

def apply_lead_tiers(connh, progress=print, src_dt=None):
    """Tier every lead. These are whole-table UPDATEs over ~2M rows, which run
    far longer than the 180s socket timeout the normal connection uses -- that
    silently killed this step on 2026-07-30 and left 948k rows untiered while
    the run only logged a soft warning. Use a dedicated long-timeout
    connection, and close it here so the caller's connection is untouched."""
    conn = S.connect_long()
    connh = {"conn": conn}          # shadow: everything below runs on the long conn
    try:
        _apply_lead_tiers(connh, progress, src_dt)
    finally:
        try: conn.close()
        except Exception: pass

def _apply_lead_tiers(connh, progress=print, src_dt=None):
    cur = connh["conn"].cursor()
    cur.execute("ALTER TABLE datamoon_leads ADD COLUMN IF NOT EXISTS tier text")
    cur.execute("ALTER TABLE datamoon_refined ADD COLUMN IF NOT EXISTS tier text")
    connh["conn"].commit()

    # pass 1: quality tier. Its inputs (source_file, job_title, seniority, the
    # validity flags) are immutable once a date is stored, so re-deriving every
    # past date each night was pure waste -- and the cost grew with the table,
    # which is what pushed this step past the timeout. Scope to the date being
    # run; pass `src_dt=None` to re-tier everything (backfill / rule change).
    # Rows never tiered (tier IS NULL) are always included, so a date that
    # missed this step still gets picked up.
    where, args = "", ()
    if src_dt:
        where = " WHERE (source_dt = %s OR tier IS NULL)"; args = (src_dt,)
    cur.execute(LEADS_QUALITY_SQL + where, args); n1 = cur.rowcount
    connh["conn"].commit()
    cur = connh["conn"].cursor()
    cur.execute(REFINED_QUALITY_SQL + where, args); n2 = cur.rowcount
    connh["conn"].commit()
    scope = f"for {src_dt} (+any untiered)" if src_dt else "for ALL dates"
    progress(f"  quality tiers set {scope}: datamoon_leads {n1:,}, datamoon_refined {n2:,}")

    # pass 2: overlap behavior overrides quality where it exists
    cur = connh["conn"].cursor()
    cur.execute("""
        CREATE TEMP TABLE dl_ov AS
        SELECT k.source_dt, k.row_id, min(lo.tier) AS tier
        FROM (SELECT source_dt, row_id,
                     unnest(string_to_array(coalesce(email_all,'') || ';' ||
                                            coalesce(phone_all,''), ';')) AS key
              FROM datamoon_leads) k
        JOIN lead_overlaps lo ON lo.match_key = k.key AND lo.tier IS NOT NULL
        GROUP BY 1, 2""")
    cur.execute("""UPDATE datamoon_leads dl SET tier = t.tier FROM dl_ov t
                   WHERE dl.source_dt = t.source_dt AND dl.row_id = t.row_id
                     AND dl.tier IS DISTINCT FROM t.tier""")
    n3 = cur.rowcount
    cur.execute("DROP TABLE dl_ov")
    # NO equivalent pass for datamoon_refined. It holds only NET-NEW leads --
    # rows where none of the lead's keys matched the pool -- while lead_overlaps
    # holds only keys that DID match. The two sets are disjoint by construction,
    # so the old dr_ov join scanned 2M rows to update exactly 0 every run
    # (measured 2026-07-30: 0 of 2,024,783). Refined keeps its quality tier.
    connh["conn"].commit()
    progress(f"  overlap behavior overrides: datamoon_leads {n3:,} "
             f"(refined: n/a — net-new cannot overlap)")
    return n1, n2

# ---------------------------------------------------------------- main analysis
def analyze(src_dt, progress=print):
    progress("\nOverlapping analysis (step 6)...")
    repliers = load_repliers(progress)
    worked = load_worked(progress)
    enrich = load_cycle_enrich(src_dt, progress)

    connh = {"conn": S.connect()}
    try:
        cur = connh["conn"].cursor()
        cur.execute("ALTER TABLE lead_overlaps ADD COLUMN IF NOT EXISTS tier text")
        connh["conn"].commit()
        cur.execute("""SELECT match_key, key_type, first_name, last_name, company_name,
                              entity_key, matched_on, overlap_count, distinct_days,
                              first_seen, last_seen, seen_dates_text, source_files
                       FROM lead_overlaps""")
        cols = ["match_key","key_type","first_name","last_name","company_name","entity_key",
                "matched_on","overlap_count","distinct_days","first_seen","last_seen",
                "seen_dates_text","source_files"]
        table = [dict(zip(cols, r)) for r in cur.fetchall()]
        progress(f"  lead_overlaps: {len(table):,} keys loaded from RDS")

        key_tiers = []           # (match_key, tier) for EVERY key
        best = {}                # entity_key -> best-scoring candidate row
        for row in table:
            mk = (row["match_key"] or "").strip().lower()
            key = _digits10(mk) if row["key_type"] == "phone" else mk
            if not key: key = mk
            wk = worked.get(key)
            en = enrich.get(key, {})
            score, why, intent, n_src, active, rec_worked = score_row(row, en, wk, src_dt)
            tier = tier_of(score)
            key_tiers.append({"match_key": row["match_key"], "tier": tier})

            rv = repliers.get(key)
            if rv == "opted_out":
                status = "opted_out"; why = why + ["OPTED OUT — DO NOT CONTACT, overrides tier"]
            elif rv:
                status = rv; why = why + [f"has replied via TextTorrent ({rv}) — work through the reply track"]
            elif wk and (wk["dni"] or "deal/funded" in wk["verdict"]):
                status = "deal_or_do_not_issue"; why = why + ["already a deal or do-not-issue"]
            elif rec_worked:
                status = "cooloff_recently_worked"
            else:
                status = "ok"

            cand = {
                "cycle_date": src_dt, "match_key": row["match_key"], "rank": 0,
                "tier": tier, "score": score,
                "first_name": row["first_name"] or "", "last_name": row["last_name"] or "",
                "contact_no": en.get("mobile") or (mk if row["key_type"] == "phone" else ""),
                "email": mk if row["key_type"] == "email" else "",
                "company_name": row["company_name"] or "",
                "key_type": row["key_type"], "matched_on": row["matched_on"] or "",
                "overlap_hits": row["overlap_count"] or 0,
                "distinct_days": row["distinct_days"] or 0,
                "first_seen": str(row["first_seen"] or "") or None,
                "last_seen": str(row["last_seen"] or "") or None,
                "source_files": row["source_files"] or "", "n_sources": n_src,
                "intent_source": intent, "active_in_cycle": active,
                "job_title": en.get("title", ""), "state": en.get("state", ""),
                "recently_worked": f"yes ({wk['days']}d ago)" if rec_worked else "no",
                "contact_status": status,
                "reason": " | ".join(why),
            }
            ent = row["entity_key"] or key
            old = best.get(ent)
            if not old or cand["score"] > old["score"] or (
                cand["score"] == old["score"] and cand["contact_no"] and not old["contact_no"]):
                best[ent] = cand

        # ---- 1. refresh lead_overlaps.tier for every key ----
        cur = connh["conn"].cursor()
        cur.execute("DROP TABLE IF EXISTS tier_stage")
        cur.execute("CREATE TEMP TABLE tier_stage (match_key text, tier text)")
        S.copy_rows(connh, "tier_stage", ["match_key", "tier"], key_tiers,
                    progress=lambda m: None)
        cur = connh["conn"].cursor()
        cur.execute("UPDATE lead_overlaps lo SET tier = s.tier "
                    "FROM tier_stage s WHERE lo.match_key = s.match_key")
        n_upd = cur.rowcount
        cur.execute("DROP TABLE tier_stage")
        connh["conn"].commit()
        progress(f"  lead_overlaps.tier refreshed on {n_upd:,} keys")

        cands = sorted(best.values(), key=lambda c: (c["tier"], -c["score"]))
        for i, c in enumerate(cands, 1):
            c["rank"] = i
        t12 = [c for c in cands if c["tier"] in ("T1", "T2")]
        t34 = [c for c in cands if c["tier"] in ("T3", "T4")]
        n_promo = {t: sum(1 for c in t12 + t34 if c["tier"] == t) for t in ("T1","T2","T3","T4")}
        progress(f"  tiers: {n_promo} across {len(cands):,} entities (every row kept)")

        # ---- 2. current-state tables: full rebuild each run ----
        for name, rows in (("t1_t2", t12), ("t3_t4", t34)):
            cur = connh["conn"].cursor()
            cur.execute(DDL_STATE.format(name=name))
            cur.execute(f"DELETE FROM {name}")
            connh["conn"].commit()
            S.copy_rows(connh, name, TABLE_COLS, rows, progress=progress)
            progress(f"  {name}: rebuilt with {len(rows):,} rows (current state as of {src_dt})")

        # ---- 3. per-date snapshots of contactable T1/T2 (unchanged behavior) ----
        contactable = [c for c in t12 if c["contact_status"] == "ok"]
        withphone = [c for c in contactable if c["contact_no"]]
        blank = [c for c in contactable if not c["contact_no"]]
        for name, rows in (("recontacting_overlaps", withphone), ("upleads_list", blank)):
            cur = connh["conn"].cursor()
            cur.execute(DDL.format(name=name))
            cur.execute(f"ALTER TABLE {name} ADD COLUMN IF NOT EXISTS contact_status text")
            cur.execute(f"DELETE FROM {name} WHERE cycle_date=%s", (src_dt,))
            connh["conn"].commit()
            S.copy_rows(connh, name, TABLE_COLS, rows, progress=progress)
            cur = connh["conn"].cursor()
            cur.execute(f"SELECT count(*) FROM {name}")
            progress(f"  {name}: +{len(rows):,} rows for {src_dt} (table total {cur.fetchone()[0]:,})")

        # ---- 4. tier every lead in datamoon_leads / datamoon_refined ----
        apply_lead_tiers(connh, progress, src_dt)

        # ---- 5. local CSV copies ----
        outdir = os.path.join(LOCAL_ROOT, f"cycle_{src_dt}")
        os.makedirs(outdir, exist_ok=True)
        for fname, rows in ((f"t1_t2_{src_dt}.csv", t12), (f"t3_t4_{src_dt}.csv", t34)):
            p = os.path.join(outdir, fname)
            with open(p, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.DictWriter(f, fieldnames=TABLE_COLS)
                w.writeheader(); w.writerows(rows)
            progress(f"  local: {p}")
        return len(t12), len(t34)
    finally:
        try: connh["conn"].close()
        except Exception: pass

if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else f"{dt.datetime.now():%Y-%m-%d}"
    analyze(d)

"""
STEP 6 — Overlapping analysis: who should we re-contact?

Runs AFTER the daily store (datamoon_leads / datamoon_refined / lead_overlaps /
S3 export are all written and verified). Scores every entity in lead_overlaps
behaviorally, drops repliers/opt-outs/deals/cool-off, and writes the T1+T2
targets into two RDS tables:

  recontacting_overlaps  T1/T2 targets WITH a phone number
  upleads_list           T1/T2 targets WITHOUT a phone (email-only; enrich later)

Idempotent per cycle_date: re-running a date replaces that date's rows.
Local CSV copies land in DataMoon/cycle_<dt>/.

Scoring (see prompt_overlap_analysis.txt for the full method and its caveats):
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
Tiers: T1 >=70, T2 >=50, T3 >=35, T4 below. Only T1/T2 are stored.

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
    "recently_worked", "reason",
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
    reason           text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cycle_date, match_key)
);
"""

def _digits10(p):
    d = re.sub(r"\D", "", p or "")
    return d[-10:] if len(d) >= 10 else None

# ---------------------------------------------------------------- inputs
def load_replier_keys(progress=print):
    """Keys of everyone who ever replied via TextTorrent (any verdict) — they are
    handled by the replier track, never by behavioral scoring. Uses the NEWEST
    retarget_replied_* export found; warns about its age because the export is a
    manual snapshot, not something the cycle refreshes."""
    dirs = sorted(glob.glob(os.path.join(LOCAL_ROOT, "retarget_replied_*")))
    keys = set()
    if not dirs:
        progress("  WARNING: no retarget_replied_* export found — replier exclusion OFF")
        return keys
    d = dirs[-1]
    progress(f"  replier export: {os.path.basename(d)} "
             f"(a manual snapshot — drop in a newer one to keep exclusions fresh)")
    for name in ("retarget_companies.csv", "excluded_do_not_contact.csv"):
        p = os.path.join(d, name)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                for ph in re.split(r"[;,]", r.get("phones_replied", "") or ""):
                    k = _digits10(ph)
                    if k: keys.add(k)
                for em in re.split(r"[;,]", r.get("emails", "") or ""):
                    em = em.strip().lower()
                    if em: keys.add(em)
    return keys

def load_worked(progress=print):
    p = os.path.join(ROOT, "recurring_worked_leads.csv")
    worked = {}
    if not os.path.exists(p):
        progress("  WARNING: recurring_worked_leads.csv not found — cool-off/deal exclusion OFF")
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

# ---------------------------------------------------------------- main analysis
def analyze(src_dt, progress=print):
    progress("\nOverlapping analysis (step 6)...")
    repliers = load_replier_keys(progress)
    worked = load_worked(progress)
    enrich = load_cycle_enrich(src_dt, progress)

    connh = {"conn": S.connect()}
    try:
        cur = connh["conn"].cursor()
        cur.execute("""SELECT match_key, key_type, first_name, last_name, company_name,
                              entity_key, matched_on, overlap_count, distinct_days,
                              first_seen, last_seen, seen_dates_text, source_files
                       FROM lead_overlaps""")
        cols = ["match_key","key_type","first_name","last_name","company_name","entity_key",
                "matched_on","overlap_count","distinct_days","first_seen","last_seen",
                "seen_dates_text","source_files"]
        table = [dict(zip(cols, r)) for r in cur.fetchall()]
        progress(f"  lead_overlaps: {len(table):,} keys loaded from RDS")

        best = {}   # entity_key -> best-scoring candidate
        skipped = {"replier": 0, "deal_dni": 0}
        for row in table:
            mk = (row["match_key"] or "").strip().lower()
            key = _digits10(mk) if row["key_type"] == "phone" else mk
            if not key: key = mk
            if key in repliers:
                skipped["replier"] += 1; continue
            wk = worked.get(key)
            if wk and (wk["dni"] or "deal/funded" in wk["verdict"]):
                skipped["deal_dni"] += 1; continue
            en = enrich.get(key, {})
            score, why, intent, n_src, active, rec_worked = score_row(row, en, wk, src_dt)
            tier = tier_of(score)
            if tier not in ("T1", "T2"):
                continue
            phone = en.get("mobile") or (mk if row["key_type"] == "phone" else "")
            email = mk if row["key_type"] == "email" else ""
            cand = {
                "cycle_date": src_dt, "match_key": row["match_key"], "rank": 0,
                "tier": tier, "score": score,
                "first_name": row["first_name"] or "", "last_name": row["last_name"] or "",
                "contact_no": phone, "email": email,
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
                "reason": " | ".join(why),
            }
            ent = row["entity_key"] or key
            old = best.get(ent)
            if not old or cand["score"] > old["score"] or (
                cand["score"] == old["score"] and cand["contact_no"] and not old["contact_no"]):
                best[ent] = cand

        cands = sorted(best.values(),
                       key=lambda c: (c["tier"], -(c["score"]), str(c["last_seen"] or "")),
                       )
        for i, c in enumerate(cands, 1):
            c["rank"] = i
        withphone = [c for c in cands if c["contact_no"]]
        blank = [c for c in cands if not c["contact_no"]]
        progress(f"  excluded: {skipped['replier']:,} replier keys, {skipped['deal_dni']:,} deal/do-not-issue")
        progress(f"  T1/T2 targets: {len(withphone):,} with phone -> recontacting_overlaps, "
                 f"{len(blank):,} without -> upleads_list")

        # ---- store (idempotent per cycle_date) ----
        for name, rows in (("recontacting_overlaps", withphone), ("upleads_list", blank)):
            cur = connh["conn"].cursor()
            cur.execute(DDL.format(name=name))
            cur.execute(f"DELETE FROM {name} WHERE cycle_date=%s", (src_dt,))
            connh["conn"].commit()
            S.copy_rows(connh, name, TABLE_COLS, rows, progress=progress)
            cur = connh["conn"].cursor()
            cur.execute(f"SELECT count(*) FROM {name}")
            progress(f"  {name}: +{len(rows):,} rows for {src_dt} (table total {cur.fetchone()[0]:,})")

        # ---- local CSV copies ----
        outdir = os.path.join(LOCAL_ROOT, f"cycle_{src_dt}")
        os.makedirs(outdir, exist_ok=True)
        for fname, rows in ((f"recontacting_overlaps_{src_dt}.csv", withphone),
                            (f"upleads_list_{src_dt}.csv", blank)):
            p = os.path.join(outdir, fname)
            with open(p, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.DictWriter(f, fieldnames=TABLE_COLS)
                w.writeheader(); w.writerows(rows)
            progress(f"  local: {p}")
        return len(withphone), len(blank)
    finally:
        try: connh["conn"].close()
        except Exception: pass

if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else f"{dt.datetime.now():%Y-%m-%d}"
    analyze(d)

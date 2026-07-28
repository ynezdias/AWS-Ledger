"""
Rebuild `lead_overlaps` as ONE ROW PER MATCHED LEAD KEY.
==========================================================================
The old table was keyed by COMPANY, so every overlapping person at the same
company collapsed into a single row -- "Self-Employed" alone covered 1,129
distinct leads, making its overlap_count of 154 a count of PEOPLE, not of
repeat hits. This rebuild keys by the email/phone that actually matched the
pool, so overlap_count means "how many times THIS record has been overlapped".

The old table is RENAMED, never dropped: lead_overlaps_by_company.

Replays every date chronologically so first_seen/last_seen come out right:
  2026-07-24  <- overlap_history.db  (predates source_file; dates only)
  2026-07-27  <- DataMoon/cycle_2026-07-27/overlapping.csv
  2026-07-28  <- DataMoon/cycle_2026-07-28/overlapping.csv

Usage:  python tools/rebuild_overlaps.py          (DM_SECRET must be set)
"""
import os, sys, sqlite3
import pandas as pd

HERE=os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,HERE)
import pipeline_store as S

ROOT=os.path.dirname(HERE)
SQLITE=os.path.join(ROOT,"overlap_history.db")

def log(m): print(m, flush=True)

def day_from_sqlite(day):
    """07-24 has no overlap export and predates source_file. overlap_history.db
    still records which keys overlapped that day, so dates/counts are exact and
    only the filename is unknown."""
    if not os.path.exists(SQLITE):
        log(f"  {SQLITE} missing — skipping {day}"); return None
    con=sqlite3.connect(SQLITE)
    q=("select match_key, company_name, first_name, last_name from overlap_history "
       "where substr(coalesce(overlap_date_1,''),1,10)=? "
       "   or substr(coalesce(overlap_date_2,''),1,10)=?")
    rows=con.execute(q,(day,day)).fetchall(); con.close()
    if not rows: return None
    return pd.DataFrame([{
        "matched_pool_keys":r[0], "company_name":r[1] or "",
        "first_name":r[2] or "", "last_name":r[3] or "",
        "source_file":"",                 # unknown for this date
    } for r in rows])

def day_from_cycle(day):
    p=os.path.join(ROOT,"DataMoon",f"cycle_{day}","overlapping.csv")
    if not os.path.exists(p):
        log(f"  {p} missing — skipping {day}"); return None
    df=pd.read_csv(p,dtype=str,keep_default_na=False)
    keep=[c for c in ("matched_pool_keys","match_type","email_norm","phone_e164",
                      "source_file","first_name","last_name","company_name") if c in df.columns]
    return df[keep]

DAYS=[("2026-07-24",day_from_sqlite),
      ("2026-07-27",day_from_cycle),
      ("2026-07-28",day_from_cycle)]

def main():
    conn=S.connect(); cur=conn.cursor()

    # ONE table only. Everything here is rebuilt from the raw overlap exports,
    # so a fresh start is always reproducible.
    cur.execute("DROP TABLE IF EXISTS lead_overlaps")
    cur.execute(S.LEAD_OVERLAPS_DDL); conn.commit()
    log("Rebuilding per-lead lead_overlaps from source exports")

    connh={"conn":conn}
    for day,loader in DAYS:
        df=loader(day)
        if df is None or df.empty:
            log(f"{day}: no data"); continue
        log(f"{day}: replaying {len(df):,} overlap rows...")
        S.apply_overlaps(connh, df, day, progress=log)

    conn=connh["conn"]; cur=conn.cursor()
    cur.execute("select count(*), sum(overlap_count), max(overlap_count) from lead_overlaps")
    n,tot,mx=cur.fetchone()
    log(f"\nDONE. {n:,} lead keys, sum(overlap_count)={tot:,}, max={mx}")
    conn.close()

if __name__=="__main__":
    main()

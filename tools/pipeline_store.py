"""
STEP 5 — Daily storage layer.  RDS holds ONLY three tables:

  datamoon_leads    raw normalized + deduped leads for the day
  datamoon_refined  the single main refined final list (net-new, scored)
  lead_overlaps     one clean row per overlapping company/name, with a repeat
                    COUNT and when it FIRST / LAST appeared

Per-lead overlap DETAIL is not kept in RDS — it goes to S3 each day:
  s3://datamoon-raw-data/Overlapping Data/dt=YYYY-MM-DD/overlapping_<dt>.csv

EVERYTHING here is idempotent per source_dt: re-running a day replaces that
day's contribution exactly (including inside lead_overlaps, via day_counts).
"""
import os, sys, io, json, re, csv, subprocess, importlib.util, datetime as dt
import pg8000
import pandas as pd

HOST_FB = "datamoon.c364acm8wlnv.us-east-2.rds.amazonaws.com"
RAW_BUCKET = "datamoon-raw-data"
OVERLAP_PREFIX = "Overlapping Data"

REFINED_COLS = [
    "row_id","source_dt","first_name","last_name","company_name","job_title","email","phone",
    "email_norm","phone_e164","email_valid","phone_valid","name_present","company_present",
    "email_company_match","email_name_match","flags","notes","flag_count","refined_status","refined_at",
]

LEAD_OVERLAPS_DDL = """
CREATE TABLE IF NOT EXISTS lead_overlaps (
    entity_key    text PRIMARY KEY,
    entity_type   text,
    display_name  text,
    overlap_count integer NOT NULL DEFAULT 0,
    distinct_days integer NOT NULL DEFAULT 0,
    first_seen    date,
    last_seen     date,
    seen_dates    date[],
    matched_on    text,
    sample_email  text,
    sample_phone  text,
    day_counts    jsonb NOT NULL DEFAULT '{}'::jsonb
);
"""
# tables created before day_counts existed need it added
LEAD_OVERLAPS_MIGRATE = (
    "ALTER TABLE lead_overlaps "
    "ADD COLUMN IF NOT EXISTS day_counts jsonb NOT NULL DEFAULT '{}'::jsonb"
)

def connect():
    c = json.loads(os.environ["DM_SECRET"])
    return pg8000.connect(user=c["username"], password=c["password"],
        host=c.get("host") or HOST_FB, port=int(c.get("port",5432)),
        database=c.get("dbname") or "datamoon", ssl_context=True, timeout=180)

def load_refiner():
    p=os.path.join(os.path.dirname(os.path.abspath(__file__)),"refine_leads.py")
    spec=importlib.util.spec_from_file_location("rl",p); m=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m); return m

# ---------------------------------------------------------------- fast COPY
def _pg_array(vals):
    if vals is None: return "{}"
    out=[]
    for v in vals:
        s=str(v).replace("\\","\\\\").replace('"','\\"')
        out.append(f'"{s}"')
    return "{"+",".join(out)+"}"

def copy_rows(conn, table, cols, rows, array_cols=(), progress=print):
    """Bulk-load dict rows via COPY ... FROM STDIN WITH CSV (fast for 100k+)."""
    if not rows: return 0
    buf=io.StringIO()
    w=csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    for r in rows:
        line=[]
        for c in cols:
            v=r.get(c)
            if c in array_cols: line.append(_pg_array(v))
            elif v is None: line.append("")
            elif isinstance(v,bool): line.append("true" if v else "false")
            else: line.append(str(v))
        w.writerow(line)
    buf.seek(0)
    cur=conn.cursor()
    cur.execute(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '')", stream=buf)
    conn.commit()
    progress(f"    COPY -> {table}: {len(rows):,} rows")
    return len(rows)

# ---------------------------------------------------------------- datamoon_leads
DM_LEADS_MAP = {   # datamoon_leads column  <-  canonical column
    "first_name":"first_name","last_name":"last_name","email":"personal_emails",
    "business_email":"business_email","additional_emails":"additional_personal_emails",
    "phone":"personal_phone","direct_phone":"direct_number","company_phone":"company_phone",
    "address":"personal_address","address_2":"personal_address_2","city":"personal_city",
    "state":"personal_state","zip":"personal_zip","linkedin_url":"linkedin_url",
    "gender":"gender","age_range":"age_range","married":"married","children":"children",
    "income_range":"income_range","net_worth":"net_worth","homeowner":"homeowner",
    "job_title":"job_title","seniority":"seniority_level","department":"department",
    "company_name":"company_name","company_domain":"company_domain",
    "company_address":"company_address","company_city":"company_city",
    "company_state":"company_state","company_zip":"company_zip",
    "company_revenue":"company_revenue","company_employees":"company_employee_count",
    "naics":"company_naics","sic":"company_sic","industry":"primary_industry",
}
DM_LEADS_COLS = list(DM_LEADS_MAP) + [
    "email_norm","phone_e164","nameaddr_key","is_complete","has_valid_phone","has_valid_email",
    "merged_from_rows","source_dt","row_id","loaded_at",
]

def load_datamoon_leads(conn, unique_df, src_dt, ts, progress=print):
    cur=conn.cursor()
    cur.execute("DELETE FROM datamoon_leads WHERE source_dt=%s",(src_dt,)); conn.commit()
    rows=[]
    for i,r in enumerate(unique_df.to_dict("records")):
        row={k:r.get(v,"") for k,v in DM_LEADS_MAP.items()}
        em=(r.get("email_norm") or "").split(";")[0]
        ph=(r.get("phone_e164") or "").split(";")[0]
        row.update({
            "email_norm":em, "phone_e164":ph, "nameaddr_key":r.get("name_addr_key",""),
            "is_complete":r.get("record_complete","")=="yes",
            "has_valid_phone":bool(ph), "has_valid_email":bool(em),
            "merged_from_rows":r.get("merged_from_rows",""),
            "source_dt":src_dt, "row_id":f"{src_dt}-{i:07d}", "loaded_at":ts.isoformat(),
        })
        rows.append(row)
    n=copy_rows(conn,"datamoon_leads",DM_LEADS_COLS,rows,progress=progress)
    progress(f"  datamoon_leads   = {n:,} rows for {src_dt}")
    return n

# ---------------------------------------------------------------- datamoon_refined
def _first(v): return (v or "").split(";")[0].strip()
def _pick(r,*cols):
    for c in cols:
        v=(r.get(c) or "").strip()
        if v: return v.split(";")[0].split(",")[0].strip()
    return ""

def load_datamoon_refined(conn, netnew_df, src_dt, ts, progress=print):
    rl=load_refiner(); cur=conn.cursor()
    cur.execute("DELETE FROM datamoon_refined WHERE source_dt=%s",(src_dt,)); conn.commit()
    rows=[]
    for i,row in enumerate(netnew_df.to_dict("records")):
        rec=rl.refine({
            "row_id":f"{src_dt}-{i:07d}", "source_dt":src_dt,
            "first_name":row.get("first_name",""), "last_name":row.get("last_name",""),
            "company_name":row.get("company_name",""), "company_domain":row.get("company_domain",""),
            "job_title":row.get("job_title",""),
            "email":_pick(row,"personal_emails","business_email","additional_personal_emails"),
            "phone":_pick(row,"personal_phone","mobile_phone"),
            "email_norm":_first(row.get("email_norm","")),
            "phone_e164":_first(row.get("phone_e164","")),
        })
        rec["refined_at"]=ts.isoformat()
        rows.append(rec)
    n=copy_rows(conn,"datamoon_refined",REFINED_COLS,rows,array_cols=("flags","notes"),progress=progress)
    clean=sum(1 for r in rows if r["refined_status"]=="clean")
    progress(f"  datamoon_refined = {n:,} rows for {src_dt}  ({clean:,} clean / {n-clean:,} flagged)")
    return n

# ---------------------------------------------------------------- lead_overlaps
def _norm(s): return re.sub(r"[^a-z0-9]+"," ",(s or "").lower()).strip()

def entity_of(company, first, last, fallback):
    c=_norm(company)
    if c: return "co:"+c, "company", (company or "").strip()
    p=_norm(f"{first or ''} {last or ''}")
    if p: return "pn:"+p, "person", f"{(first or '').strip()} {(last or '').strip()}".strip()
    return "key:"+(fallback or ""), "contact", (fallback or "")

def apply_overlaps(conn, overlap_df, src_dt, progress=print):
    """Fold one day's overlaps into the per-entity summary. Idempotent: the day's
    contribution is stored in day_counts, so re-running replaces it exactly."""
    cur=conn.cursor(); cur.execute(LEAD_OVERLAPS_DDL); cur.execute(LEAD_OVERLAPS_MIGRATE); conn.commit()
    day=src_dt
    agg={}
    for r in overlap_df.to_dict("records"):
        em=_first(r.get("email_norm","")); ph=_first(r.get("phone_e164",""))
        ek,et,dn=entity_of(r.get("company_name"),r.get("first_name"),r.get("last_name"), em or ph)
        a=agg.setdefault(ek,{"type":et,"name":dn,"n":0,"m":set(),"email":"","phone":""})
        a["n"]+=1
        # accept either `match_type` (new cycle output) or matched_on_email/phone (legacy)
        mt=str(r.get("match_type","") or "")
        if not mt:
            mt="+".join([t for t,f in (("email",r.get("matched_on_email")),("phone",r.get("matched_on_phone")))
                         if str(f).lower() in ("true","t","1")])
        for tok in mt.replace("+"," ").split():
            if tok in ("phone","email"): a["m"].add(tok)
        if not a["email"] and em: a["email"]=em
        if not a["phone"] and ph: a["phone"]=ph

    # rows to reconcile = entities in today's data + any that already carry this day
    cur.execute("SELECT entity_key, day_counts, matched_on, sample_email, sample_phone, "
                "entity_type, display_name FROM lead_overlaps WHERE day_counts ? %s",(day,))
    existing={r[0]:r for r in cur.fetchall()}
    keys=list(agg)
    B=20000
    for i in range(0,len(keys),B):
        cur.execute("SELECT entity_key, day_counts, matched_on, sample_email, sample_phone, "
                    "entity_type, display_name FROM lead_overlaps WHERE entity_key = ANY(%s)",(keys[i:i+B],))
        for r in cur.fetchall(): existing[r[0]]=r

    ins=[]; upd=[]; dele=[]
    touched=set(agg)|set(existing)
    for ek in touched:
        ex=existing.get(ek)
        old_dc = dict(ex[1] or {}) if ex else {}
        old_dc.pop(day, None)                     # drop this day's previous contribution
        a=agg.get(ek)
        if a: old_dc[day]=a["n"]                  # ...and re-add the current one
        if not old_dc:
            dele.append(ek); continue
        # date[] / date columns need real date objects, not strings
        days=[dt.date.fromisoformat(d) for d in sorted(old_dc)]
        cnt=sum(int(v) for v in old_dc.values())
        toks=set()
        if ex and ex[2]: toks|=set(str(ex[2]).replace("+"," ").split())
        if a: toks|=a["m"]
        etype = (a["type"] if a else ex[5]); dname=(a["name"] if a else ex[6])
        email = (ex[3] if ex and ex[3] else (a["email"] if a else "")) or None
        phone = (ex[4] if ex and ex[4] else (a["phone"] if a else "")) or None
        rec=(etype,dname,cnt,len(days),days[0],days[-1],days,
             "+".join(sorted(toks)) if toks else None,email,phone,json.dumps(old_dc),ek)
        (upd if ex else ins).append(rec)

    if dele:
        cur.execute("DELETE FROM lead_overlaps WHERE entity_key = ANY(%s)",(dele,))
    # Bulk upsert through a temp table + one INSERT ... ON CONFLICT DO UPDATE:
    # per-row UPDATEs are ~1 network round-trip each (15k rows ≈ 10+ min on RDS).
    allrecs = ins + upd
    if allrecs:
        cur.execute("DROP TABLE IF EXISTS lo_stage")
        cur.execute("""CREATE TEMP TABLE lo_stage (
            entity_type text, display_name text, overlap_count integer, distinct_days integer,
            first_seen date, last_seen date, seen_dates date[], matched_on text,
            sample_email text, sample_phone text, day_counts jsonb, entity_key text)""")
        stage_cols=["entity_type","display_name","overlap_count","distinct_days","first_seen",
                    "last_seen","seen_dates","matched_on","sample_email","sample_phone",
                    "day_counts","entity_key"]
        stage_rows=[]
        for (etype,dname,cnt,ndays,fs,ls,days,mo,email,phone,dc,ek) in allrecs:
            stage_rows.append({
                "entity_type":etype,"display_name":dname,"overlap_count":cnt,"distinct_days":ndays,
                "first_seen":fs.isoformat(),"last_seen":ls.isoformat(),
                "seen_dates":[d.isoformat() for d in days],
                "matched_on":mo,"sample_email":email,"sample_phone":phone,
                "day_counts":dc,"entity_key":ek})
        copy_rows(conn,"lo_stage",stage_cols,stage_rows,array_cols=("seen_dates",),
                  progress=lambda m: None)
        cur.execute("""INSERT INTO lead_overlaps AS t (entity_type,display_name,overlap_count,
            distinct_days,first_seen,last_seen,seen_dates,matched_on,sample_email,sample_phone,
            day_counts,entity_key)
            SELECT entity_type,display_name,overlap_count,distinct_days,first_seen,last_seen,
                   seen_dates,matched_on,sample_email,sample_phone,day_counts,entity_key
            FROM lo_stage
            ON CONFLICT (entity_key) DO UPDATE SET
              entity_type=EXCLUDED.entity_type, display_name=EXCLUDED.display_name,
              overlap_count=EXCLUDED.overlap_count, distinct_days=EXCLUDED.distinct_days,
              first_seen=EXCLUDED.first_seen, last_seen=EXCLUDED.last_seen,
              seen_dates=EXCLUDED.seen_dates, matched_on=EXCLUDED.matched_on,
              sample_email=EXCLUDED.sample_email, sample_phone=EXCLUDED.sample_phone,
              day_counts=EXCLUDED.day_counts""")
        cur.execute("DROP TABLE lo_stage")
    conn.commit()
    progress(f"  lead_overlaps    : {len(ins):,} new entities, {len(upd):,} updated, {len(dele):,} removed")
    return len(ins),len(upd)

# ---------------------------------------------------------------- S3 export
def upload_overlaps_s3(local_csv, src_dt, progress=print):
    uri=f"s3://{RAW_BUCKET}/{OVERLAP_PREFIX}/dt={src_dt}/overlapping_{src_dt}.csv"
    subprocess.check_call(["aws","s3","cp",local_csv,uri,"--region","us-east-2","--only-show-errors"])
    progress(f"  overlaps -> {uri}")
    return uri

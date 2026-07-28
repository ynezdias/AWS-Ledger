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

# ONE ROW PER MATCHED LEAD KEY (the email or phone that actually hit the pool).
#
# The previous version keyed this table by COMPANY, which silently blended many
# different people into one row -- "Self-Employed" alone covered 1,129 distinct
# leads, so its overlap_count of 154 counted people, not repeat hits. Keying by
# the matched key makes overlap_count mean what it says: the number of times
# THIS record has been found already in the pool.
LEAD_OVERLAPS_DDL = """
CREATE TABLE IF NOT EXISTS lead_overlaps (
    match_key       text PRIMARY KEY,
    key_type        text,
    first_name      text,
    last_name       text,
    company_name    text,
    entity_key      text,
    entity_type     text,
    display_name    text,
    matched_on      text,
    overlap_count   integer NOT NULL DEFAULT 0,
    distinct_days   integer NOT NULL DEFAULT 0,
    first_seen      date,
    last_seen       date,
    seen_dates      date[],
    seen_dates_text text,
    matched_source  text,
    pool_tables     text,
    source_files    text,
    day_counts      jsonb NOT NULL DEFAULT '{}'::jsonb,
    day_files       jsonb NOT NULL DEFAULT '{}'::jsonb
);
"""
LEAD_OVERLAPS_MIGRATE = (
    "ALTER TABLE lead_overlaps "
    "ADD COLUMN IF NOT EXISTS day_counts jsonb NOT NULL DEFAULT '{}'::jsonb, "
    "ADD COLUMN IF NOT EXISTS day_files jsonb NOT NULL DEFAULT '{}'::jsonb, "
    "ADD COLUMN IF NOT EXISTS seen_dates_text text, "
    "ADD COLUMN IF NOT EXISTS matched_source text, "
    "ADD COLUMN IF NOT EXISTS pool_tables text, "
    "ADD COLUMN IF NOT EXISTS source_files text, "
    "ADD COLUMN IF NOT EXISTS entity_key text, "
    "ADD COLUMN IF NOT EXISTS entity_type text, "
    "ADD COLUMN IF NOT EXISTS display_name text, "
    "ADD COLUMN IF NOT EXISTS matched_on text"
)

# Everything the cycle checks today is the AWS lead pool; the column exists so a
# future DataMoon-history comparison can be told apart from a pool hit.
POOL_SOURCE = "leadpool"

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

# Rows per COPY chunk. Raised 25k->100k (2026-07-28, user asked for bigger
# batches); each chunk still commits independently and retries on network
# error, so the only cost of a drop is resending one larger chunk.
COPY_CHUNK = int(os.environ.get("DM_COPY_CHUNK", "100000"))
# A batch this small that still won't go through is a real error, not congestion.
MIN_COPY_CHUNK = int(os.environ.get("DM_MIN_COPY_CHUNK", "5000"))

def copy_rows(connh, table, cols, rows, array_cols=(), progress=print):
    """Bulk-load dict rows via COPY in CHUNKS, committing each chunk.
    `connh` is a dict {"conn": <connection>} so we can swap in a fresh
    connection after a network error and retry ONLY the failed chunk
    (already-committed chunks are never resent -> no duplicates).

    A chunk that keeps failing is SPLIT IN HALF and retried rather than
    aborting the load: big batches are much faster on a healthy link but are
    the first thing to die on a flaky one, so we degrade the batch size
    instead of the whole run."""
    if not rows: return 0
    import time
    def render(batch):
        buf=io.StringIO()
        w=csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        for r in batch:
            line=[]
            for c in cols:
                v=r.get(c)
                if c in array_cols: line.append(_pg_array(v))
                elif v is None: line.append("")
                elif isinstance(v,bool): line.append("true" if v else "false")
                else: line.append(str(v))
            w.writerow(line)
        buf.seek(0); return buf
    sql=f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '')"

    def reconnect():
        """Re-establishing the link can itself fail; retry instead of letting
        that kill a load that is otherwise fine."""
        try: connh["conn"].close()
        except Exception: pass
        for t in range(5):
            try:
                connh["conn"]=connect(); return
            except Exception as e:
                progress(f"    reconnect failed ({type(e).__name__}), retry {t+1}/5")
                time.sleep(3*(t+1))
        raise RuntimeError("could not re-establish a database connection")

    def send(batch):
        for attempt in range(4):
            try:
                cur=connh["conn"].cursor()
                cur.execute(sql, stream=render(batch))
                connh["conn"].commit()
                return
            except Exception as e:
                progress(f"    COPY -> {table}: {type(e).__name__} on {len(batch):,} rows "
                         f"(attempt {attempt+1}/4) — reconnecting")
                reconnect()
                time.sleep(2)
        if len(batch) > MIN_COPY_CHUNK:
            half=len(batch)//2
            progress(f"    COPY -> {table}: splitting {len(batch):,} into 2 x ~{half:,}")
            send(batch[:half]); send(batch[half:])
            return
        raise RuntimeError(f"COPY into {table} failed on a {len(batch):,}-row batch")

    done=0
    for i in range(0,len(rows),COPY_CHUNK):
        batch=rows[i:i+COPY_CHUNK]
        send(batch)
        done+=len(batch)
        if (i//COPY_CHUNK)%8==7 or done==len(rows):
            progress(f"    COPY -> {table}: {done:,}/{len(rows):,}")
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
    "email_norm","phone_e164","email_all","phone_all",
    "nameaddr_key","is_complete","has_valid_phone","has_valid_email",
    "merged_from_rows","source_file","source_dt","row_id","loaded_at",
]

# email_norm/phone_e164 keep ONE key each for compatibility with anything that
# joins them straight to the lead pool. A deduped lead can legitimately own
# several (its merged duplicates' keys), so the full ';'-joined set is kept
# alongside rather than discarded.
# source_file: the raw filename(s) the lead came from — ';'-joined when a merge
# group spans more than one file.
DM_LEADS_MIGRATE = (
    "ALTER TABLE datamoon_leads "
    "ADD COLUMN IF NOT EXISTS email_all text, "
    "ADD COLUMN IF NOT EXISTS phone_all text, "
    "ADD COLUMN IF NOT EXISTS source_file text"
)

# The load is DELETE-by-source_dt then COPY, which is idempotent for runs that
# happen ONE AT A TIME but not for overlapping ones: two runs can both delete,
# then both insert, leaving two complete copies of the day. This unique index
# makes that collide loudly instead of silently doubling the table.
# (datamoon_refined already has UNIQUE(row_id), which is why it never doubled.)
DM_LEADS_UNIQUE = (
    "CREATE UNIQUE INDEX IF NOT EXISTS datamoon_leads_srcdt_rowid_uidx "
    "ON datamoon_leads (source_dt, row_id)"
)

def load_datamoon_leads(connh, unique_df, src_dt, ts, progress=print):
    cur=connh["conn"].cursor()
    cur.execute(DM_LEADS_MIGRATE)
    cur.execute(DM_LEADS_UNIQUE)
    cur.execute("DELETE FROM datamoon_leads WHERE source_dt=%s",(src_dt,)); connh["conn"].commit()
    rows=[]
    for i,r in enumerate(unique_df.to_dict("records")):
        row={k:r.get(v,"") for k,v in DM_LEADS_MAP.items()}
        em_all=(r.get("email_norm") or ""); ph_all=(r.get("phone_e164") or "")
        em=em_all.split(";")[0]; ph=ph_all.split(";")[0]
        row.update({
            "email_norm":em, "phone_e164":ph,
            "email_all":em_all, "phone_all":ph_all,
            "nameaddr_key":r.get("name_addr_key",""),
            "is_complete":r.get("record_complete","")=="yes",
            "has_valid_phone":bool(ph), "has_valid_email":bool(em),
            "merged_from_rows":r.get("merged_from_rows",""),
            "source_file":r.get("source_file",""),
            "source_dt":src_dt, "row_id":f"{src_dt}-{i:07d}", "loaded_at":ts.isoformat(),
        })
        rows.append(row)
    n=copy_rows(connh,"datamoon_leads",DM_LEADS_COLS,rows,progress=progress)
    progress(f"  datamoon_leads   = {n:,} rows for {src_dt}")
    return n

# ---------------------------------------------------------------- datamoon_refined
def _first(v): return (v or "").split(";")[0].strip()
def _pick(r,*cols):
    for c in cols:
        v=(r.get(c) or "").strip()
        if v: return v.split(";")[0].split(",")[0].strip()
    return ""

DM_REFINED_MIGRATE = (
    "ALTER TABLE datamoon_refined ADD COLUMN IF NOT EXISTS source_file text"
)

def load_datamoon_refined(connh, netnew_df, src_dt, ts, progress=print):
    rl=load_refiner(); cur=connh["conn"].cursor()
    cur.execute(DM_REFINED_MIGRATE)
    cur.execute("DELETE FROM datamoon_refined WHERE source_dt=%s",(src_dt,)); connh["conn"].commit()
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
        rec["source_file"]=row.get("source_file","")
        rows.append(rec)
    n=copy_rows(connh,"datamoon_refined",REFINED_COLS+["source_file"],rows,
                array_cols=("flags","notes"),progress=progress)
    clean=sum(1 for r in rows if r["refined_status"]=="clean")
    progress(f"  datamoon_refined = {n:,} rows for {src_dt}  ({clean:,} clean / {n-clean:,} flagged)")
    return n

# ---------------------------------------------------------------- lead_overlaps
def _norm(s): return re.sub(r"[^a-z0-9]+"," ",(s or "").lower()).strip()

def entity_of(company, first, last, fallback):
    """Company/person rollup, carried over from the old per-company table so a
    single table can still answer "which accounts keep coming back". Computed
    per LEAD here, so it groups rather than blends."""
    c=_norm(company)
    if c: return "co:"+c, "company", (company or "").strip()
    p=_norm(f"{first or ''} {last or ''}")
    if p: return "pn:"+p, "person", f"{(first or '').strip()} {(last or '').strip()}".strip()
    return "key:"+(fallback or ""), "contact", (fallback or "")

def key_type_of(k):
    """The pool stores emails and E.164 phones in separate tables."""
    return "email" if "@" in k else "phone"

POOL_TABLE = {"email": "lead_emails", "phone": "lead_phones"}

def _matched_keys(r):
    """The pool keys this row actually hit. `matched_pool_keys` is written by the
    cycle; fall back to the row's own keys for older exports that lack it."""
    mk=[k.strip() for k in str(r.get("matched_pool_keys","") or "").split(";") if k.strip()]
    if mk: return mk
    mt=str(r.get("match_type","") or "")
    out=[]
    if "email" in mt: out+= [k for k in str(r.get("email_norm","") or "").split(";") if k]
    if "phone" in mt: out+= [k for k in str(r.get("phone_e164","") or "").split(";") if k]
    return out

def apply_overlaps(connh, overlap_df, src_dt, progress=print):
    """Fold one day's overlaps into the per-LEAD summary, one row per matched
    pool key. Idempotent: the day's contribution lives in day_counts/day_files,
    so re-running a date replaces exactly that date. Single transaction, so a
    mid-way network failure rolls back cleanly and the call can be retried."""
    if isinstance(connh, dict): conn=connh["conn"]
    else: conn=connh
    cur=conn.cursor(); cur.execute(LEAD_OVERLAPS_DDL); cur.execute(LEAD_OVERLAPS_MIGRATE); conn.commit()
    day=src_dt

    agg={}
    for r in overlap_df.to_dict("records"):
        files={f.strip() for f in str(r.get("source_file","") or "").split(";") if f.strip()}
        mt={t for t in str(r.get("match_type","") or "").replace("+"," ").split()
            if t in ("email","phone")}
        for k in _matched_keys(r):
            a=agg.setdefault(k,{"n":0,"files":set(),"mt":set(),
                                "first":(r.get("first_name") or "").strip(),
                                "last":(r.get("last_name") or "").strip(),
                                "co":(r.get("company_name") or "").strip()})
            a["n"]+=1
            a["files"]|=files
            # no match_type on the 07-24 backfill: the key itself tells us
            a["mt"]|= (mt or {key_type_of(k)})
            for fld,src in (("first","first_name"),("last","last_name"),("co","company_name")):
                if not a[fld]: a[fld]=(r.get(src) or "").strip()

    # rows to reconcile = keys in today's data + any row that already carries this day
    SEL=("SELECT match_key, day_counts, day_files, first_name, last_name, company_name, "
         "matched_on FROM lead_overlaps ")
    cur.execute(SEL+"WHERE day_counts ? %s",(day,))
    existing={r[0]:r for r in cur.fetchall()}
    keys=list(agg)
    B=100000
    for i in range(0,len(keys),B):
        cur.execute(SEL+"WHERE match_key = ANY(%s)",(keys[i:i+B],))
        for r in cur.fetchall(): existing[r[0]]=r

    ins=[]; upd=[]; dele=[]
    for mk in set(agg)|set(existing):
        ex=existing.get(mk)
        old_dc = dict(ex[1] or {}) if ex else {}
        old_df = dict(ex[2] or {}) if ex else {}
        old_dc.pop(day, None); old_df.pop(day, None)   # drop this day's prior contribution
        a=agg.get(mk)
        if a:                                          # ...and re-add the current one
            old_dc[day]=a["n"]
            if a["files"]: old_df[day]=";".join(sorted(a["files"]))
        if not old_dc:
            dele.append(mk); continue
        days=[dt.date.fromisoformat(d) for d in sorted(old_dc)]
        cnt=sum(int(v) for v in old_dc.values())
        files=sorted({f for v in old_df.values() for f in str(v).split(";") if f})
        kt=key_type_of(mk)
        first=(a["first"] if a and a["first"] else (ex[3] if ex else "")) or None
        last =(a["last"]  if a and a["last"]  else (ex[4] if ex else "")) or None
        co   =(a["co"]    if a and a["co"]    else (ex[5] if ex else "")) or None
        toks=set()
        if ex and ex[6]: toks|=set(str(ex[6]).replace("+"," ").split())
        if a: toks|=a["mt"]
        ek,et,dn=entity_of(co,first,last,mk)
        rec=(kt,first,last,co,ek,et,dn,"+".join(sorted(toks)) or None,
             cnt,len(days),days[0],days[-1],days,
             "; ".join(d.isoformat() for d in days),          # seen_dates_text
             POOL_SOURCE, POOL_TABLE[kt], ";".join(files) or None,
             json.dumps(old_dc), json.dumps(old_df), mk)
        (upd if ex else ins).append(rec)

    if dele:
        cur.execute("DELETE FROM lead_overlaps WHERE match_key = ANY(%s)",(dele,))
    # Bulk upsert through a temp table + one INSERT ... ON CONFLICT DO UPDATE:
    # per-row UPDATEs are ~1 network round-trip each (15k rows ≈ 10+ min on RDS).
    allrecs = ins + upd
    if allrecs:
        cols=["key_type","first_name","last_name","company_name","entity_key","entity_type",
              "display_name","matched_on","overlap_count","distinct_days",
              "first_seen","last_seen","seen_dates","seen_dates_text","matched_source",
              "pool_tables","source_files","day_counts","day_files","match_key"]
        cur.execute("DROP TABLE IF EXISTS lo_stage")
        cur.execute("""CREATE TEMP TABLE lo_stage (
            key_type text, first_name text, last_name text, company_name text,
            entity_key text, entity_type text, display_name text, matched_on text,
            overlap_count integer, distinct_days integer, first_seen date, last_seen date,
            seen_dates date[], seen_dates_text text, matched_source text, pool_tables text,
            source_files text, day_counts jsonb, day_files jsonb, match_key text)""")
        stage_rows=[]
        for rec in allrecs:
            d=dict(zip(cols,rec))
            d["first_seen"]=d["first_seen"].isoformat(); d["last_seen"]=d["last_seen"].isoformat()
            d["seen_dates"]=[x.isoformat() for x in d["seen_dates"]]
            stage_rows.append(d)
        # NB: lo_stage is a TEMP table — if copy_rows has to reconnect, the new
        # session won't have it and the INSERT below fails; the caller's retry
        # loop then re-runs this whole (idempotent) function on the new conn.
        ch={"conn":conn}
        copy_rows(ch,"lo_stage",cols,stage_rows,array_cols=("seen_dates",),progress=lambda m: None)
        conn=ch["conn"]; cur=conn.cursor()
        if isinstance(connh, dict): connh["conn"]=conn
        setters=",".join(f"{c}=EXCLUDED.{c}" for c in cols if c!="match_key")
        cur.execute(f"""INSERT INTO lead_overlaps AS t ({','.join(cols)})
            SELECT {','.join(cols)} FROM lo_stage
            ON CONFLICT (match_key) DO UPDATE SET {setters}""")
        cur.execute("DROP TABLE lo_stage")
    conn.commit()
    progress(f"  lead_overlaps    : {len(ins):,} new keys, {len(upd):,} updated, {len(dele):,} removed")
    return len(ins),len(upd)

# ---------------------------------------------------------------- S3 export
def upload_overlaps_s3(local_csv, src_dt, progress=print):
    uri=f"s3://{RAW_BUCKET}/{OVERLAP_PREFIX}/dt={src_dt}/overlapping_{src_dt}.csv"
    subprocess.check_call(["aws","s3","cp",local_csv,uri,"--region","us-east-2","--only-show-errors"])
    progress(f"  overlaps -> {uri}")
    return uri

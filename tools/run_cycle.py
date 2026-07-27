"""
DataMoon daily cycle — ONE COMMAND, end to end.
==========================================================================
Reads every raw file you uploaded for a date, then:

  1. COMBINE    all files under s3://datamoon-raw-data/raw/dt=<DT>/
                (.csv / .csv.gz / .xlsx / .jsonl.gz — any DataMoon layout)
  2. CLEAN + NORMALIZE to the canonical schema (email / E.164 phone / name+addr)
  3. DEDUP      transitive union-find; winner keeps the most complete record,
                blanks back-filled from its duplicates. No row is dropped.
  4. OVERLAP    vs the AWS lead pool (leadpool.lead_emails + lead_phones)
  5. STORE      datamoon_leads    <- all unique normalized rows for the day
                datamoon_refined  <- the net-new refined final list (scored)
                lead_overlaps     <- per-company repeat count + first/last seen
                S3 "Overlapping Data/dt=<DT>/overlapping_<DT>.csv" <- detail
                (local copies under DataMoon/cycle_<DT>/)

Idempotent: re-running a date replaces exactly that date's contribution.

Usage:
    tools/run_cycle.ps1                 # today
    tools/run_cycle.ps1 2026-07-28      # a specific date
"""
import os, sys, json, csv, subprocess, datetime as dt, importlib.util
import pandas as pd

HERE=os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,HERE)
import dm_normalize as N
import pipeline_store as S

RAW_BUCKET="datamoon-raw-data"
LOCAL_ROOT=r"c:/Users/ydias/OneDrive/Desktop/AWS Ledger/DataMoon"
WORK=os.path.join(os.environ.get("TEMP","/tmp"),"datamoon_cycle")

def log(m): print(m, flush=True)

def sh(*a):
    return subprocess.check_output(list(a), text=True)

# ---------------------------------------------------------------- 1. fetch raw
def fetch_raw(src_dt):
    prefix=f"raw/dt={src_dt}/"
    out=sh("aws","s3api","list-objects-v2","--bucket",RAW_BUCKET,"--prefix",prefix,
           "--region","us-east-2","--query","Contents[].{k:Key,s:Size}","--output","json")
    objs=json.loads(out) if out.strip() and out.strip()!="null" else []
    objs=[o for o in objs if o["s"]>0 and not o["k"].endswith("/")]
    if not objs:
        raise SystemExit(f"No raw files found under s3://{RAW_BUCKET}/{prefix}\n"
                         f"Upload your files there first, then re-run.")
    d=os.path.join(WORK,src_dt); os.makedirs(d,exist_ok=True)
    files=[]
    log(f"Found {len(objs)} raw object(s) under {prefix}:")
    for o in objs:
        name=o["k"].split("/")[-1]
        local=os.path.join(d,name)
        if not (os.path.exists(local) and os.path.getsize(local)==o["s"]):
            subprocess.check_call(["aws","s3","cp",f"s3://{RAW_BUCKET}/{o['k']}",local,
                                   "--region","us-east-2","--only-show-errors"])
        log(f"  {o['s']/1e6:9.1f} MB  {name}")
        files.append(local)
    return files

# ---------------------------------------------------------------- 2-3. normalize+dedup
def normalize_dedup(files):
    frames=[]; total=0
    for f in files:
        n=0
        for chunk in N.load_any(f):
            frames.append(chunk); n+=len(chunk)
        log(f"  loaded {n:>9,}  {os.path.basename(f)}"); total+=n
    df=pd.concat(frames,ignore_index=True).fillna(""); del frames
    log(f"TOTAL rows in: {total:,}")
    log("Normalizing + transitive dedup...")
    uniq=N.dedupe(df, progress=log)
    log(f"Unique after dedup: {len(uniq):,}  (internal duplicates: {total-len(uniq):,})")
    return total, uniq

# ---------------------------------------------------------------- 4. overlap
def overlap(uniq):
    secret=sh("aws","secretsmanager","get-secret-value","--secret-id","lead-pool/postgres",
              "--region","us-east-2","--query","SecretString","--output","text")
    d=json.loads(secret)
    import pg8000
    conn=pg8000.connect(host=d.get("host") or "lead-pool.c364acm8wlnv.us-east-2.rds.amazonaws.com",
        port=int(d.get("port",5432)), database=d.get("dbname") or "leadpool",
        user=d.get("username"), password=d.get("password"), ssl_context=True, timeout=180)
    cur=conn.cursor()
    def match(keys, tbl, col):
        if not keys: return set()
        cur.execute("DROP TABLE IF EXISTS dm_keys"); conn.commit()
        cur.execute("CREATE TEMP TABLE dm_keys (v text)")
        for i in range(0,len(keys),50000):
            cur.execute("INSERT INTO dm_keys(v) SELECT unnest(%s::text[])",(keys[i:i+50000],))
        conn.commit()
        cur.execute(f"CREATE INDEX ON dm_keys(v)")
        cur.execute(f"SELECT DISTINCT k.v FROM dm_keys k JOIN {tbl} t ON t.{col}=k.v")
        m={r[0] for r in cur.fetchall()}
        cur.execute("DROP TABLE dm_keys"); conn.commit()
        return m
    allph=list({k for v in uniq["phone_e164"] for k in str(v).split(";") if k})
    allem=list({k for v in uniq["email_norm"] for k in str(v).split(";") if k})
    log(f"Matching {len(allph):,} phones / {len(allem):,} emails vs lead pool...")
    mph=match(allph,"lead_phones","phone_e164")
    mem=match(allem,"lead_emails","email_norm")
    conn.close()
    log(f"  matched keys: {len(mph):,} phones, {len(mem):,} emails")

    inpool=[];mt=[];mk=[]
    for pev,emv in zip(uniq["phone_e164"],uniq["email_norm"]):
        p=[k for k in str(pev).split(";") if k and k in mph]
        e=[k for k in str(emv).split(";") if k and k in mem]
        if p or e:
            inpool.append("yes"); mt.append("phone+email" if (p and e) else ("phone" if p else "email"))
            mk.append(";".join(p+e))
        else: inpool.append("no"); mt.append(""); mk.append("")
    uniq=uniq.copy()
    uniq["in_lead_pool"]=inpool; uniq["match_type"]=mt; uniq["matched_pool_keys"]=mk
    return uniq

# ---------------------------------------------------------------- main
def main(src_dt):
    ts=dt.datetime.now(dt.timezone.utc)
    log(f"\n{'='*64}\nDataMoon cycle  dt={src_dt}   started {ts:%Y-%m-%d %H:%M:%S} UTC\n{'='*64}")
    files=fetch_raw(src_dt)
    rows_in, uniq = normalize_dedup(files)
    uniq = overlap(uniq)

    ov=uniq[uniq["in_lead_pool"]=="yes"].copy()
    nn=uniq[uniq["in_lead_pool"]=="no"].copy()

    outdir=os.path.join(LOCAL_ROOT,f"cycle_{src_dt}"); os.makedirs(outdir,exist_ok=True)
    p_all=os.path.join(outdir,"combined_normalized_unique.csv")
    p_ov =os.path.join(outdir,"overlapping.csv")
    p_nn =os.path.join(outdir,"final_ready_leads.csv")
    uniq.to_csv(p_all,index=False,quoting=csv.QUOTE_MINIMAL)
    ov.to_csv(p_ov,index=False,quoting=csv.QUOTE_MINIMAL)
    nn.drop(columns=["in_lead_pool","match_type","matched_pool_keys"]).to_csv(p_nn,index=False,quoting=csv.QUOTE_MINIMAL)

    log("\nStoring to AWS...")
    conn=S.connect()
    try:
        S.load_datamoon_leads(conn, uniq, src_dt, ts, progress=log)
        S.load_datamoon_refined(conn, nn, src_dt, ts, progress=log)
        S.apply_overlaps(conn, ov, src_dt, progress=log)
        S.upload_overlaps_s3(p_ov, src_dt, progress=log)
        cur=conn.cursor()
        cur.execute("SELECT count(*) FROM datamoon_refined"); tot_ref=cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM lead_overlaps"); tot_ent=cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM lead_overlaps WHERE distinct_days>1"); rep=cur.fetchone()[0]
    finally:
        conn.close()

    U=len(uniq); n_ov=len(ov); n_nn=len(nn)
    rep_txt=f"""DataMoon Cycle Report — dt={src_dt}
{'='*56}
Raw files processed:        {len(files)}
Rows in:                    {rows_in:,}
Internal duplicates:        {rows_in-U:,}
Unique normalized:          {U:,}

OVERLAP vs AWS lead pool (phone OR email; EIN not present in DataMoon)
  Already in pool:          {n_ov:,}  ({100*n_ov/U:.1f}%)
  NET-NEW refined:          {n_nn:,}  ({100*n_nn/U:.1f}%)

RECONCILE  {rows_in:,} = {rows_in-U:,} dup + {n_ov:,} overlap + {n_nn:,} net-new  ->  {'BALANCED' if (rows_in-U)+n_ov+n_nn==rows_in else 'MISMATCH'}

Stored in RDS datamoon:
  datamoon_leads     {U:,} rows for this date
  datamoon_refined   {n_nn:,} rows for this date   (table total {tot_ref:,})
  lead_overlaps      {tot_ent:,} entities tracked, {rep:,} seen on >1 day
S3: s3://{S.RAW_BUCKET}/{S.OVERLAP_PREFIX}/dt={src_dt}/overlapping_{src_dt}.csv
Local: {outdir}
"""
    open(os.path.join(outdir,"CYCLE_REPORT.txt"),"w").write(rep_txt)
    log("\n"+rep_txt)

if __name__=="__main__":
    d = sys.argv[1] if len(sys.argv)>1 else f"{dt.datetime.now():%Y-%m-%d}"
    main(d)

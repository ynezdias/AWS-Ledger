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
  6. ANALYZE    overlap_analysis.py scores lead_overlaps behaviorally and
                stores T1/T2 targets: recontacting_overlaps (with phone) and
                upleads_list (email-only). Non-fatal: an analysis failure
                never rolls back the store above.

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
# Below this share of rows carrying an email/phone we assume the layout wasn't
# recognised rather than that the data is genuinely that poor. Override with
# MIN_KEY_YIELD=0 to force a run through.
MIN_KEY_YIELD = float(os.environ.get("MIN_KEY_YIELD", "0.5"))

def normalize_dedup(files):
    frames=[]; total=0; stats={"bad_lines":0}
    for f in files:
        n=0; base=N.canon_source(os.path.basename(f))
        if base != os.path.basename(f):
            log(f"  source name canonicalised: {os.path.basename(f)!r} -> {base!r}")
        for chunk in N.load_any(f, stats=stats):
            # Provenance: which raw file this row arrived in. dedupe() unions
            # these across a merge group, so a lead built from rows in two files
            # ends up listing both.
            chunk["_source_file"]=base
            frames.append(chunk); n+=len(chunk)
        log(f"  loaded {n:>9,}  {base}"); total+=n
    df=pd.concat(frames,ignore_index=True).fillna(""); del frames
    log(f"TOTAL rows in: {total:,}")
    if stats["bad_lines"]:
        log(f"  WARNING: {stats['bad_lines']:,} unparseable line(s) skipped — "
            f"these are NOT in the counts above")

    # Refuse to publish a confident-looking empty result (see N.key_yield).
    ky=N.key_yield(df)
    log(f"Key yield: {100*ky:.1f}% of rows have an email or phone")
    if ky < MIN_KEY_YIELD:
        raise SystemExit(
            f"\nABORT: only {100*ky:.1f}% of rows carry an email or phone "
            f"(threshold {100*MIN_KEY_YIELD:.0f}%).\n"
            f"This almost always means a NEW column layout that dm_normalize.UPPER_MAP\n"
            f"does not cover, so every match key came out blank. Nothing was written.\n"
            f"Columns seen: {sorted(c for c in df.columns if not c.startswith('_'))[:15]}\n"
            f"Fix the mapping, or set MIN_KEY_YIELD=0 to force the run.")

    log("Normalizing + transitive dedup...")
    uniq=N.dedupe(df, progress=log)
    log(f"Unique after dedup: {len(uniq):,}  (internal duplicates: {total-len(uniq):,})")

    # REAL integrity check: dedupe records each winner's group size, so those
    # sizes must add back up to the row count we read. This catches a row being
    # dropped or double-counted; comparing rows_in to dups+unique cannot, since
    # dups is DERIVED from unique and the comparison is true by construction.
    grp=pd.to_numeric(uniq["merged_from_rows"],errors="coerce").fillna(0).astype(int).sum()
    if grp != total:
        raise SystemExit(f"ABORT: dedup lost rows — group sizes sum to {grp:,} "
                         f"but {total:,} rows were read. Nothing was written.")
    log(f"Integrity: group sizes sum to {grp:,} = rows in  (OK)")
    return total, uniq, stats

# ---------------------------------------------------------------- 4. overlap
def overlap(uniq):
    """Chunked, self-healing pool matching. One giant temp-table join proved
    fragile (connection died mid-query at 1.5M keys); instead probe the pool
    index in 50k-key ANY() chunks — each query is seconds — and reconnect+retry
    a chunk on network error."""
    secret=sh("aws","secretsmanager","get-secret-value","--secret-id","lead-pool/postgres",
              "--region","us-east-2","--query","SecretString","--output","text")
    d=json.loads(secret)
    import pg8000, time
    def fresh_conn():
        return pg8000.connect(host=d.get("host") or "lead-pool.c364acm8wlnv.us-east-2.rds.amazonaws.com",
            port=int(d.get("port",5432)), database=d.get("dbname") or "leadpool",
            user=d.get("username"), password=d.get("password"), ssl_context=True, timeout=180)
    state={"conn":fresh_conn()}
    def match(keys, tbl, col, label):
        if not keys: return set()
        out=set(); B=int(os.environ.get("DM_PROBE_CHUNK","200000"))  # raised 50k->200k
        for i in range(0,len(keys),B):
            chunk=keys[i:i+B]
            for attempt in range(4):
                try:
                    cur=state["conn"].cursor()
                    cur.execute(f"SELECT DISTINCT {col} FROM {tbl} WHERE {col} = ANY(%s)",(chunk,))
                    out.update(r[0] for r in cur.fetchall())
                    break
                except Exception as e:
                    log(f"    {label} chunk {i//B+1}: {type(e).__name__} (attempt {attempt+1}/4) — reconnecting")
                    try: state["conn"].close()
                    except Exception: pass
                    time.sleep(3*(attempt+1))
                    state["conn"]=fresh_conn()
            else:
                raise RuntimeError(f"pool match failed after retries ({label} chunk {i//B+1})")
            done=min(i+B,len(keys))
            if (i//B)%10==9 or done==len(keys):
                log(f"    {label}: probed {done:,}/{len(keys):,}  (matched so far {len(out):,})")
        return out
    allph=list({k for v in uniq["phone_e164"] for k in str(v).split(";") if k})
    allem=list({k for v in uniq["email_norm"] for k in str(v).split(";") if k})
    log(f"Matching {len(allph):,} phones / {len(allem):,} emails vs lead pool...")
    mph=match(allph,"lead_phones","phone_e164","phones")
    mem=match(allem,"lead_emails","email_norm","emails")
    try: state["conn"].close()
    except Exception: pass
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

# ---------------------------------------------------------------- store phase
def store(uniq, nn, ov, p_ov, src_dt, ts):
    """Load the three RDS tables + S3 export. Network-resilient: COPYs are
    chunked with reconnect inside copy_rows; apply_overlaps is one retryable
    transaction."""
    import time
    log("\nStoring to AWS...")
    connh={"conn":S.connect()}
    try:
        # One store at a time, ever: two concurrent stores for the same date
        # interleave their DELETE+COPY and double the tables (this happened on
        # 2026-07-28). The advisory lock is held by THIS session until it
        # disconnects; a second run aborts immediately instead of corrupting.
        cur=connh["conn"].cursor()
        cur.execute("SELECT pg_try_advisory_lock(hashtext('datamoon_store'))")
        if not cur.fetchone()[0]:
            raise SystemExit("Another store is already running against RDS "
                             "(advisory lock 'datamoon_store' is held). "
                             "Wait for it to finish and re-run.")
        S.load_datamoon_leads(connh, uniq, src_dt, ts, progress=log)
        S.load_datamoon_refined(connh, nn, src_dt, ts, progress=log)
        for attempt in range(3):
            try:
                S.apply_overlaps(connh, ov, src_dt, progress=log)
                break
            except Exception as e:
                log(f"  apply_overlaps: {type(e).__name__} (attempt {attempt+1}/3) — reconnecting")
                try: connh["conn"].close()
                except Exception: pass
                time.sleep(5); connh["conn"]=S.connect()
        else:
            raise RuntimeError("apply_overlaps failed after retries")
        S.upload_overlaps_s3(p_ov, src_dt, progress=log)
        cur=connh["conn"].cursor()
        cur.execute("SELECT count(*) FROM datamoon_refined"); tot_ref=cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM lead_overlaps"); tot_ent=cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM lead_overlaps WHERE distinct_days>1"); rep=cur.fetchone()[0]
    finally:
        try: connh["conn"].close()
        except Exception: pass
    return tot_ref, tot_ent, rep

def run_analysis(src_dt):
    """Step 6: overlapping analysis into recontacting_overlaps / upleads_list.
    Runs only after the store has fully succeeded; a failure here is reported
    but never undoes the day's stored data."""
    try:
        import overlap_analysis as A
        A.analyze(src_dt, progress=log)
    except Exception as e:
        # Loud, not advisory: this step failing means leads are UNTIERED and the
        # call lists are stale, which is not a complete cycle. The old wording
        # ("cycle data is already stored") read as harmless and hid 948k
        # untiered rows on 2026-07-30.
        log(f"\n{'!'*68}\n!! CYCLE INCOMPLETE — tiering/list update FAILED\n"
            f"!!   {type(e).__name__}: {e}\n"
            f"!! Raw/refined data for {src_dt} IS stored, but leads are NOT tiered\n"
            f"!! and t1_t2/t3_t4/recontacting_overlaps/upleads_list may be stale.\n"
            f"!! FIX: python tools/overlap_analysis.py {src_dt}\n{'!'*68}")
        raise SystemExit(f"cycle incomplete: tiering failed for {src_dt}")

def verify_tiers(src_dt, progress=log):
    """Prove the tiering actually landed. analyze() logs nothing for steps it
    never reaches, so a clean-looking log is not evidence -- only the table is."""
    conn=S.connect()
    try:
        cur=conn.cursor(); bad=[]
        for t in ("datamoon_leads","datamoon_refined"):
            cur.execute(f"select count(*), count(tier) from {t} where source_dt=%s",(src_dt,))
            n,tiered=cur.fetchone()
            progress(f"  {t:17} {src_dt}: {n:,} rows / {tiered:,} tiered")
            if n and tiered<n: bad.append(f"{t} {n-tiered:,} untiered")
        if bad: raise SystemExit("TIER VERIFY FAILED: "+"; ".join(bad))
        progress("  tier verify: OK")
    finally:
        try: conn.close()
        except Exception: pass

def store_only(src_dt):
    """Resume a cycle whose compute finished but whose store failed: reuse the
    local CSVs in DataMoon/cycle_<dt>/ instead of recomputing everything."""
    ts=dt.datetime.now(dt.timezone.utc)
    outdir=os.path.join(LOCAL_ROOT,f"cycle_{src_dt}")
    p_all=os.path.join(outdir,"combined_normalized_unique.csv")
    p_ov =os.path.join(outdir,"overlapping.csv")
    p_nn =os.path.join(outdir,"final_ready_leads.csv")
    for p in (p_all,p_ov,p_nn):
        if not os.path.exists(p): raise SystemExit(f"missing {p} — run the full cycle instead")
    log(f"store-only for {src_dt}: loading local CSVs...")
    uniq=pd.read_csv(p_all,dtype=str,keep_default_na=False)
    ov  =pd.read_csv(p_ov ,dtype=str,keep_default_na=False)
    nn  =pd.read_csv(p_nn ,dtype=str,keep_default_na=False)
    log(f"  unique={len(uniq):,}  overlap={len(ov):,}  net-new={len(nn):,}")
    tot_ref,tot_ent,rep = store(uniq, nn, ov, p_ov, src_dt, ts)
    run_analysis(src_dt)
    verify_tiers(src_dt)
    log(f"\nDONE. datamoon_refined total={tot_ref:,}; lead_overlaps {tot_ent:,} entities ({rep:,} repeat).")

# ---------------------------------------------------------------- main
def main(src_dt):
    ts=dt.datetime.now(dt.timezone.utc)
    log(f"\n{'='*64}\nDataMoon cycle  dt={src_dt}   started {ts:%Y-%m-%d %H:%M:%S} UTC\n{'='*64}")
    files=fetch_raw(src_dt)
    rows_in, uniq, stats = normalize_dedup(files)
    uniq = overlap(uniq)

    # Stamp the identifier ONCE on the full unique set, before splitting. Both
    # datamoon_leads and datamoon_refined then use the same row_id for the same
    # person; numbering them separately downstream made the id mean two
    # different leads in the two tables.
    uniq=uniq.reset_index(drop=True)
    uniq["row_id"]=[f"{src_dt}-{i:07d}" for i in range(len(uniq))]

    ov=uniq[uniq["in_lead_pool"]=="yes"].copy()
    nn=uniq[uniq["in_lead_pool"]=="no"].copy()

    outdir=os.path.join(LOCAL_ROOT,f"cycle_{src_dt}"); os.makedirs(outdir,exist_ok=True)
    p_all=os.path.join(outdir,"combined_normalized_unique.csv")
    p_ov =os.path.join(outdir,"overlapping.csv")
    p_nn =os.path.join(outdir,"final_ready_leads.csv")
    uniq.to_csv(p_all,index=False,quoting=csv.QUOTE_MINIMAL)
    ov.to_csv(p_ov,index=False,quoting=csv.QUOTE_MINIMAL)
    nn.drop(columns=["in_lead_pool","match_type","matched_pool_keys"]).to_csv(p_nn,index=False,quoting=csv.QUOTE_MINIMAL)

    tot_ref, tot_ent, rep = store(uniq, nn, ov, p_ov, src_dt, ts)
    run_analysis(src_dt)
    verify_tiers(src_dt)

    U=len(uniq); n_ov=len(ov); n_nn=len(nn)
    grp_sum=int(pd.to_numeric(uniq["merged_from_rows"],errors="coerce").fillna(0).sum())
    rep_txt=f"""DataMoon Cycle Report — dt={src_dt}
{'='*56}
Raw files processed:        {len(files)}
Rows in:                    {rows_in:,}
Internal duplicates:        {rows_in-U:,}
Unique normalized:          {U:,}

OVERLAP vs AWS lead pool (phone OR email; EIN not present in DataMoon)
  Already in pool:          {n_ov:,}  ({100*n_ov/U:.1f}%)
  NET-NEW refined:          {n_nn:,}  ({100*n_nn/U:.1f}%)

RECONCILE  {rows_in:,} = {rows_in-U:,} dup + {n_ov:,} overlap + {n_nn:,} net-new
  Merge groups sum to source rows:  {grp_sum:,} vs {rows_in:,}   -> {'OK' if grp_sum==rows_in else 'MISMATCH'}
  Overlap + net-new cover uniques:  {n_ov+n_nn:,} vs {U:,}       -> {'OK' if n_ov+n_nn==U else 'MISMATCH'}
  Unparseable lines skipped:        {stats['bad_lines']:,}

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
    args=[a for a in sys.argv[1:] if a!="--store-only"]
    d = args[0] if args else f"{dt.datetime.now():%Y-%m-%d}"
    if "--store-only" in sys.argv:
        store_only(d)
    else:
        main(d)

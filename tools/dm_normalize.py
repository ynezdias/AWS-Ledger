"""
DataMoon canonical schema + normalizers + transitive (union-find) dedup.
Shared by the cycle runner. Handles every layout seen so far:

  * 58-col DataMoon full export        (sha256_lc_hem ... last_updated)
  * 29-col DataMoon audience_export
  * UPPERCASE B2B layout               (EMAIL/PHONE/FIRST_NAME/...) — used by
    both the XLSX exports and the Google-Sheet drainer .jsonl.gz files
"""
import re, hashlib, gzip, json, os
import pandas as pd

# ---------------------------------------------------------------- schema
CANON = [
    "sha256_lc_hem","score_category","first_name","last_name",
    "personal_emails","business_email","additional_personal_emails",
    "personal_phone","mobile_phone","direct_number","company_phone",
    "personal_emails_validation_status","business_email_validation_status",
    "linkedin_url","personal_address","personal_address_2","personal_city",
    "personal_state","personal_zip","personal_zip4","contact_country",
    "gender","age_range","married","children","income_range","net_worth","homeowner",
    "job_title","seniority_level","department",
    "company_name","company_domain","company_sic","company_naics",
    "company_address","company_city","company_state","company_zip","company_country",
    "company_revenue","company_employee_count","primary_industry","last_updated",
]

# UPPERCASE layout (XLSX + drainer jsonl) -> canonical
UPPER_MAP = {
    "EMAIL":"personal_emails","BUSINESS_EMAIL":"business_email",
    "ADDITIONAL_EMAILS":"additional_personal_emails",
    "PHONE":"personal_phone","MOBILE_PHONE":"mobile_phone",
    "DIRECT_PHONE":"direct_number","COMPANY_PHONE":"company_phone",
    "FIRST_NAME":"first_name","LAST_NAME":"last_name",
    "ADDRESS":"personal_address","ADDRESS_2":"personal_address_2",
    "CITY":"personal_city","STATE":"personal_state","ZIP":"personal_zip",
    "COMPANY_NAME":"company_name","COMPANY_DOMAIN":"company_domain",
    "COMPANY_ADDRESS":"company_address","COMPANY_CITY":"company_city",
    "COMPANY_STATE":"company_state","COMPANY_ZIP":"company_zip",
    "COMPANY_REVENUE":"company_revenue","COMPANY_EMPLOYEES":"company_employee_count",
    "NAICS":"company_naics","SIC":"company_sic","INDUSTRY":"primary_industry",
    "JOB_TITLE":"job_title","DEPARTMENT":"department","SENIORITY":"seniority_level",
    "GENDER":"gender","AGE_RANGE":"age_range","MARRIED":"married","CHILDREN":"children",
    "INCOME_RANGE":"income_range","NET_WORTH":"net_worth","HOMEOWNER":"homeowner",
    "LINKEDIN_URL":"linkedin_url","SCORE_CATEGORY":"score_category",
}

# ---------------------------------------------------------------- field normalizers
def clean(s):
    if s is None: return ""
    s = str(s).strip()
    return "" if s.lower() in ("nan","none","null","n/a","na","") else s

_EMAIL_RE = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")

def norm_emails(*vals):
    out=set()
    for v in vals:
        v=clean(v)
        if not v: continue
        for part in re.split(r"[;,/|]+", v):
            p=part.strip().lower()
            if _EMAIL_RE.fullmatch(p): out.add(p)
    return out

def norm_phones(*vals):
    """US 10-digit -> E.164. Deliberately EXCLUDES business/company switchboard
    numbers at call sites: merging on those collapses distinct coworkers."""
    out=set()
    for v in vals:
        v=clean(v)
        if not v: continue
        for part in re.split(r"[;,/|]+", v):
            d=re.sub(r"\D","",part)
            if len(d)==11 and d.startswith("1"): d=d[1:]
            if len(d)==10 and d[0] in "23456789": out.add("+1"+d)
    return out

def norm_txt(s):
    return re.sub(r"[^a-z0-9]+"," ",clean(s).lower()).strip()

def nameaddr_key(fn,ln,street,zc):
    fn=norm_txt(fn); ln=norm_txt(ln); st=norm_txt(street)
    z=re.sub(r"\D","",clean(zc))[:5]
    if fn and ln and st and z:
        return hashlib.sha1(f"{fn}|{ln}|{st}|{z}".encode()).hexdigest()
    return ""

# ---------------------------------------------------------------- loading
def _to_canon(df):
    df.columns=[str(c).strip().strip('"') for c in df.columns]
    if not any(c in df.columns for c in ("sha256_lc_hem","personal_emails")):
        df=df.rename(columns={k:v for k,v in UPPER_MAP.items() if k in df.columns})
    for c in CANON:
        if c not in df.columns: df[c]=""
    return df[CANON]

KEY_SRC_COLS = ["personal_emails","business_email","additional_personal_emails",
                "personal_phone","mobile_phone"]

def key_yield(df):
    """Fraction of rows carrying at least one email or phone we could key on.

    A layout we don't have a mapping for produces all-empty canonical columns,
    which looks EXACTLY like a clean run: nothing dedups, nothing overlaps, and
    every row is reported as net-new. Callers check this and refuse to continue
    rather than publish a confident-looking empty result."""
    if len(df)==0: return 0.0
    have=None
    for c in KEY_SRC_COLS:
        if c not in df.columns: continue
        col=df[c].fillna("").astype(str).str.strip()!=""
        have=col if have is None else (have|col)
    return 0.0 if have is None else float(have.mean())

def load_any(path, chunksize=100000, stats=None):
    """Yield canonical DataFrames from a csv / csv.gz / xlsx / jsonl.gz file.

    `stats` (optional dict) accumulates rows silently dropped as unparseable, so
    the caller can report them instead of losing rows to a stderr warning."""
    import warnings
    if stats is None: stats={}
    stats.setdefault("bad_lines",0)

    def _bump(caught):
        for w in caught:
            if "Skipping line" in str(w.message): stats["bad_lines"]+=1

    low=path.lower()
    if low.endswith(".jsonl.gz") or low.endswith(".jsonl"):
        op = gzip.open if low.endswith(".gz") else open
        rows=[]
        with op(path,"rt",encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    stats["bad_lines"]+=1; continue
                if len(rows)>=chunksize:
                    yield _to_canon(pd.DataFrame(rows).astype(str)); rows=[]
        if rows: yield _to_canon(pd.DataFrame(rows).astype(str))
    elif low.endswith(".xlsx"):
        yield _to_canon(pd.read_excel(path, dtype=str, engine="openpyxl").fillna(""))
    else:
        reader=pd.read_csv(path, dtype=str, chunksize=chunksize,
                           keep_default_na=False, na_values=[],
                           engine="c", on_bad_lines="warn")
        while True:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                try: chunk=next(reader)
                except StopIteration: chunk=None
                _bump(caught)
            if chunk is None: break
            yield _to_canon(chunk)

# ---------------------------------------------------------------- dedup
def dedupe(df, progress=print):
    """Transitive union-find dedup on email / personal+mobile phone / name+addr.
    Returns the deduped winner frame (blanks back-filled from each group)."""
    df=df.fillna("")
    N=len(df)
    pe=df["personal_emails"].tolist(); be=df["business_email"].tolist()
    ae=df["additional_personal_emails"].tolist()
    pp=df["personal_phone"].tolist(); mp=df["mobile_phone"].tolist()
    fn=df["first_name"].tolist(); ln=df["last_name"].tolist()
    st=df["personal_address"].tolist(); zc=df["personal_zip"].tolist()

    ek=[None]*N; pk=[None]*N; na=[""]*N
    for i in range(N):
        ek[i]=norm_emails(pe[i],be[i],ae[i])
        pk[i]=norm_phones(pp[i],mp[i])
        na[i]=nameaddr_key(fn[i],ln[i],st[i],zc[i])
        if i and i%200000==0: progress(f"    keys {i:,}/{N:,}")

    parent=list(range(N))
    def find(x):
        r=x
        while parent[r]!=r: r=parent[r]
        while parent[x]!=r: parent[x],x=r,parent[x]
        return r
    def union(a,b):
        ra,rb=find(a),find(b)
        if ra!=rb: parent[max(ra,rb)]=min(ra,rb)
    seen={}
    for i in range(N):
        for k in ek[i]:
            j=seen.get(("e",k));  seen[("e",k)]=i if j is None else j
            if j is not None: union(i,j)
        for k in pk[i]:
            j=seen.get(("p",k));  seen[("p",k)]=i if j is None else j
            if j is not None: union(i,j)
        if na[i]:
            j=seen.get(("n",na[i])); seen[("n",na[i])]=i if j is None else j
            if j is not None: union(i,j)
        if i and i%200000==0: progress(f"    union {i:,}/{N:,}")
    del seen

    roots=[find(i) for i in range(N)]
    df["_root"]=roots
    df["_email_keys"]=[";".join(sorted(ek[i])) for i in range(N)]
    df["_phone_keys"]=[";".join(sorted(pk[i])) for i in range(N)]
    df["_na_key"]=na

    # Union every member's match keys onto the group. The winner row below is
    # picked by completeness, so without this the keys contributed ONLY by the
    # rows it absorbed are lost -- and those keys are exactly what the lead-pool
    # overlap step matches on. Losing one silently turns an existing lead into
    # a "net-new" one.
    root_em={}; root_ph={}
    for i in range(N):
        r=roots[i]
        if ek[i]: root_em.setdefault(r,set()).update(ek[i])
        if pk[i]: root_ph.setdefault(r,set()).update(pk[i])

    # Same treatment for provenance: one surviving lead can be the merge of rows
    # that arrived in several different raw files, so record ALL of them.
    root_src={}
    if "_source_file" in df.columns:
        srcs=df["_source_file"].tolist()
        for i in range(N):
            if srcs[i]: root_src.setdefault(roots[i],set()).add(srcs[i])
    df["_score"]=(df[CANON]!="").sum(axis=1).astype(int)
    df["_merged"]=df.groupby("_root")["_root"].transform("size")

    ds=df.sort_values(["_root","_score"],ascending=[True,False],kind="stable")
    bcols=CANON+["_email_keys","_phone_keys","_na_key"]
    winners=ds[["_root"]+bcols].replace("",pd.NA).groupby("_root",sort=False).first().reset_index()
    meta=ds.groupby("_root",sort=False).agg(merged_from_rows=("_merged","first")).reset_index()
    out=winners.merge(meta,on="_root",how="left").fillna("")
    # Overwrite the winner's own keys with the whole group's union (see above).
    out["_email_keys"]=[";".join(sorted(root_em.get(r,()))) for r in out["_root"]]
    out["_phone_keys"]=[";".join(sorted(root_ph.get(r,()))) for r in out["_root"]]
    if root_src:
        out["source_file"]=[";".join(sorted(root_src.get(r,()))) for r in out["_root"]]
    out=out.drop(columns=["_root"])
    out=out.rename(columns={"_email_keys":"email_norm","_phone_keys":"phone_e164","_na_key":"name_addr_key"})
    out["record_complete"]=((out["first_name"]!="")&(out["last_name"]!="")&
                            (out["personal_address"]!="")&
                            ((out["personal_emails"]!="")|(out["business_email"]!=""))
                            ).map({True:"yes",False:"no"})
    return out

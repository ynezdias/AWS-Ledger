# DataMoon Daily Cycle — Operating Procedure

You upload the raw data manually; one command does the rest in AWS.

## 1. Upload your raw files

Put every file for the day under the date prefix in S3:

```
s3://datamoon-raw-data/raw/dt=YYYY-MM-DD/
```

```powershell
aws s3 cp "C:\path\to\yourfile.csv" "s3://datamoon-raw-data/raw/dt=2026-07-28/" --region us-east-2
```

Drop in as many files as you like. Accepted: `.csv`, `.csv.gz`, `.xlsx`, `.jsonl.gz`.
Layouts handled automatically:

- 58-column DataMoon full export (`sha256_lc_hem …`)
- 29-column `audience_export`
- UPPERCASE B2B layout (`EMAIL` / `PHONE` / `FIRST_NAME` …) — used by the XLSX
  exports and by the Google-Sheet drainer's `.jsonl.gz` files

Anything the daily drainer already wrote for that date is picked up too — you do
not need to move it.

## 2. Run the cycle

```powershell
.\tools\run_cycle.ps1 2026-07-28      # a specific date
.\tools\run_cycle.ps1                 # today
```

(Or just tell Claude *"execute the pipeline for 2026-07-28"*.)

## 3. What it does

| Step | Action |
|------|--------|
| 1 | Downloads every raw file under `raw/dt=<DT>/` |
| 2 | Cleans + normalizes to the canonical schema (email, E.164 phone, name+address) |
| 3 | Transitive union-find dedup — winner keeps the most complete record, blanks back-filled from its duplicates, **no row dropped** |
| 4 | Overlap check against the AWS lead pool (`lead_emails.email_norm`, `lead_phones.phone_e164`) |
| 5 | Stores everything (below) and prints a reconciliation report |

## 4. Where the data lands

**RDS `datamoon` — exactly three tables**

| Table | Contents |
|-------|----------|
| `datamoon_leads` | all unique normalized rows for the day (the raw normalized data) |
| `datamoon_refined` | **the refined final list** — net-new leads, genuineness-scored |
| `lead_overlaps` | one row per overlapping **company/name**: `overlap_count`, `distinct_days`, `first_seen`, **`last_seen`**, `seen_dates` |

**S3 — overlap detail, one file per day**

```
s3://datamoon-raw-data/Overlapping Data/dt=YYYY-MM-DD/overlapping_YYYY-MM-DD.csv
```

**Local copies** — `DataMoon/cycle_<DT>/`
`combined_normalized_unique.csv`, `overlapping.csv`, `final_ready_leads.csv`, `CYCLE_REPORT.txt`

## 5. Safe to re-run

Every step is idempotent per date. Re-running `2026-07-28` replaces exactly that
date's contribution — including inside `lead_overlaps`, where each day's share is
tracked in `day_counts`, so repeat counts never double up. If a load fails
halfway, just run it again.

## Useful queries

```sql
-- the refined final list for a day
SELECT * FROM datamoon_refined WHERE source_dt = '2026-07-28';

-- clean (unflagged) refined leads only
SELECT * FROM datamoon_refined WHERE refined_status = 'clean';

-- companies that keep coming back, most recent first
SELECT display_name, overlap_count, distinct_days, first_seen, last_seen
FROM lead_overlaps
WHERE distinct_days > 1
ORDER BY last_seen DESC, overlap_count DESC;
```

## Notes

- **EIN matching is not possible** — no DataMoon export carries an EIN column.
  The lead pool has `leads.ein`, but there is nothing on the DataMoon side to
  join it to. Overlap is by phone **or** email.
- Dedup deliberately ignores `company_phone` / `direct_number`: merging on a
  shared company switchboard number collapses distinct coworkers into one lead.
- Needs a valid AWS session (`aws login`) — the script reads the RDS credentials
  from Secrets Manager (`datamoon/postgres`, `lead-pool/postgres`).

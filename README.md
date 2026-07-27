# AWS Ledger — DataMoon Lead Pipeline

A daily, **no-data-loss** pipeline that turns raw DataMoon leads (landing in Google
Sheets) into a clean, deduplicated, analyzed lead list in AWS — and flags which leads
overlap with data you already own.

> **New to AWS?** Read [`docs/NEXT-STEPS.md`](docs/NEXT-STEPS.md) first. It explains every
> AWS service used here in plain English and walks you through deploying step by step.
> **Nothing in this repo touches your real AWS account until you deliberately run the
> deploy commands.** Right now it's just files on your computer.

---

## What this does (the big picture)

```
DataMoon website
      │  (you already sync this)
      ▼
Google Sheet  ──────────►  a small "inbox". Data piles in, we empty it constantly.
      │
      │  every few minutes, an AWS Lambda "drains" the sheet
      ▼
Amazon S3  ────────────►  cheap, permanent storage. The RAW copy lives here forever.
      │                    (this is our safety net — we can rebuild everything from it)
      ▼
AWS Glue  ─────────────►  cleans + deduplicates the data (the "ETL" step)
      │
      ▼
AWS RDS (PostgreSQL) ──►  a real database — the official "source of truth"
      │
      ▼
Analysis  ─────────────►  compares new leads against data we already own, and scores
                          the remaining leads for "genuineness"
```

Every incoming lead ends up in exactly one of two buckets:

- **Overlap** — we already had this lead (matches our history or lead pool).
- **Refined** — a genuinely new lead worth pursuing.

...and we **never delete the raw data**, so nothing is ever truly lost.

---

## Repository layout

```
AWS Ledger/
├── README.md                  ← you are here
├── docs/
│   └── NEXT-STEPS.md          ← beginner deploy guide (READ THIS)
├── sql/
│   └── schema.sql             ← all database tables (the data model)
├── src/
│   ├── lambdas/
│   │   └── sheet_drainer/     ← the "drain the Google Sheet" function
│   │       ├── handler.py
│   │       └── requirements.txt
│   └── glue/                  ← the big data-cleaning + analysis jobs
│       ├── dedup_normalize.py
│       ├── overlap_analysis.py
│       └── genuineness.py
└── infra/                     ← Terraform: defines all the AWS resources as code
    ├── versions.tf
    ├── variables.tf
    ├── providers.tf
    ├── s3.tf
    ├── secrets.tf
    ├── iam.tf
    ├── lambda.tf
    ├── eventbridge.tf
    ├── outputs.tf
    └── terraform.tfvars.example
```

The **ingestion + storage foundation** (S3, the drain Lambda, its schedule, secrets, and
the database schema) is built out first because it proves the riskiest promise —
*no data loss*. The heavier analysis pieces (Glue jobs, RDS instance, Step Functions
orchestration) are scaffolded as clearly-marked next steps so you can grow into them.

---

## Built to match YOUR existing process

The sample data in `DataMoon/` and its `quality_report.txt` files show you already run a
documented, reconciled process. This pipeline reproduces it exactly:

| Your quality_report step | Where it lives here |
|---|---|
| STEP 1 — normalize + internal dedup, **no rows dropped** | `src/glue/dedup_normalize.py` |
| STEP 2 — overlap: **email OR phone; fallback name+street** | `src/glue/overlap_analysis.py` |
| STEP 3 — final unique leads (incomplete kept + flagged) | `refined_leads` table |
| RECONCILE — `rows_in = internal_dup + overlap + final` | enforced in code; fails loudly if it doesn't balance |

**Matching uses three independent keys (an OR match):**
- `email_key` — DataMoon's `sha256_lc_hem` (a hashed email; privacy-safe), else a hash of the email
- `phone_key` — last-10 digits of `personal_phone` (fallback `mobile_phone`)
- `nameaddr_key` — hash of name + street + zip (fallback only)

**Two DataMoon export layouts are handled** (the 58-column full export and the
29-column `audience_export`) and mapped to one canonical schema. DataMoon's own
`score_category` and email-validation status feed the genuineness stage.

**Volume note:** the "5M" figure is your **lead-pool size** (~4M emails / 5.3M phones
indexed), not daily inflow — daily DataMoon batches are ~1K–200K rows, so Google Sheets
as a rolling buffer is comfortable.

---

## Build status

| Piece | Status |
|---|---|
| Database schema (SQL) | ✅ Written — ready to run against a Postgres DB |
| Sheet-drainer Lambda (no-data-loss drain) | ✅ Written — ready to test locally |
| Terraform: S3 + secrets + IAM + Lambda + schedule | ✅ Scaffolded |
| Glue ETL jobs (clean / dedup / overlap / genuineness) | 🟡 Stubbed — logic outlined, to be filled in |
| Terraform: VPC + RDS + Glue + Step Functions | 🟡 Next phase (see NEXT-STEPS.md) |
| Third-party enrichment | ⬜ Optional, hook left in place |

---

## Quick start (local, no AWS needed yet)

You can inspect and test the data-model and the drain logic without any AWS account:

```bash
# 1. Look at the data model
cat sql/schema.sql

# 2. Set up a local Python env for the Lambda
cd src/lambdas/sheet_drainer
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

When you're ready to actually deploy to AWS, follow
[`docs/NEXT-STEPS.md`](docs/NEXT-STEPS.md).

---

## Key design guarantees

1. **No data loss.** The drain Lambda only deletes rows from the Google Sheet *after* it
   has confirmed they are safely written to S3 (read → write → **verify** → delete). The
   S3 raw zone is immutable and kept forever.
2. **Sheet stays small.** By draining every few minutes, the Google Sheet never
   approaches its ~10-million-cell limit, no matter how much daily volume flows through.
3. **Nothing overwritten.** Overlap matches are *appended* to a dedicated table; original
   records are never modified.
```

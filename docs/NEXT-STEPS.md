# Next Steps — Deploying the pipeline (beginner-friendly)

This guide assumes you have **never used AWS from the command line before**. Take it
one section at a time. Nothing here charges your account until you reach the
`terraform apply` step, and even then the foundation pieces cost only a few dollars a
month.

> **Golden rule:** never paste passwords, keys, or the Google JSON file directly into
> any file in this repo. They go into AWS Secrets Manager (covered below). The
> `.gitignore` is set up to help prevent accidents.

---

## The AWS services we use, in one line each

| Service | What it is (plain English) |
|---|---|
| **S3** | An infinite, cheap hard drive in the cloud. Stores every raw + cleaned file. |
| **Lambda** | A small function that runs on a schedule without a server. Drains the sheet. |
| **EventBridge** | A cloud alarm clock. Tells the Lambda "run now" every few minutes. |
| **Secrets Manager** | A locked vault for passwords and keys. |
| **RDS (PostgreSQL)** | A managed database — the official source of truth. *(next phase)* |
| **Glue** | Runs the big data-cleaning + analysis jobs across many machines. *(next phase)* |
| **Step Functions** | A flowchart that runs the daily jobs in order. *(next phase)* |
| **Terraform** | The tool that creates all of the above from the text files in `infra/`. |

---

## Phase 0 — One-time setup on your computer

1. **Create an AWS account** at https://aws.amazon.com if you don't have one.
2. **Install the tools** (Windows):
   - AWS CLI: https://aws.amazon.com/cli/
   - Terraform: https://developer.hashicorp.com/terraform/install
   - Python 3.12: https://www.python.org/downloads/
3. **Connect the AWS CLI to your account:**
   ```powershell
   aws configure
   ```
   Paste the Access Key + Secret when prompted (create these under AWS Console →
   IAM → Users → Security credentials). Set region to `us-east-1`.
4. **Confirm it works:**
   ```powershell
   aws sts get-caller-identity
   ```
   You should see your account number.

---

## Phase 1 — Deploy the ingestion foundation (S3 + drain Lambda)

### 1a. Give the drain Lambda its Google libraries
The Lambda needs the Google client libraries bundled with it. Install them into the
function folder before zipping:
```powershell
cd "src\lambdas\sheet_drainer"
pip install -r requirements.txt -t .
cd ..\..\..
```

### 1b. Set your configuration
```powershell
cd infra
copy terraform.tfvars.example terraform.tfvars
```
Open `terraform.tfvars` and fill in:
- `data_bucket_name` — any unique lowercase name, e.g. `aws-ledger-datamoon-7x9k2`
- `google_sheet_id` — the long ID from your sheet's URL

### 1c. Create the AWS resources
```powershell
terraform init      # downloads the AWS plugin (first time only)
terraform plan      # shows what WILL be created — read it, nothing happens yet
terraform apply     # type "yes" to actually create everything
```
When it finishes, Terraform prints the bucket name and Lambda name.

### 1d. Give Google access (into the vault, not the repo)
1. In Google Cloud Console, create a **service account**, enable the **Google Sheets
   API**, and download its **JSON key**.
2. **Share your Google Sheet** with the service account's email (Editor access) — this
   is what lets the Lambda clear rows.
3. Put the JSON into Secrets Manager (replace the path with your downloaded file):
   ```powershell
   aws secretsmanager put-secret-value `
     --secret-id datamoon/google-service-account `
     --secret-string file://C:\path\to\service-account.json
   ```

### 1e. Test it
- Put a few test rows in the sheet.
- Manually run the Lambda once:
  ```powershell
  aws lambda invoke --function-name aws-ledger-sheet-drainer out.json
  cat out.json
  ```
- Check the S3 bucket → `raw/dt=.../` for a `.jsonl.gz` file, and confirm the test
  rows disappeared from the sheet. **That proves the no-data-loss drain works.**
- From now on EventBridge runs it automatically every 3 minutes.

---

## Phase 2 — Database (RDS PostgreSQL)

Not yet coded in Terraform (kept separate so Phase 1 stays simple). When ready, we add
`infra/vpc.tf` + `infra/rds.tf`, then:
```powershell
# after RDS exists, create the tables:
psql "postgresql://USER:PASSWORD@YOUR-RDS-ENDPOINT:5432/DBNAME" -f ..\sql\schema.sql
```
Store the DB password in the `datamoon/rds-credentials` secret (already created).

---

## Phase 3 — Cleaning + analysis (Glue jobs)

The logic already lives in `src/glue/`:
- `dedup_normalize.py` — clean + dedupe raw → curated + load RDS
- `overlap_analysis.py` — split into overlap vs refined (with a reconcile safety check)
- `genuineness.py` — score refined leads (rule-based; enrichment optional)

Deploying these means: upload each script to S3, create a Glue job pointing at it, and
add a Glue Connection to RDS. We'll add `infra/glue.tf` to automate that.

---

## Phase 4 — Orchestration (Step Functions)

A daily state machine runs: **Glue clean → load RDS → overlap → genuineness**, with
retries and alerts. Added as `infra/stepfunctions.tf`. The drain Lambda keeps running
independently every few minutes.

---

## Tearing it all down

To remove everything Terraform created (and stop any charges):
```powershell
cd infra
terraform destroy
```
Your raw data in S3 is versioned; empty the bucket first if `destroy` complains.

---

## Where to get help
- Re-read `README.md` for the big picture.
- The architecture rationale and decisions live in the approved plan:
  `C:\Users\ydias\.claude\plans\project-data-architecture-will-cozy-sunbeam.md`

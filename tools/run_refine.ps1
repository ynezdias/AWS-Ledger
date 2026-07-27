# STEP 4 runner — fetch datamoon DB creds from Secrets Manager, run refinement.
# Usage:  .\tools\run_refine.ps1 [source_dt]   (default 2026-07-24)
param([string]$SourceDt = "2026-07-24")

$ErrorActionPreference = "Stop"
$env:DM_SECRET = aws secretsmanager get-secret-value `
    --secret-id datamoon/postgres --region us-east-2 `
    --query SecretString --output text

python "$PSScriptRoot\refine_leads.py" $SourceDt

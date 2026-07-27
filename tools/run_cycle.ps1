# DataMoon daily cycle — fetches secrets, then runs the full pipeline.
#   .\tools\run_cycle.ps1              -> today
#   .\tools\run_cycle.ps1 2026-07-28   -> a specific date
param([string]$SourceDt = "")

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

Write-Host "Fetching RDS credentials from Secrets Manager..."
$env:DM_SECRET = aws secretsmanager get-secret-value --secret-id datamoon/postgres `
    --region us-east-2 --query SecretString --output text
if (-not $env:DM_SECRET) { throw "Could not read secret datamoon/postgres (is your AWS session valid? run 'aws login')" }

Push-Location $root
try {
    if ($SourceDt) { python tools/run_cycle.py $SourceDt }
    else           { python tools/run_cycle.py }
}
finally {
    Pop-Location
    $env:DM_SECRET = $null
}

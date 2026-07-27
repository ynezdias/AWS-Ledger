<#
Repackages src/lambdas/sheet_drainer and updates the deployed Lambda.

Run this after ANY edit to handler.py:
    powershell -File tools\deploy_sheet_drainer.ps1

The Lambda, IAM roles, Secrets Manager entry and EventBridge schedule already
exist (created 2026-07-23) — this script only refreshes the function code.
Dependencies are installed for Linux/x86_64 because that is what Lambda runs,
not for this Windows machine.
#>

$ErrorActionPreference = "Stop"

$Region       = "us-east-2"
$FunctionName = "datamoon-sheet-drainer"
$SrcDir       = Join-Path $PSScriptRoot "..\src\lambdas\sheet_drainer"
$BuildDir     = Join-Path $env:TEMP "sheet_drainer_build"
$Zip          = Join-Path $env:TEMP "sheet_drainer.zip"

Write-Host "==> Installing dependencies for Linux/x86_64..."
if (Test-Path $BuildDir) { Remove-Item -LiteralPath $BuildDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
python -m pip install --quiet --target $BuildDir `
    --platform manylinux2014_x86_64 --python-version 3.12 `
    --implementation cp --only-binary=:all: `
    google-api-python-client google-auth

# Trim things Lambda never needs, to stay under the 50 MB direct-upload limit.
Get-ChildItem $BuildDir -Recurse -Directory -Include __pycache__ |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem $BuildDir -Directory -Filter "*.dist-info" |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem $BuildDir -Directory -Filter "bin" |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

Copy-Item (Join-Path $SrcDir "handler.py") $BuildDir

Write-Host "==> Zipping..."
if (Test-Path $Zip) { Remove-Item -LiteralPath $Zip -Force }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory(
    $BuildDir, $Zip, [System.IO.Compression.CompressionLevel]::Optimal, $false)
Write-Host ("    {0:N1} MB" -f ((Get-Item $Zip).Length / 1MB))

Write-Host "==> Updating $FunctionName..."
aws lambda update-function-code `
    --function-name $FunctionName `
    --zip-file "fileb://$Zip" `
    --region $Region `
    --query LastUpdateStatus --output text
aws lambda wait function-updated --function-name $FunctionName --region $Region

Write-Host "==> Done. Test with:"
Write-Host "    aws lambda invoke --function-name $FunctionName --region $Region out.json"

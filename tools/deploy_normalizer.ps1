<#
Repackages src/lambdas/normalizer and updates the deployed Lambda.

Run this after ANY edit to handler.py:
    powershell -File tools\deploy_normalizer.ps1

The normalizer has no third-party dependencies (stdlib + the boto3 that ships
with the Lambda runtime), so this is just a zip-and-upload.

Re-run a specific day by hand:
    aws lambda invoke --function-name datamoon-normalizer --region us-east-2 `
      --cli-binary-format raw-in-base64-out `
      --payload '{\"source_dt\":\"2026-07-24\"}' out.json
It only reads raw/ and only writes "Normalized DataMoon/", so re-running is safe.
#>

$ErrorActionPreference = "Stop"

$Region       = "us-east-2"
$FunctionName = "datamoon-normalizer"
$SrcDir       = Join-Path $PSScriptRoot "..\src\lambdas\normalizer"
$BuildDir     = Join-Path $env:TEMP "normalizer_build"
$Zip          = Join-Path $env:TEMP "normalizer.zip"

if (Test-Path $BuildDir) { Remove-Item -LiteralPath $BuildDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
Copy-Item (Join-Path $SrcDir "handler.py") $BuildDir

Write-Host "==> Zipping..."
if (Test-Path $Zip) { Remove-Item -LiteralPath $Zip -Force }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory(
    $BuildDir, $Zip, [System.IO.Compression.CompressionLevel]::Optimal, $false)

Write-Host "==> Updating $FunctionName..."
aws lambda update-function-code `
    --function-name $FunctionName `
    --zip-file "fileb://$Zip" `
    --region $Region `
    --query LastUpdateStatus --output text
aws lambda wait function-updated --function-name $FunctionName --region $Region

Write-Host "==> Done."

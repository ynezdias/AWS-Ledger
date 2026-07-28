# Redeploys the enrich Lambda CODE only (resources are already created).
# Deps must be installed for linux x86_64 (this box is Windows ARM64).
$ErrorActionPreference = "Stop"
$build = "$env:TEMP\eb"   # short path: deep anthropic paths break MAX_PATH under long roots

if (Test-Path $build) { Remove-Item -Recurse -Force $build }
New-Item -ItemType Directory -Force $build | Out-Null
python -m pip install --platform manylinux2014_x86_64 --only-binary=:all: `
    --python-version 3.12 --implementation cp -t $build anthropic pg8000 --quiet
Copy-Item "$PSScriptRoot\handler.py" $build
python -c "import pathlib, shutil; root = pathlib.Path(r'$env:TEMP\eb'); [shutil.rmtree(p) for p in root.rglob('__pycache__')]; shutil.make_archive(r'$env:TEMP\enrich_temp', 'zip', root)"

aws lambda update-function-code --function-name datamoon-enrich-temp `
    --zip-file fileb://$env:TEMP\enrich_temp.zip --region us-east-2 `
    --query "[FunctionName,LastUpdateStatus]" --output text

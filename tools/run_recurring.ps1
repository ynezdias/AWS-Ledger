# Pulls both DB passwords from Secrets Manager and runs the recurring-leads analysis.
# boto3 cannot load credentials on this box, so secrets come via the AWS CLI.

$ErrorActionPreference = "Stop"

$dm = aws secretsmanager get-secret-value --secret-id datamoon/postgres `
      --region us-east-2 --query SecretString --output text | ConvertFrom-Json
$lp = aws secretsmanager get-secret-value --secret-id lead-pool/postgres `
      --region us-east-2 --query SecretString --output text | ConvertFrom-Json

$env:DM_PW = $dm.password
$env:LP_PW = $lp.password

python "$PSScriptRoot\recurring_worked_leads.py"

Remove-Item Env:\DM_PW, Env:\LP_PW

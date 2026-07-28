# Removes ALL temporary Step-6 enrichment resources. RDS is never touched.
# Enrichment JSON already exported to s3://datamoon-raw-data/Enrichment/ is KEPT
# (delete that prefix manually if you also want it gone).
$ErrorActionPreference = "Continue"
$R = "us-east-2"

Write-Host "Deleting SQS event source mapping..."
$uuid = aws lambda list-event-source-mappings --function-name datamoon-enrich-temp --region $R --query "EventSourceMappings[0].UUID" --output text
if ($uuid -and $uuid -ne "None") { aws lambda delete-event-source-mapping --uuid $uuid --region $R | Out-Null }

Write-Host "Deleting Lambda..."
aws lambda delete-function --function-name datamoon-enrich-temp --region $R

Write-Host "Deleting SQS queues..."
aws sqs delete-queue --queue-url https://sqs.us-east-2.amazonaws.com/870730509769/datamoon-enrich-temp-queue --region $R
aws sqs delete-queue --queue-url https://sqs.us-east-2.amazonaws.com/870730509769/datamoon-enrich-temp-dlq --region $R

Write-Host "Deleting DynamoDB cache table..."
aws dynamodb delete-table --table-name datamoon-enrich-cache-temp --region $R | Out-Null

Write-Host "Deleting IAM role..."
aws iam delete-role-policy --role-name datamoon-enrich-temp-role --policy-name datamoon-enrich-temp-policy
aws iam delete-role --role-name datamoon-enrich-temp-role

Write-Host "Deleting CloudWatch log group..."
aws logs delete-log-group --log-group-name /aws/lambda/datamoon-enrich-temp --region $R

Write-Host "Done. Temporary enrichment stack removed."

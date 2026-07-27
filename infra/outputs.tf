# Handy values printed after `terraform apply` so you know what got created.

output "data_bucket" {
  description = "Name of the S3 data-lake bucket."
  value       = aws_s3_bucket.data_lake.id
}

output "drain_lambda_name" {
  description = "Name of the sheet-drainer Lambda function."
  value       = aws_lambda_function.sheet_drainer.function_name
}

output "google_secret_name" {
  description = "Secrets Manager entry to paste your Google service-account JSON into."
  value       = aws_secretsmanager_secret.google_service_account.name
}

output "rds_secret_name" {
  description = "Secrets Manager entry for future RDS credentials."
  value       = aws_secretsmanager_secret.rds_credentials.name
}

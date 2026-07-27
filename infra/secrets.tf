# =============================================================================
# Secrets Manager — safe home for credentials (never put these in code/git).
# =============================================================================

# A slot to hold the Google service-account JSON that lets the drain Lambda
# read + clear the sheet. Terraform creates the empty secret; you paste the
# actual JSON in AFTER (see NEXT-STEPS.md) so it never lives in this repo.
resource "aws_secretsmanager_secret" "google_service_account" {
  name        = "datamoon/google-service-account"
  description = "Google service-account JSON used by the sheet_drainer Lambda."
}

# A slot for the RDS database connection string / password, used later by the
# Glue jobs. Populated when you stand up RDS.
resource "aws_secretsmanager_secret" "rds_credentials" {
  name        = "datamoon/rds-credentials"
  description = "PostgreSQL connection credentials for the pipeline."
}

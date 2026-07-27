# Inputs you can tweak without editing the main code. Provide real values in a
# file called terraform.tfvars (copy terraform.tfvars.example to start).

variable "aws_region" {
  description = "AWS region to deploy into (e.g. us-east-2)."
  type        = string
  default     = "us-east-2"
}

variable "environment" {
  description = "Environment name used in tags/resource names (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "project_prefix" {
  description = "Short prefix for resource names. Keep it lowercase, no spaces."
  type        = string
  default     = "aws-ledger"
}

variable "data_bucket_name" {
  description = <<-EOT
    Globally-unique S3 bucket name for the data lake. S3 bucket names must be
    unique across ALL of AWS, so add a random suffix, e.g.
    "aws-ledger-datamoon-8f3k2".
  EOT
  type        = string
}

variable "google_sheet_id" {
  description = "The Google Sheet ID that DataMoon syncs into (from its URL)."
  type        = string
}

variable "google_sheet_range" {
  description = "Tab/range to drain, e.g. Sheet1."
  type        = string
  default     = "Sheet1"
}

variable "drain_batch_rows" {
  description = "Max rows the drain Lambda pulls per run."
  type        = number
  default     = 20000
}

variable "drain_schedule" {
  description = "How often to drain the sheet (EventBridge rate/cron expression)."
  type        = string
  default     = "rate(3 minutes)"
}

variable "raw_retention_days_to_glacier" {
  description = "Days before raw files transition to cheap Glacier storage."
  type        = number
  default     = 90
}

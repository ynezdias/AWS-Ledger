# =============================================================================
# The sheet_drainer Lambda function itself.
# =============================================================================

# Zip up the Lambda source code so AWS can run it. NOTE: the Google client
# libraries in requirements.txt must be installed into the source folder (or a
# Lambda Layer) before deploy — see NEXT-STEPS.md for the one-line build command.
data "archive_file" "sheet_drainer" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambdas/sheet_drainer"
  output_path = "${path.module}/build/sheet_drainer.zip"
}

resource "aws_lambda_function" "sheet_drainer" {
  function_name    = "${var.project_prefix}-sheet-drainer"
  role             = aws_iam_role.sheet_drainer.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.sheet_drainer.output_path
  source_code_hash = data.archive_file.sheet_drainer.output_base64sha256
  timeout          = 120 # seconds — plenty for a batched read/write/delete
  memory_size      = 512

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.data_lake.id
      S3_RAW_PREFIX      = "raw"
      SHEET_ID           = var.google_sheet_id
      SHEET_RANGE        = var.google_sheet_range
      GOOGLE_SECRET_NAME = aws_secretsmanager_secret.google_service_account.name
      BATCH_ROWS         = tostring(var.drain_batch_rows)
    }
  }
}

# Keep Lambda logs for 30 days (avoids unbounded log storage cost).
resource "aws_cloudwatch_log_group" "sheet_drainer" {
  name              = "/aws/lambda/${aws_lambda_function.sheet_drainer.function_name}"
  retention_in_days = 30
}

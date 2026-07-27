# =============================================================================
# IAM — the permissions the drain Lambda is allowed to use (least privilege).
# A "role" is an identity the Lambda assumes; "policies" list what it may do.
# =============================================================================

# Let the Lambda service assume this role.
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sheet_drainer" {
  name               = "${var.project_prefix}-sheet-drainer"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Exactly the permissions the drain Lambda needs — no more:
#   * write + read-back objects in OUR bucket's raw/ prefix (verify step)
#   * read the Google service-account secret
#   * write its own logs
data "aws_iam_policy_document" "sheet_drainer" {
  statement {
    sid     = "WriteAndVerifyRawObjects"
    actions = ["s3:PutObject", "s3:GetObject", "s3:GetObjectAttributes"]
    resources = ["${aws_s3_bucket.data_lake.arn}/raw/*"]
  }

  statement {
    sid       = "ReadGoogleSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.google_service_account.arn]
  }

  statement {
    sid     = "WriteLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_role_policy" "sheet_drainer" {
  name   = "${var.project_prefix}-sheet-drainer-policy"
  role   = aws_iam_role.sheet_drainer.id
  policy = data.aws_iam_policy_document.sheet_drainer.json
}

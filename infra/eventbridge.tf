# =============================================================================
# EventBridge schedule — runs the drain Lambda every few minutes so the Google
# Sheet is emptied continuously and never fills up.
# =============================================================================

resource "aws_cloudwatch_event_rule" "drain_schedule" {
  name                = "${var.project_prefix}-drain-schedule"
  description         = "Trigger the sheet_drainer Lambda on a fixed interval."
  schedule_expression = var.drain_schedule
}

resource "aws_cloudwatch_event_target" "drain_lambda" {
  rule = aws_cloudwatch_event_rule.drain_schedule.name
  arn  = aws_lambda_function.sheet_drainer.arn
}

# Allow EventBridge to invoke the Lambda.
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sheet_drainer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.drain_schedule.arn
}

# --- Basic safety alarm: warn if the drain Lambda starts erroring -----------
resource "aws_cloudwatch_metric_alarm" "drain_errors" {
  alarm_name          = "${var.project_prefix}-drain-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "The sheet_drainer Lambda reported errors — investigate."
  dimensions = {
    FunctionName = aws_lambda_function.sheet_drainer.function_name
  }
}

# or ABOUTME: CloudWatch Alarm configurations for SQS Ingestion metrics and Step Functions Pipeline failures

# ==========================================
# 1. SQS DLQ MESSAGE COUNT ALARM
# ==========================================
resource "aws_cloudwatch_metric_alarm" "sqs_dlq_message_count" {
  alarm_name          = "${local.project_prefix}-${var.environment}-sqs-dlq-count"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "This alarm triggers when any messages land in the raw ingestion Dead Letter Queue (DLQ)"
  alarm_actions       = [local.sns_topic_arn]
  ok_actions          = [local.sns_topic_arn]

  dimensions = {
    QueueName = local.dlq_queue_name
  }

  tags = local.tags
}

# ==========================================
# 2. SQS LATENCY / STALLING ALARM (Age of Oldest Message > 45m)
# ==========================================
resource "aws_cloudwatch_metric_alarm" "sqs_latency_stalling" {
  alarm_name          = "${local.project_prefix}-${var.environment}-sqs-queue-stalling"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 2700 # 45 minutes in seconds
  alarm_description   = "This alarm triggers when the oldest message in the raw ingest queue exceeds 45 minutes, indicating consumer lag"
  alarm_actions       = [local.sns_topic_arn]
  ok_actions          = [local.sns_topic_arn]

  dimensions = {
    QueueName = local.queue_name
  }

  tags = local.tags
}

# ==========================================
# 3. STEP FUNCTIONS FAILURE ALARM
# ==========================================
resource "aws_cloudwatch_metric_alarm" "sfn_execution_failure" {
  alarm_name          = "${local.project_prefix}-${var.environment}-sfn-execution-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "This alarm triggers when any Step Functions pipeline execution fails"
  alarm_actions       = [local.sns_topic_arn]
  ok_actions          = [local.sns_topic_arn]

  dimensions = {
    StateMachineArn = local.state_machine_arn
  }

  tags = local.tags
}

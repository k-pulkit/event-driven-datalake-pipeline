# ABOUTME: Provision Step Functions State Machine and EventBridge scheduled cron trigger for data processing

# ==========================================
# 1. STEP FUNCTIONS STATE MACHINE
# ==========================================
resource "aws_sfn_state_machine" "transit_pipeline_orchestrator" {
  name     = "${local.project_prefix}-${var.environment}-orchestrator"
  role_arn = local.sfn_execution_role_arn

  definition = templatefile("${path.module}/../../../../../stepFunctions/transit_pipeline_sfn.asl", {
    aws_region           = var.aws_region
    account_id           = data.aws_caller_identity.current.account_id
    project_prefix       = local.project_prefix
    environment          = var.environment
    sqs_queue_url        = local.sqs_queue_url
    landing_bucket_id    = local.landing_bucket_id
    glue_silver_job_name = local.glue_silver_job_name
    glue_gold_job_name   = local.glue_gold_job_name
    sns_topic_arn        = aws_sns_topic.pipeline_alerts.arn
  })

  tags = local.tags
}

# ==========================================
# 2. EVENTBRIDGE SCHEDULER (Cron every 5 mins)
# ==========================================
resource "aws_scheduler_schedule" "transit_pipeline_schedule" {
  name        = "${local.project_prefix}-${var.environment}-schedule"
  group_name  = "default"

  flexible_time_window {
    mode = "OFF"
  }

  state               = "DISABLED"
  schedule_expression = "rate(5 minutes)"

  target {
    arn      = aws_sfn_state_machine.transit_pipeline_orchestrator.arn
    role_arn = local.eventbridge_sfn_target_role_arn
  }
}

# ==========================================
# 3. SNS TOPIC FOR PIPELINE ALERTS
# ==========================================
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.project_prefix}-${var.environment}-alerts"
  tags = local.tags
}

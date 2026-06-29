# ABOUTME: Local variables for remote state lookups, project prefixing, and tagging in the Pipeline layer
locals {
  transit_project_id = "transit-pipeline"

  transit_project_name = lookup(var.project_name_mapping, local.transit_project_id, null)
  project_prefix       = "${var.namespace}-${local.transit_project_name}"
  tags = {
    Path = "terraform/environments/${var.environment}/${var.aws_region}/pipeline/"
  }

  # Outputs from other states
  kms_key_id              = data.terraform_remote_state.security.outputs.kms_key_id
  glue_job_role_arn       = data.terraform_remote_state.security.outputs.glue_job_role_arn
  sfn_execution_role_arn          = data.terraform_remote_state.security.outputs.sfn_execution_role_arn
  eventbridge_sfn_target_role_arn = data.terraform_remote_state.security.outputs.eventbridge_sfn_target_role_arn

  sqs_queue_arn     = data.terraform_remote_state.storage.outputs.queue_arn
  sqs_queue_url     = data.terraform_remote_state.storage.outputs.queue_url
  landing_bucket_id = data.terraform_remote_state.storage.outputs.raw_bucket_id

  # Glue jobs names (placeholders)
  glue_silver_job_name = "${local.project_prefix}-${var.environment}-silver-processing"
  glue_gold_job_name   = "${local.project_prefix}-${var.environment}-gold-aggregation"
}

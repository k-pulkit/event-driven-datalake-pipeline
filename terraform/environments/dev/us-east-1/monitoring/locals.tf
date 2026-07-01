# or ABOUTME: Local variables for remote state mappings and resource tags in the Monitoring layer

locals {
  transit_project_id = "transit-pipeline"
  transit_project_name = lookup(var.project_name_mapping, local.transit_project_id, null)
  project_prefix       = "${var.namespace}-${local.transit_project_name}"

  tags = {
    Path = "terraform/environments/${var.environment}/${var.aws_region}/monitoring/"
  }

  # Outputs from other states
  sns_topic_arn      = data.terraform_remote_state.pipeline.outputs.sns_topic_arn
  state_machine_arn  = data.terraform_remote_state.pipeline.outputs.state_machine_arn

  # SQS Queues names extracted from outputs
  queue_name     = data.terraform_remote_state.storage.outputs.queue_name
  dlq_queue_name = data.terraform_remote_state.storage.outputs.dlq_name
}

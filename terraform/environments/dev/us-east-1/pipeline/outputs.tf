# ABOUTME: Output definitions for the Pipeline layer
output "state_machine_arn" {
  description = "The ARN of the Step Functions orchestrator state machine"
  value       = aws_sfn_state_machine.transit_pipeline_orchestrator.arn
}

output "raw_crawler_name" {
  description = "The name of the raw Glue crawler"
  value       = aws_glue_crawler.raw_crawler.name
}

output "raw_crawler_arn" {
  description = "The ARN of the raw Glue crawler"
  value       = aws_glue_crawler.raw_crawler.arn
}

# ABOUTME: Output definitions for the Pipeline layer
output "state_machine_arn" {
  description = "The ARN of the Step Functions orchestrator state machine"
  value       = aws_sfn_state_machine.transit_pipeline_orchestrator.arn
}

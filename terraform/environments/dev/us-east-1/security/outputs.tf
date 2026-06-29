# ABOUTME: Output definitions for the Security layer
output "kms_key_arn" {
  description = "The ARN of the KMS customer managed key"
  value       = module.transit_pipeline_kms_key.key_arn
}

output "kms_key_id" {
  description = "The ID of the KMS customer managed key"
  value       = module.transit_pipeline_kms_key.key_id
}

output "glue_job_role_arn" {
  description = "The ARN of the IAM role for Glue jobs"
  value       = aws_iam_role.glue_job_role.arn
}

output "sfn_execution_role_arn" {
  description = "The ARN of the IAM role for Step Functions execution"
  value       = aws_iam_role.sfn_execution_role.arn
}

output "pipe_execution_role_arn" {
  description = "The ARN of the IAM role for EventBridge Pipe execution"
  value       = aws_iam_role.pipe_execution_role.arn
}

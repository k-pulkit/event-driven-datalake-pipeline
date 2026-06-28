# ABOUTME: Output definitions for the Security layer
output "kms_key_arn" {
  description = "The ARN of the KMS customer managed key"
  value       = module.transit_pipeline_kms_key.key_arn
}

output "kms_key_id" {
  description = "The ID of the KMS customer managed key"
  value       = module.transit_pipeline_kms_key.key_id
}

# or ABOUTME: Terraform configuration for the Athena Workgroup with SSE-KMS results encryption and query scan limit cost guardrails

# ==========================================
# ATHENA WORKGROUP CONFIGURATION
# ==========================================
resource "aws_athena_workgroup" "analytics" {
  name        = "${local.project_prefix}-${var.environment}-athena-workgroup"
  description = "Workgroup for data analysis, BI queries, and pipeline aggregations"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    
    # Enforce 10 GB per-query scan limit to prevent runaway costs
    bytes_scanned_cutoff_per_query = 10737418240

    result_configuration {
      output_location = "s3://${local.processed_bucket_id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = local.kms_key_arn
      }
    }
  }

  tags = local.tags
}

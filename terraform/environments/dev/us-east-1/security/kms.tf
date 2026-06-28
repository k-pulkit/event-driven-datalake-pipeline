# ABOUTME: Main orchestration for the Security layer (KMS Key and IAM)

# Ensure we have a valid project name
resource "terraform_data" "assert_project_name_valid" {
  lifecycle {
    precondition {
      # Assert that the local variable is not null or empty
      condition = local.transit_project_name != null && local.transit_project_name != ""

      # The custom error message shown to the developer
      error_message = "ERROR: Local variable 'project_name' resolved to null for project ID '${local.transit_project_name}'. Check the registry in globals.tf."
    }
  }
}

module "transit_pipeline_kms_key" {
  source  = "terraform-aws-modules/kms/aws"
  version = "~> 4.2.0"

  description             = "The Customer Managed KMS key for encrypting data at rest in S3 buckets."
  enable_key_rotation     = true
  deletion_window_in_days = 30

  enable_default_policy = true # Root delegation
  key_statements = [
    {
      sid = "S3 Access"
      actions = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]
      resources = ["*"]

      principals = [
        {
          type        = "Service"
          identifiers = ["s3.amazonaws.com"]
        }
      ]

      condition = [
        {
          test     = "ArnLike"
          variable = "aws:SourceArn"
          values = [
            "arn:aws:s3:::${local.project_prefix}-${var.environment}-${var.aws_region}-*"
          ]
        },
        {
          test     = "StringEquals"
          variable = "kms:ViaService"
          values = [
            "s3.${var.aws_region}.amazonaws.com",
            "sqs.${var.aws_region}.amazonaws.com"
          ]
        }
      ]
    },
    {
      sid = "SQS Access"

      actions = [
        "kms:Decrypt",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ]

      resources = ["*"]

      principals = [{
        type        = "Service"
        identifiers = ["sqs.amazonaws.com"]
      }]

      condition = [
        {
          test     = "ArnLike"
          variable = "aws:SourceArn"
          values = [
            "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${local.project_prefix}-${var.environment}*"
          ]
        },
        {
          test     = "StringEquals"
          variable = "kms:ViaService"
          values = [
            "sqs.${var.aws_region}.amazonaws.com"
          ]
      }]
    }
  ]

  aliases = ["${local.project_prefix}-${var.environment}-kms-key"]

  tags = local.tags
}
# ABOUTME: Main orchestration for the Security layer (KMS Key and IAM)

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
            "arn:aws:sqs:${var.aws_region}:${local.account_id}:${local.project_prefix}-${var.environment}*"
          ]
        },
        {
          test     = "StringEquals"
          variable = "kms:ViaService"
          values = [
            "sqs.${var.aws_region}.amazonaws.com"
          ]
      }]
    },
    {
      sid = "CloudWatchLogsAccess"

      actions = [
        "kms:Encrypt*",
        "kms:Decrypt*",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:Describe*"
      ]

      resources = ["*"]

      principals = [{
        type        = "Service"
        identifiers = ["logs.${var.aws_region}.amazonaws.com"]
      }]

      condition = [
        {
          test     = "ArnLike"
          variable = "kms:EncryptionContext:aws:logs:arn"
          values = [
            "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/jobs/*"
          ]
        }
      ]
    }
  ]

  aliases = ["${local.project_prefix}-${var.environment}-kms-key"]

  tags = local.tags
}
# ABOUTME: Provision SQS Ingest Queue and SQS Ingest DLQ with custom KMS encryption and SQS Access policies

# ==========================================
# SQS QUEUE & DLQ
# S3 Events push raw ingested message events to SQS queue
# ==========================================
module "raw_ingest_sqs_queue" {
  source  = "terraform-aws-modules/sqs/aws"
  version = "~> 5.2.0"

  name = "${local.project_prefix}-${var.environment}-raw-ingest-queue"

  message_retention_seconds  = 86400 * 7 # 7 days
  visibility_timeout_seconds = 3600      # 1 hour

  kms_master_key_id                 = local.kms_master_key_id
  kms_data_key_reuse_period_seconds = 3600

  # Policy to allow S3 Events to send messages to the queue
  create_queue_policy = true
  queue_policy_statements = {
    s3_access = {
      sid    = "S3Access"
      effect = "Allow"
      actions = [
        "sqs:SendMessage"
      ]
      principals = [
        {
          type        = "Service"
          identifiers = ["s3.amazonaws.com"]
        }
      ]
      conditions = [
        {
          test     = "StringEquals"
          variable = "aws:SourceAccount"
          values = [
            "${local.account_id}"
          ]
        },
        {
          test     = "ArnLike"
          variable = "aws:SourceArn"
          values = [
            module.s3_datalake_buckets["landing"].s3_bucket_arn
          ]
        }
      ]
    }
  }

  # Configure dead letter queue
  create_dlq = true
  redrive_policy = {
    maxReceiveCount = 3
  }

  tags = local.tags

  depends_on = [module.s3_datalake_buckets]
}
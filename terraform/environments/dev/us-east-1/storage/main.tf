# ABOUTME: Main orchestration for the Data layer (S3 and SQS resources)

# ==========================================
# ACCESS LOGS BUCKET
# Need to enable ACLs for access logs bucket, so S3 server logging service and write logs to S3 bucket
# ==========================================
module "s3_access_logs" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 5.14.0"

  bucket = "${local.project_prefix}-${var.environment}-${var.aws_region}-access-logs"

  control_object_ownership = true
  object_ownership         = "ObjectWriter"

  # Grants permissions for S3 log delivery                                                                                                    
  grant = [
    {
      type       = "Group"
      permission = "WRITE"
      uri        = "http://acs.amazonaws.com/groups/s3/LogDelivery"
    },
    {
      type       = "Group"
      permission = "READ_ACP"
      uri        = "http://acs.amazonaws.com/groups/s3/LogDelivery"
    }
  ]

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = local.tags
}

# ==========================================
# DATALAKE BUCKETS
# Create datalake buckets, and enable server access logging for each bucket
# ==========================================
module "s3_datalake_buckets" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 5.14.0"

  for_each = toset(["landing", "silver", "gold", "queries", "quarantine"])

  bucket = "${local.project_prefix}-${var.environment}-${var.aws_region}-${each.key}-zone"

  # Enable server access logging
  logging = {
    target_bucket = module.s3_access_logs.s3_bucket_id
    target_prefix = "logs/${each.key}/"
  }

  # Enable customer managed encyption for datalake buckets
  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = local.kms_master_key_id
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  tags = local.tags

  depends_on = [module.s3_access_logs]
}

# ==========================================
# S3 EVENTS
# Configure S3 Event Notifications
# ==========================================
resource "aws_s3_bucket_notification" "raw_ingestion_event_notification" {
  bucket = module.s3_datalake_buckets["landing"].s3_bucket_id
  queue {
    queue_arn     = module.raw_ingest_sqs_queue.queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".csv"
  }

  depends_on = [module.s3_datalake_buckets, module.raw_ingest_sqs_queue]
}

# ABOUTME: Glue Crawler configurations for raw landing zone data auto-discovery (Pipeline Layer)

# ==========================================
# GLUE CRAWLER (Landing Zone Auto-Discovery)
# ==========================================
resource "aws_glue_crawler" "raw_crawler" {
  database_name = data.terraform_remote_state.storage.outputs.landing_db_name
  name          = "${local.project_prefix}-${var.environment}-raw-crawler"
  role          = local.glue_job_role_arn

  # Crawl S3 Landing targets (isolation of transactions and lookups)
  s3_target {
    path = "s3://${local.landing_bucket_id}/incoming/"
  }

  s3_target {
    path = "s3://${local.landing_bucket_id}/dimensions/routes/"
  }

  s3_target {
    path = "s3://${local.landing_bucket_id}/dimensions/vehicles/"
  }

  # Configures table names prefix to clarify their quality tier
  table_prefix = "raw_"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        TableThreshold = 1
      }
    }
  })

  tags = local.tags
}

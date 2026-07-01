# ABOUTME: Glue Crawler configurations for raw landing zone data auto-discovery (Pipeline Layer)

# Custom CSV Classifier to force headers parsing
resource "aws_glue_classifier" "csv_header_classifier" {
  name = "${local.project_prefix}-${var.environment}-csv-header-classifier"

  csv_classifier {
    allow_single_column    = false
    contains_header        = "PRESENT"
    delimiter              = ","
    disable_value_trimming = false
    header                 = []
    quote_symbol           = "\""
  }
}

# ==========================================
# GLUE CRAWLER (Landing Zone Auto-Discovery)
# ==========================================
resource "aws_glue_crawler" "raw_crawler" {
  database_name = data.terraform_remote_state.storage.outputs.landing_db_name
  name          = "${local.project_prefix}-${var.environment}-raw-crawler"
  role          = local.glue_job_role_arn
  classifiers   = [aws_glue_classifier.csv_header_classifier.name]

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
        TableThreshold = 3
      }
    }
  })

  tags = local.tags
}

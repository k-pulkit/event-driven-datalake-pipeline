# ABOUTME: Provision Glue PySpark (Silver) and Glue PySpark (Gold) jobs with KMS security configurations

# ==========================================
# S3 SCRIPT UPLOADS
# ==========================================
resource "aws_s3_object" "glue_silver_script" {
  bucket = local.landing_bucket_id
  key    = "scripts/silver_processing.py"
  source = "../../../../../glue-scripts/src/silver_processing.py"
  etag   = filemd5("../../../../../glue-scripts/src/silver_processing.py")
}

resource "aws_s3_object" "glue_gold_script" {
  bucket = local.landing_bucket_id
  key    = "scripts/gold_aggregation.py"
  source = "../../../../../glue-scripts/src/gold_aggregation.py"
  etag   = filemd5("../../../../../glue-scripts/src/gold_aggregation.py")
}

# ==========================================
# GLUE SECURITY CONFIGURATION
# ==========================================
resource "aws_glue_security_configuration" "glue_sec_config" {
  name = "${local.project_prefix}-${var.environment}-glue-sec-config"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = local.kms_key_arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = local.kms_key_arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = local.kms_key_arn
    }
  }
}

# ==========================================
# SILVER PROCESSING JOB (PySpark)
# ==========================================
resource "aws_glue_job" "silver_job" {
  name                   = local.glue_silver_job_name
  role_arn               = local.glue_job_role_arn
  glue_version           = "5.1"
  worker_type            = "G.1X"
  number_of_workers      = 2
  security_configuration = aws_glue_security_configuration.glue_sec_config.name

  command {
    name            = "glueetl"
    script_location = "s3://${local.landing_bucket_id}/${aws_s3_object.glue_silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--TempDir"                 = "s3://${local.landing_bucket_id}/temporary/"
    "--job-bookmark-option"     = "job-bookmark-disable"
    "--datalake-formats"        = "iceberg"
    "--enable-glue-datacatalog" = "true"
    "--catalog_name"            = "iceberg_glue"
    "--database_name"           = local.landing_db_name
    "--silver_database"         = local.silver_db_name
    "--silver_table_active"     = "silver_trips_active"
    "--silver_table_history"    = "silver_trips_history"
    "--quarantine_bucket"       = local.quarantine_bucket_id
    "--processed_bucket"        = local.processed_bucket_id
    "--s3_file_paths"           = ""
    "--iceberg_branch_name"     = ""

    # Configure Spark to run Apache Iceberg with Glue Data Catalog
    "--conf" = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.iceberg_glue=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.iceberg_glue.warehouse=s3://${local.processed_bucket_id}/ --conf spark.sql.catalog.iceberg_glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.iceberg_glue.glue.skip-name-validation=false"

    # Observability & Monitoring Settings
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.landing_bucket_id}/sparkHistoryLogs/"
  }

  tags = local.tags
}

# ==========================================
# GOLD AGGREGATION JOB (PySpark)
# ==========================================
resource "aws_glue_job" "gold_job" {
  name                   = local.glue_gold_job_name
  role_arn               = local.glue_job_role_arn
  glue_version           = "5.1"
  worker_type            = "G.1X"
  number_of_workers      = 2
  security_configuration = aws_glue_security_configuration.glue_sec_config.name

  command {
    name            = "glueetl"
    script_location = "s3://${local.landing_bucket_id}/${aws_s3_object.glue_gold_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--TempDir"                 = "s3://${local.landing_bucket_id}/temporary/"
    "--job-bookmark-option"     = "job-bookmark-disable"
    "--datalake-formats"        = "iceberg"
    "--enable-glue-datacatalog" = "true"
    "--catalog_name"            = "iceberg_glue"
    "--database_name"           = local.landing_db_name
    "--silver_database"         = local.silver_db_name
    "--silver_table_active"     = "silver_trips_active"
    "--silver_table_history"    = "silver_trips_history"
    "--start_date"              = ""
    "--end_date"                = ""
    "--iceberg_branch_name"     = "test_branch"
    "--rds_secret_name"         = local.rds_secret_name

    # Configure Spark to run Apache Iceberg with Glue Data Catalog
    "--conf" = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.iceberg_glue=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.iceberg_glue.warehouse=s3://${local.processed_bucket_id}/ --conf spark.sql.catalog.iceberg_glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.iceberg_glue.glue.skip-name-validation=false"

    # Observability & Monitoring Settings
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.landing_bucket_id}/sparkHistoryLogs/"
  }

  tags = local.tags
}

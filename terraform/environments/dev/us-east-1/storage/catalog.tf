# ABOUTME: Glue Data Catalog databases, Crawler, and Catalog Encryption configuration for the storage layer

# ==========================================
# GLUE DATA CATALOG DATABASES
# ==========================================
resource "aws_glue_catalog_database" "landing_db" {
  name        = replace("${local.project_prefix}-${var.environment}-landing-db", "-", "_")
  description = "Glue Catalog database for Raw Landing Zone data"
}

resource "aws_glue_catalog_database" "silver_db" {
  name        = replace("${local.project_prefix}-${var.environment}-silver-db", "-", "_")
  description = "Glue Catalog database for Standardized Silver Zone"
}

# ==========================================
# GLUE DATA CATALOG ENCRYPTION SETTINGS
# ==========================================
resource "aws_glue_data_catalog_encryption_settings" "catalog_encryption" {
  data_catalog_encryption_settings {
    connection_password_encryption {
      aws_kms_key_id                       = local.kms_master_key_arn
      return_connection_password_encrypted = true
    }
    encryption_at_rest {
      catalog_encryption_mode = "SSE-KMS"
      sse_aws_kms_key_id      = local.kms_master_key_arn
    }
  }
}



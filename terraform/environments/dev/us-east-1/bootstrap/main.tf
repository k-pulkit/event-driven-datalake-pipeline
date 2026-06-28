# ABOUTME: Bootstrap configuration to provision S3 remote state bucket and DynamoDB lock table
resource "aws_s3_bucket" "state_bucket" {
  bucket = "pkca-terraform-${var.environment}-${var.aws_region}-state-bucket"
  region = var.aws_region

  tags = {
    Path = "terraform/environments/dev/us-east-1/bootstrap/"
  }
}

resource "aws_s3_bucket_public_access_block" "state_bucket_public_block" {
  bucket                  = aws_s3_bucket.state_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "state_bucket_versioning" {
  bucket = aws_s3_bucket.state_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "state_bucket_lifecycle" {
  bucket = aws_s3_bucket.state_bucket.id
  rule {
    status = "Enabled"
    id     = "object_versions"
    # Retain current files indefinitely.
    # Only clean up historical versions.
    noncurrent_version_expiration {
      newer_noncurrent_versions = 3
      noncurrent_days           = 7
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state_bucket_encryption" {
  bucket = aws_s3_bucket.state_bucket.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_dynamodb_table" "lock_table" {
  name = "pkca-terraform-${var.environment}-lock"

  billing_mode = "PAY_PER_REQUEST"

  hash_key = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Path = "terraform/environments/dev/us-east-1/bootstrap/"
  }
}
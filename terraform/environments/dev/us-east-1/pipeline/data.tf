# ABOUTME: Remote state data source lookups for security (IAM roles) and storage (SQS queue)

data "aws_caller_identity" "current" {}

data "terraform_remote_state" "security" {
  backend = "s3"
  config = {
    bucket         = "pkca-terraform-dev-us-east-1-state-bucket"
    key            = "terraform/environments/${var.environment}/${var.aws_region}/security/terraform.tfstate"
    region         = var.aws_region
    dynamodb_table = "pkca-terraform-dev-lock"
  }
}

data "terraform_remote_state" "storage" {
  backend = "s3"
  config = {
    bucket         = "pkca-terraform-dev-us-east-1-state-bucket"
    key            = "terraform/environments/${var.environment}/${var.aws_region}/storage/terraform.tfstate"
    region         = var.aws_region
    dynamodb_table = "pkca-terraform-dev-lock"
  }
}

# or ABOUTME: Remote state data source lookups for the Monitoring layer (reading storage and pipeline outputs)

data "aws_caller_identity" "current" {}

data "terraform_remote_state" "storage" {
  backend = "s3"
  config = {
    bucket         = "pkca-terraform-dev-us-east-1-state-bucket"
    key            = "terraform/environments/${var.environment}/${var.aws_region}/storage/terraform.tfstate"
    region         = var.aws_region
  }
}

data "terraform_remote_state" "pipeline" {
  backend = "s3"
  config = {
    bucket         = "pkca-terraform-dev-us-east-1-state-bucket"
    key            = "terraform/environments/${var.environment}/${var.aws_region}/pipeline/terraform.tfstate"
    region         = var.aws_region
  }
}

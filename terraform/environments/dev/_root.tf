# ABOUTME: Common configs for all environments
# Versions, Common vars, Provider settings
# This file contains the key configuration that are shared across all regions in current environment.

# ==========================================                                                                                                  
# 1. Terraform Settings & Backend                                                                                                             
# ==========================================  
terraform {
  required_version = ">= 1.5.0, < 2.0.0" # Constrain the CLI to a tested range

  # 1. Required provider versions
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.52.0"
    }
  }

  # 2. Empty S3 Backend configuration
  # terraform init \
  #    -backend-config="$(git rev-parse --show-toplevel)/terraform/environments/dev/backend-shared.tfvars" \
  #    -backend-config="key=$(git rev-parse --show-prefix)terraform.tfstate"
  backend "s3" {}
}

# ==========================================
# 3. Provider Settings
# ==========================================
provider "aws" {
  region              = var.aws_region
  allowed_account_ids = ["164593963429"]

  dynamic "assume_role" {
    for_each = var.is_github_actions ? [1] : []

    content {
      role_arn     = var.terraform_deployment_role_arn
      session_name = "TerraformDeploymentSession-${var.environment}"
    }
  }

  default_tags {
    tags = {
      Environment = var.environment
      Region      = var.aws_region
      ManagedBy   = "Terraform"
      Owner       = "EDAI-DE"
      Path        = "${var.environment}/${var.aws_region}/" # Default path unless overridden
    }
  }
}

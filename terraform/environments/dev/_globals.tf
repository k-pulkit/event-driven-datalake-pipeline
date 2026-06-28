#ABOUTME: Shared variable declarations
variable "is_github_actions" {
  type        = bool
  description = "Flags if being run in CICD runner"
  default     = false
}

variable "terraform_deployment_role_arn" {
  type        = string
  description = "Terraform deployment role ARN"
  default     = "arn:aws:iam::164593963429:role/TerraformDeploymentRole-Dev"
}

variable "namespace" {
  type        = string
  description = "Org Name"
  default     = "edai"
}

variable "project_name_mapping" {
  type        = map(string)
  description = "Project Name Map"
  default = {
    "transit-pipeline" = "city-transit-pipeline"
  }
}

variable "environment" {
  type        = string
  description = "Environment"
  default     = "dev"
  validation {
    condition     = var.environment == "dev"
    error_message = "Cannot use environment value other than 'dev'. Please validate the execution directory"
  }
}

variable "aws_region" {
  type        = string
  description = "AWS region"
}
# ABOUTME: Common local variables for the Storage layer bucket prefixing and tagging
locals {
  transit_project_id = "transit-pipeline"

  account_id        = data.aws_caller_identity.current.account_id
  kms_master_key_id = data.terraform_remote_state.security.outputs.kms_key_id

  transit_project_name = lookup(var.project_name_mapping, local.transit_project_id, null)
  project_prefix       = "${var.namespace}-${local.transit_project_name}"
  tags = {
    Path = "terraform/environments/${var.environment}/${var.aws_region}/storage/"
  }
}

# Ensure we have a valid project name
resource "terraform_data" "assert_project_name_valid" {
  lifecycle {
    precondition {
      # Assert that the local variable is not null or empty
      condition = local.transit_project_name != null && local.transit_project_name != ""

      # The custom error message shown to the developer
      error_message = "ERROR: Local variable 'project_name' resolved to null for project ID '${local.transit_project_name}'. Check the registry in globals.tf."
    }
  }
}
# ABOUTME: Common local variables for the Storage layer bucket prefixing and tagging
locals {
  transit_project_id = "transit-pipeline"

  kms_master_key_id = data.terraform_remote_state.security.outputs.kms_key_id

  transit_project_name = lookup(var.project_name_mapping, local.transit_project_id, null)
  project_prefix       = "${var.namespace}-${local.transit_project_name}"
  tags = {
    Path = "terraform/environments/${var.environment}/${var.aws_region}/storage/"
  }
}
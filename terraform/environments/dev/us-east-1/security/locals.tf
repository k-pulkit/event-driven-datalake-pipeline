# ABOUTME: Common local variables for security resource naming and tagging
locals {
  transit_project_id = "transit-pipeline"

  transit_project_name = lookup(var.project_name_mapping, local.transit_project_id, null)
  project_prefix       = "${var.namespace}-${local.transit_project_name}"
  tags = {
    Path = "terraform/environments/${var.environment}/${var.aws_region}/security/"
  }
}
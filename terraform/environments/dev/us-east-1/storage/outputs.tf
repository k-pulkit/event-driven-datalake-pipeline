# ABOUTME: Output definitions for the Data layer
output "raw_bucket_id" {
  description = "The name of the Raw S3 bucket"
  value       = module.s3_datalake_buckets["landing"].s3_bucket_id
}

output "raw_bucket_arn" {
  description = "The ARN of the Raw S3 bucket"
  value       = module.s3_datalake_buckets["landing"].s3_bucket_arn
}

output "processed_bucket_id" {
  description = "The name of the Processed S3 bucket"
  value       = module.s3_datalake_buckets["silver"].s3_bucket_id
}

output "quarantine_bucket_id" {
  description = "The name of the Quarantine S3 bucket"
  value       = module.s3_datalake_buckets["quarantine"].s3_bucket_id
}

output "queue_url" {
  description = "The URL of the main SQS queue"
  value       = module.raw_ingest_sqs_queue.queue_url
}

output "queue_arn" {
  description = "The ARN of the main SQS queue"
  value       = module.raw_ingest_sqs_queue.queue_arn
}

output "dlq_url" {
  description = "The URL of the DLQ SQS queue"
  value       = module.raw_ingest_sqs_queue.dead_letter_queue_arn
}

output "landing_db_name" {
  description = "The name of the Glue Landing Data Catalog Database"
  value       = aws_glue_catalog_database.landing_db.name
}

output "silver_db_name" {
  description = "The name of the Glue Silver Data Catalog Database"
  value       = aws_glue_catalog_database.silver_db.name
}

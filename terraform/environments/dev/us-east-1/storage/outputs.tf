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

# ==========================================
# RDS PostgreSQL Outputs
# ==========================================
output "rds_endpoint" {
  description = "The connection endpoint of the RDS PostgreSQL instance"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_address" {
  description = "The connection address (hostname) of the RDS PostgreSQL instance"
  value       = aws_db_instance.postgres.address
}

output "rds_port" {
  description = "The port the RDS PostgreSQL database is listening on"
  value       = aws_db_instance.postgres.port
}

output "rds_db_name" {
  description = "The name of the default database created in RDS"
  value       = aws_db_instance.postgres.db_name
}

output "rds_username" {
  description = "The master username for the RDS PostgreSQL database"
  value       = aws_db_instance.postgres.username
}

output "rds_password" {
  description = "The master database password"
  value       = random_password.db_password.result
  sensitive   = true
}

output "rds_security_group_id" {
  description = "The ID of the RDS security group"
  value       = aws_security_group.rds_sg.id
}

output "rds_secret_name" {
  description = "The friendly name of the Secrets Manager secret storing RDS credentials"
  value       = aws_secretsmanager_secret.db_secret.name
}

output "queue_name" {
  description = "The name of the main SQS ingest queue"
  value       = module.raw_ingest_sqs_queue.queue_name
}

output "dlq_name" {
  description = "The name of the SQS Dead Letter Queue (DLQ)"
  value       = module.raw_ingest_sqs_queue.dead_letter_queue_name
}

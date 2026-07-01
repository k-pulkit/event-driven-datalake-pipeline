# ABOUTME: IAM Role and policies for Step Functions workflow execution and orchestration

resource "aws_iam_role" "sfn_execution_role" {
  name = "${local.project_prefix}-${var.environment}-sfn-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = local.account_id
        }
      }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "sfn_execution_policy" {
  name = "${local.project_prefix}-${var.environment}-sfn-execution-policy"
  role = aws_iam_role.sfn_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Glue Job Invocation
      {
        Sid    = "GlueJobInvocation"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.account_id}:job/${local.project_prefix}-*"
        ]
      },
      # SQS Queue Access (Polling and Deleting)
      {
        Sid    = "SQSQueueAccess"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:DeleteMessageBatch",
          "sqs:GetQueueAttributes"
        ]
        Resource = [
          "arn:aws:sqs:${var.aws_region}:${local.account_id}:${local.project_prefix}-${var.environment}-*"
        ]
      },
      # KMS Decryption for SQS
      {
        Sid    = "KMSDecryptAccess"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = [
          module.transit_pipeline_kms_key.key_arn
        ]
      },
      # Step Functions Self-Lookup (concurrency check)
      {
        Sid    = "SFNSelfLookup"
        Effect = "Allow"
        Action = [
          "states:ListExecutions"
        ]
        Resource = [
          "arn:aws:states:${var.aws_region}:${local.account_id}:stateMachine:${local.project_prefix}-${var.environment}-orchestrator"
        ]
      },
      # SNS Alerting Access
      {
        Sid    = "SNSAlertingAccess"
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [
          "arn:aws:sns:${var.aws_region}:${local.account_id}:${var.namespace}-${var.environment}-alerts-topic"
        ]
      },
      # S3 Read Access for Run Metadata
      {
        Sid    = "S3MetadataRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = [
          "arn:aws:s3:::${local.project_prefix}-${var.environment}-*-processed-zone/metadata/*"
        ]
      },
      # CloudWatch Logging / Log Delivery
      {
        Sid    = "CloudWatchLogsDelivery"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

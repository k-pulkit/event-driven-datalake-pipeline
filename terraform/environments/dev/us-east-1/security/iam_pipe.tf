# ABOUTME: IAM Role and policies for EventBridge Pipe SQS-to-SFN synchronous trigger

resource "aws_iam_role" "pipe_execution_role" {
  name = "${local.project_prefix}-${var.environment}-pipe-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "pipes.amazonaws.com"
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

resource "aws_iam_role_policy" "pipe_execution_policy" {
  name = "${local.project_prefix}-${var.environment}-pipe-execution-policy"
  role = aws_iam_role.pipe_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Poll SQS Ingest Queue
      {
        Sid    = "SQSPollingAccess"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = [
          "arn:aws:sqs:${var.aws_region}:${local.account_id}:${local.project_prefix}-${var.environment}-*"
        ]
      },
      # Decrypt SQS Ingest Queue using KMS key
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
      # Trigger Step Functions Orchestrator
      {
        Sid    = "SFNTriggerAccess"
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          "arn:aws:states:${var.aws_region}:${local.account_id}:stateMachine:${local.project_prefix}-*"
        ]
      }
    ]
  })
}

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

# ABOUTME: IAM Role and policies for EventBridge scheduler to trigger Step Functions

resource "aws_iam_role" "eventbridge_sfn_target_role" {
  name = "${local.project_prefix}-${var.environment}-eventbridge-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "scheduler.amazonaws.com"
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

resource "aws_iam_role_policy" "eventbridge_sfn_target_policy" {
  name = "${local.project_prefix}-${var.environment}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_sfn_target_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "AllowEventBridgeToStartExecution"
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = "arn:aws:states:${var.aws_region}:${local.account_id}:stateMachine:${local.project_prefix}-${var.environment}-orchestrator"
    }]
  })
}

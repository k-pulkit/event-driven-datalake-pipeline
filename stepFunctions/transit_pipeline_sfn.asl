{
  "Comment": "Transit Ingestion Pipeline Orchestrator with Native SQS Polling, Serialization, and Alerting (JSONata)",
  "QueryLanguage": "JSONata",
  "StartAt": "InitScope",
  "States": {
    "InitScope": {
      "Type": "Pass",
      "Comment": "Initialize state variables with default values",
      "Assign": {
        "sqs_messages": "{% [] %}",
        "active_runs": "{% [] %}",
        "error_message": "{% '' %}",
        "error_source": "{% 'None' %}"
      },
      "Next": "CheckActiveRuns"
    },
    "CheckActiveRuns": {
      "Type": "Task",
      "Comment": "List active executions to prevent concurrent processing",
      "Resource": "arn:aws:states:::aws-sdk:sfn:listExecutions",
      "Arguments": {
        "StateMachineArn": "arn:aws:states:${aws_region}:${account_id}:stateMachine:${project_prefix}-${environment}-orchestrator",
        "StatusFilter": "RUNNING"
      },
      "Assign": {
        "active_runs": "{% $states.result.Executions %}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Comment": "Handle active executions listing failure",
          "Assign": {
            "error_message": "{% 'Active execution check failed: ' & $string($states.errorOutput) %}",
            "error_source": "CheckActiveRuns"
          },
          "Next": "ConstructAlertMessage"
        }
      ],
      "Next": "DetermineConcurrency"
    },
    "DetermineConcurrency": {
      "Type": "Choice",
      "Comment": "Succeed immediately if another execution is running",
      "Choices": [
        {
          "Condition": "{% $count($active_runs) > 1 %}",
          "Next": "ExitConcurrency"
        }
      ],
      "Default": "PollSQS"
    },
    "ExitConcurrency": {
      "Type": "Succeed",
      "Comment": "Serial processing safety exit: another instance is active"
    },
    "PollSQS": {
      "Type": "Task",
      "Comment": "Poll batch of messages from raw ingestion SQS queue",
      "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
      "Arguments": {
        "QueueUrl": "${sqs_queue_url}",
        "MaxNumberOfMessages": 10,
        "WaitTimeSeconds": 20,
        "VisibilityTimeout": 1800
      },
      "Assign": {
        "sqs_messages": "{% $states.result.Messages %}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 3,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Comment": "Handle SQS polling failure",
          "Assign": {
            "error_message": "{% 'SQS message polling failed: ' & $string($states.errorOutput) %}",
            "error_source": "PollSQS"
          },
          "Next": "ConstructAlertMessage"
        }
      ],
      "Next": "CheckMessagesExist"
    },
    "CheckMessagesExist": {
      "Type": "Choice",
      "Comment": "Route execution depending on whether SQS had messages",
      "Choices": [
        {
          "Condition": "{% $count($sqs_messages) = 0 %}",
          "Next": "ExitEmpty"
        }
      ],
      "Default": "RunGlueSilverJob"
    },
    "ExitEmpty": {
      "Type": "Succeed",
      "Comment": "No data found: SQS raw queue is empty"
    },
    "RunGlueSilverJob": {
      "Type": "Task",
      "Comment": "Execute PySpark Glue Silver Job to process raw CSV files",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "TimeoutSeconds": 300,
      "Arguments": {
        "JobName": "${glue_silver_job_name}",
        "Arguments": {
          "--s3_bucket": "${landing_bucket_id}",
          "--s3_file_paths": "{% $string($map($sqs_messages.Body, function($body) {$eval($body).Records.s3.('s3://' & bucket.name & '/' & object.key)} )) %}",
          "--iceberg_branch_name": "{% 'wap_' & $states.context.Execution.Name %}"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Glue.ConcurrentRunsExceededException"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Comment": "Handle Glue Silver job failure",
          "Assign": {
            "error_message": "{% 'Glue Silver Job execution failed: ' & $string($states.errorOutput) %}",
            "error_source": "GlueSilverProcessing"
          },
          "Next": "ConstructAlertMessage"
        }
      ],
      "Next": "RunGlueGoldJob"
    },
    "RunGlueGoldJob": {
      "Type": "Task",
      "Comment": "Execute Python Shell Glue Gold Job to aggregate ridership records",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "TimeoutSeconds": 300,
      "Arguments": {
        "JobName": "${glue_gold_job_name}",
        "Arguments": {
          "--iceberg_branch_name": "{% 'wap_' & $states.context.Execution.Name %}"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Glue.ConcurrentRunsExceededException"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Comment": "Handle Glue Gold job failure",
          "Assign": {
            "error_message": "{% 'Glue Gold Job execution failed: ' & $string($states.errorOutput) %}",
            "error_source": "GlueGoldAggregation"
          },
          "Next": "ConstructAlertMessage"
        }
      ],
      "Next": "DeleteSQSMessages"
    },
    "DeleteSQSMessages": {
      "Type": "Map",
      "Comment": "Delete successfully processed message batch from SQS",
      "Items": "{% $sqs_messages %}",
      "MaxConcurrency": 10,
      "ItemProcessor": {
        "ProcessorConfig": {
          "Mode": "INLINE"
        },
        "StartAt": "DeleteSingleMessage",
        "States": {
          "DeleteSingleMessage": {
            "Type": "Task",
            "Comment": "Delete single message by its SQS receipt handle",
            "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
            "Arguments": {
              "QueueUrl": "${sqs_queue_url}",
              "ReceiptHandle": "{% $states.input.ReceiptHandle %}"
            },
            "Retry": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "IntervalSeconds": 2,
                "MaxAttempts": 3,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "DeleteSingleMessageFailed"
              }
            ],
            "End": true
          },
          "DeleteSingleMessageFailed": {
            "Type": "Pass",
            "Comment": "Ignore individual delete failure, let SQS handle visible retries",
            "End": true
          }
        }
      },
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Comment": "Handle SQS message deletion batch failure",
          "Assign": {
            "error_message": "{% 'SQS message deletion batch failed: ' & $string($states.errorOutput) %}",
            "error_source": "DeleteSQSMessages"
          },
          "Next": "ConstructAlertMessage"
        }
      ],
      "Next": "PipelineSucceeded"
    },
    "ConstructAlertMessage": {
      "Type": "Pass",
      "Comment": "Construct dynamic SNS email alert message body using JSONata",
      "Assign": {
        "sns_message": "{% 'ALERT: Transit Ingestion Pipeline Invalidation Detected.\n\nFailed Stage: ' & $error_source & '\nError Detail: ' & $error_message & '\nExecution ID: ' & $states.context.Execution.Id & '\nTimestamp: ' & $now() %}"
      },
      "Next": "SendAlertNotification"
    },
    "SendAlertNotification": {
      "Type": "Task",
      "Comment": "Publish failure notification to SNS Topic",
      "Resource": "arn:aws:states:::aws-sdk:sns:publish",
      "Arguments": {
        "TopicArn": "${sns_topic_arn}",
        "Subject": "Transit Pipeline Ingestion Failure (${environment})",
        "Message": "{% $sns_message %}"
      },
      "Next": "PipelineFailed"
    },
    "PipelineSucceeded": {
      "Type": "Succeed",
      "Comment": "Pipeline completed successfully. Messages removed from queue."
    },
    "PipelineFailed": {
      "Type": "Fail",
      "Comment": "Pipeline execution failed. Alert sent and SQS messages preserved.",
      "Error": "IngestionFailed",
      "Cause": "Orchestration failed during execution of processing stages."
    }
  }
}
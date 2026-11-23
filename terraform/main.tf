terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "name" {
  type    = string
  default = "mufc-montecarlo"
}

variable "timeout_seconds" {
  description = "Function timeout. 1800 is the new on-demand maximum."
  type        = number
  default     = 1800
}

data "aws_caller_identity" "current" {}

locals {
  # AWS guidance: SQS visibility timeout >= 6x the function timeout.
  # For a 30 minute function that's 3 hours. Lambda itself only enforces
  # visibility >= 1x the timeout, and only when the event source mapping is
  # created or updated - raising the function timeout later isn't checked.
  visibility_timeout = var.timeout_seconds * 6
}

################################################################################
# Results bucket
################################################################################
resource "aws_s3_bucket" "results" {
  bucket        = "${var.name}-results-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

################################################################################
# Queue + DLQ
################################################################################
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "jobs" {
  name                       = "${var.name}-jobs"
  visibility_timeout_seconds = local.visibility_timeout
  message_retention_seconds  = 86400

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 2
  })
}

################################################################################
# Lambda
################################################################################
# build/ is created by ./build.sh (handler + numpy for arm64)
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../build"
  output_path = "${path.module}/../dist/lambda.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${var.name}-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "sqs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
}

resource "aws_iam_role_policy" "results" {
  name = "results-bucket"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "${aws_s3_bucket.results.arn}/results/*"
      },
      {
        # needed so HeadObject on a missing key returns 404 rather than 403
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.results.arn
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "sim" {
  function_name    = var.name
  role             = aws_iam_role.lambda.arn
  handler          = "handler.handler"
  runtime          = "python3.14"
  architectures    = ["arm64"]
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  # The AWS provider (v6.73.0 at time of writing) still validates timeout
  # as 1-900, so create at 900 and raise it below with the CLI.
  timeout     = 900
  memory_size = 1769 # one full vCPU - numpy here is single threaded so more won't help

  # Cap how many 30 minute runs can be going at once (and the bill).
  reserved_concurrent_executions = 5

  environment {
    variables = {
      RESULTS_BUCKET   = aws_s3_bucket.results.bucket
      SAFETY_MARGIN_MS = "60000"
    }
  }

  lifecycle {
    # timeout is managed by terraform_data.lambda_timeout until the provider
    # accepts values over 900
    ignore_changes = [timeout]
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# Workaround for the provider's 900s validation: set the real timeout via the
# AWS CLI. Re-runs whenever the timeout variable or function code changes.
resource "terraform_data" "lambda_timeout" {
  triggers_replace = [var.timeout_seconds, aws_lambda_function.sim.source_code_hash]

  provisioner "local-exec" {
    command = <<-EOT
      aws lambda wait function-updated-v2 --region ${var.region} --function-name ${aws_lambda_function.sim.function_name}
      aws lambda update-function-configuration --region ${var.region} --function-name ${aws_lambda_function.sim.function_name} --timeout ${var.timeout_seconds} --query Timeout
      aws lambda wait function-updated-v2 --region ${var.region} --function-name ${aws_lambda_function.sim.function_name}
    EOT
  }
}

resource "aws_lambda_event_source_mapping" "jobs" {
  # make sure the function is at its real timeout before SQS starts invoking it
  depends_on = [terraform_data.lambda_timeout]

  event_source_arn        = aws_sqs_queue.jobs.arn
  function_name           = aws_lambda_function.sim.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 5
  }
}

################################################################################
# Outputs
################################################################################
output "queue_url" {
  value = aws_sqs_queue.jobs.url
}

output "results_bucket" {
  value = aws_s3_bucket.results.bucket
}

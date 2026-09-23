provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "Terraform"
    }
  }
}

locals {
  service_templates = { for filename in fileset("${path.module}/policies", "*_policy.json") : trimsuffix(filename, "_policy.json") => filename }
}

moved {
  from = aws_iam_policy.project["healthcare_hai_prediction_s3_policy"]
  to   = aws_iam_policy.service["s3"]
}

moved {
  from = aws_iam_user_policy_attachment.project["healthcare_hai_prediction_s3_policy"]
  to   = aws_iam_user_policy_attachment.service["s3"]
}

resource "aws_iam_policy" "service" {
  for_each    = local.service_templates
  name        = "${var.project_name}_${each.key}_policy"
  description = "Project-scoped HAI storage permissions; no IAM administration or data deletion."
  policy = templatefile("${path.module}/policies/${each.value}", {
    DATA_BUCKET_NAME = var.data_bucket_name
    AWS_REGION       = var.aws_region
    AWS_ACCOUNT_ID   = var.expected_account_id
    PROJECT_NAME     = var.project_name
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_user_policy_attachment" "service" {
  for_each   = aws_iam_policy.service
  user       = var.deployment_user_name
  policy_arn = each.value.arn
}

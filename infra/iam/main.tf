provider "aws" {
  profile             = var.admin_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

locals {
  service_templates = { for filename in fileset("${path.module}/policies", "*_policy.json") : trimsuffix(filename, "_policy.json") => filename }
  # A description change replaces the policy, which prevent_destroy blocks, so existing policies keep the shared text.
  service_descriptions = {
    lakehouse = "Project-scoped Iceberg table files; write and delete current objects only under lakehouse/."
  }
}

resource "aws_iam_policy" "service" {
  for_each    = local.service_templates
  name        = "${var.project_name}_${each.key}_policy"
  description = lookup(local.service_descriptions, each.key, "Project-scoped HAI storage permissions; no IAM administration or data deletion.")
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
  user       = var.aws_profile
  policy_arn = each.value.arn
}

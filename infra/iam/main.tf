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
  secret_names = [var.census_secret_name, var.bls_secret_name, var.hud_secret_name, var.jev_secret_name, var.airflow_admin_secret_name]
  managed_secret_names = [
    var.postgres_password_name,
    var.polaris_root_secret_name,
    var.polaris_reader_client_id_name,
    var.polaris_reader_client_secret_name,
    var.polaris_gold_client_id_name,
    var.polaris_gold_client_secret_name,
    var.polaris_silver_client_id_name,
    var.polaris_silver_client_secret_name,
    var.airflow_db_password_name,
    var.airflow_fernet_key_name,
    var.airflow_jwt_secret_name,
    var.airflow_api_secret_key_name,
    var.marquez_db_password_name,
  ]
  service_descriptions = {
    lakehouse    = "Project-scoped Iceberg table files; write and delete current objects only under lakehouse/."
    assume_roles = "Lets the project user assume the two lakehouse roles and no other role."
  }
}

resource "aws_iam_policy" "service" {
  for_each    = local.service_templates
  name        = "${var.project_name}_${each.key}_policy"
  description = lookup(local.service_descriptions, each.key, "Project-scoped HAI storage permissions; no IAM administration or data deletion.")
  policy = templatefile("${path.module}/policies/${each.value}", {
    DATA_BUCKET_NAME     = var.data_bucket_name
    AWS_REGION           = var.aws_region
    AWS_ACCOUNT_ID       = var.expected_account_id
    PROJECT_NAME         = var.project_name
    SECRET_NAMES         = local.secret_names
    MANAGED_SECRET_NAMES = local.managed_secret_names
    READER_ROLE_NAME     = var.lakehouse_reader_role_name
    WRITER_ROLE_NAME     = var.lakehouse_writer_role_name
  })

  lifecycle {
    prevent_destroy = true

    precondition {
      condition     = length(distinct(concat(local.secret_names, local.managed_secret_names))) == length(local.secret_names) + length(local.managed_secret_names)
      error_message = "Each secret name must be different."
    }
  }
}

resource "aws_iam_user_policy_attachment" "service" {
  for_each   = aws_iam_policy.service
  user       = var.aws_profile
  policy_arn = each.value.arn
}

provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
  # The layer of each catalog writer role. Only these four writers run a job; the role of a job is never another layer's.
  writer_role_layers = {
    write_silver_task   = "silver"
    write_gold_task     = "gold"
    write_features_task = "features"
    write_bronze_task   = "bronze"
  }
  writer_parameters = {
    for role, names in var.writer_parameter_names : role => [
      { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${try(names[0], "invalid")}" },
      { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${try(names[1], "invalid")}" }
    ]
  }
  reader_parameters = [
    { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.aws_polaris_reader_client_id_name}" },
    { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.aws_polaris_reader_client_secret_name}" }
  ]
  # Attributes the provider assigns at apply, which the shared flow module's task settings leave out.
  task_attributes_assigned_at_apply = ["arn", "arn_without_revision", "id", "region", "revision", "tags_all"]
}

variable "host_catalog_setup_prepared" {
  description = "Prepare the task role of the catalog setup one-shot on the container host, which sets up the AWS Polaris catalog and the fourteen pair parameters from inside the private network (scripts/lakehouse/aws_catalog_setup.py, launched by host_session catalog-setup). Off by default."
  type        = bool
  default     = false
  nullable    = false
}

locals {
  # The fourteen AWS Polaris pair parameters, the client ID and the secret of each of the seven identities. The task role reads and writes exactly these.
  catalog_setup_pair_names = [
    var.aws_polaris_reader_client_id_name, var.aws_polaris_reader_client_secret_name,
    var.aws_polaris_e2e_client_id_name, var.aws_polaris_e2e_client_secret_name,
    var.aws_polaris_bronze_client_id_name, var.aws_polaris_bronze_client_secret_name,
    var.aws_polaris_silver_client_id_name, var.aws_polaris_silver_client_secret_name,
    var.aws_polaris_gold_client_id_name, var.aws_polaris_gold_client_secret_name,
    var.aws_polaris_features_client_id_name, var.aws_polaris_features_client_secret_name,
    var.aws_polaris_serving_client_id_name, var.aws_polaris_serving_client_secret_name,
  ]
  catalog_setup_ssm_arn      = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter"
  catalog_setup_records      = "arn:aws:s3:::${var.data_bucket_name}/lakehouse/catalog_setup"
  catalog_setup_record_scope = "lakehouse/catalog_setup/"
}

# The catalog setup one-shot sets up the AWS catalog and each identity's roles, grants and pair, and nothing else: the pair parameters, its own S3 records (write-once approvals,
# leased locks, pending-write marks), the services apply lock that every host launch honours, and the listing of the project's tasks that proves no job is live. It holds no grant
# on the Polaris root secret (the execution role injects it), no read of the data, no write outside lakehouse/catalog_setup/ and no other service.
resource "aws_iam_role" "host_catalog_setup_task" {
  count = var.host_catalog_setup_prepared ? 1 : 0

  name                 = format(local.platform_name, "host_catalog_setup_task")
  description          = "Catalog setup one-shot on the container host: the AWS Polaris pair parameters, its own S3 records, the services apply lock and the task listing, and nothing else."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "host_catalog_setup_task" {
  count = var.host_catalog_setup_prepared ? 1 : 0

  name = "catalog_setup"
  role = aws_iam_role.host_catalog_setup_task[0].name
  policy = templatefile("${path.module}/owner_policies/host_catalog_setup.json", {
    PAIR_ARNS    = jsonencode([for name in local.catalog_setup_pair_names : "${local.catalog_setup_ssm_arn}${name}"])
    HOLD_ARN     = "${local.catalog_setup_ssm_arn}${var.services_apply_lock_parameter_name}"
    RECORDS_ARN  = local.catalog_setup_records
    BUCKET_ARN   = "arn:aws:s3:::${var.data_bucket_name}"
    RECORD_SCOPE = local.catalog_setup_record_scope
    CLUSTER_ARN  = "${local.ecs_arn}:cluster/${local.cluster_name}"
    TASK_ARN     = "${local.ecs_arn}:task/${local.cluster_name}/*"
  })
}

output "host_catalog_setup_role" {
  description = "The catalog setup one-shot's task role name and ARN. Null until host_catalog_setup_prepared is true. The services root takes the ARN as catalog_setup_task_role_arn."
  value       = var.host_catalog_setup_prepared ? { name = aws_iam_role.host_catalog_setup_task[0].name, arn = aws_iam_role.host_catalog_setup_task[0].arn } : null
}

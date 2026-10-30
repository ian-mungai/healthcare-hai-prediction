locals {
  athena_mirror_config = jsondecode(file("${path.module}/../../config/lakehouse/athena_mirror.json"))
  # The mirror covers the gold layer only: every table the gold publish writes, and no other layer or namespace.
  athena_layers        = local.athena_mirror_config.layers
  athena_account_arn   = "arn:aws:glue:${var.aws_region}:${var.expected_account_id}"
  athena_catalog_arn   = "${local.athena_account_arn}:catalog"
  athena_results_path  = "${local.athena_mirror_config.workgroup.results_prefix}*"
  athena_workgroup_arn = "arn:aws:athena:${var.aws_region}:${var.expected_account_id}:workgroup/${local.athena_mirror_config.workgroup.name}"
  athena_table_arns    = { for layer, spec in local.athena_layers : layer => "${local.athena_account_arn}:table/${spec.database}/*" }
  athena_table_folders = [for layer, spec in local.athena_layers : "lakehouse/${spec.namespace}/*"]
}

resource "aws_glue_catalog_database" "athena_mirror" {
  for_each = var.athena_mirror_prepared ? local.athena_layers : {}

  name        = each.value.database
  description = "Read-only Glue registrations of every published ${each.key} table, Public and Confidential. Polaris is the catalog and the source of truth."
  tags        = merge(local.project_tags, { DataClassification = "Confidential" })
}

resource "aws_athena_workgroup" "athena_mirror" {
  count = var.athena_mirror_prepared ? 1 : 0

  name          = local.athena_mirror_config.workgroup.name
  description   = "Queries over the Glue mirror of published tables. A query that scans past the configured bytes is cancelled."
  force_destroy = true
  tags          = local.project_tags

  lifecycle {
    precondition {
      condition     = local.athena_mirror_config.workgroup.bytes_scanned_limit >= 10485760
      error_message = "Athena accepts a per-query limit of 10 MB or more."
    }
  }

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = local.athena_mirror_config.workgroup.bytes_scanned_limit

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.data_bucket_name}/${local.athena_mirror_config.workgroup.results_prefix}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_iam_role_policy" "athena_mirror_writer" {
  for_each = var.athena_mirror_prepared && var.job_writer_layers_prepared ? { for layer, spec in local.athena_layers : layer => spec if contains(keys(local.job_writer_layer_definitions), layer) } : {}

  name = "glue_mirror_registrations"
  role = aws_iam_role.job_write_layer_task[each.key].name
  policy = templatefile("${path.module}/owner_policies/job_writer_glue_mirror.json", {
    CATALOG_ARN       = local.athena_catalog_arn
    DATABASE_ARN      = "${local.athena_account_arn}:database/${each.value.database}"
    REGISTRATION_ARNS = jsonencode([local.athena_catalog_arn, "${local.athena_account_arn}:database/${each.value.database}", local.athena_table_arns[each.key]])
  })
}

# The owner's one way in. The role trusts the one user the owner names and carries the reader policy inline. The stack attaches nothing to a user or
# a group, and the policy names no principal.
resource "aws_iam_role" "athena_reader" {
  count = var.athena_mirror_prepared && var.athena_owner_user != "" ? 1 : 0

  name                 = format(local.platform_name, "athena_reader")
  description          = "Athena review of the gold mirror for the owner's user only."
  max_session_duration = 3600
  tags                 = local.project_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = "arn:aws:iam::${var.expected_account_id}:user/${var.athena_owner_user}" }
    }]
  })
}

resource "aws_iam_role_policy" "athena_reader" {
  count = var.athena_mirror_prepared && var.athena_owner_user != "" ? 1 : 0

  name = "gold_mirror_review"
  role = aws_iam_role.athena_reader[0].name
  policy = templatefile("${path.module}/owner_policies/athena_reader.json", {
    WORKGROUP_ARN      = local.athena_workgroup_arn
    GLUE_ARNS          = jsonencode(concat([local.athena_catalog_arn], [for layer, spec in local.athena_layers : "${local.athena_account_arn}:database/${spec.database}"], values(local.athena_table_arns)))
    TABLE_OBJECT_ARNS  = jsonencode([for folder in local.athena_table_folders : "${local.bronze_bucket_arn}/${folder}"])
    LIST_PREFIXES      = jsonencode(concat(local.athena_table_folders, [local.athena_results_path]))
    BUCKET_ARN         = local.bronze_bucket_arn
    RESULTS_OBJECT_ARN = "${local.bronze_bucket_arn}/${local.athena_results_path}"
  })
}

output "athena_mirror_contract" {
  description = "The Athena mirror: whether it is prepared, the database and workgroup names and the one reader role, which trusts only the owner user."
  value = {
    prepared            = var.athena_mirror_prepared
    databases           = { for layer, spec in local.athena_layers : layer => spec.database }
    workgroup           = local.athena_mirror_config.workgroup.name
    bytes_scanned_limit = local.athena_mirror_config.workgroup.bytes_scanned_limit
    reader_role_arn     = try(aws_iam_role.athena_reader[0].arn, null)
    writer_layers       = sort(keys(local.athena_layers))
    owner_user          = var.athena_owner_user
  }
}

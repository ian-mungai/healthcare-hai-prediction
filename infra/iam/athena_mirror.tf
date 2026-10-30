locals {
  athena_mirror_config = jsondecode(file("${path.module}/../../config/lakehouse/athena_mirror.json"))
  # The mirror covers the gold layer only: every table the gold publish writes, and no other layer or namespace.
  athena_layers        = local.athena_mirror_config.layers
  athena_account_arn   = "arn:aws:glue:${var.aws_region}:${var.expected_account_id}"
  athena_catalog_arn   = "${local.athena_account_arn}:catalog"
  athena_results_path  = "${local.athena_mirror_config.workgroup.results_prefix}*"
  athena_workgroup_arn = "arn:aws:athena:${var.aws_region}:${var.expected_account_id}:workgroup/${local.athena_mirror_config.workgroup.name}"
  athena_table_arns    = { for layer, spec in local.athena_layers : layer => "${local.athena_account_arn}:table/${spec.database}/*" }
  # The reader reads the folders of the 70 tables that the gold publish contract names, and nothing else of the namespace: not publish_runs and not a
  # table that is not in the contract. The folder pattern is the table name plus a wildcard, so a table folder with a suffix still matches.
  athena_contract_tables = sort(keys(jsondecode(file("${path.module}/../../data_contracts/lakehouse/gold_contract.json")).publish.tables))
  athena_table_folders   = flatten([for layer, spec in local.athena_layers : [for table in local.athena_contract_tables : "lakehouse/${spec.namespace}/${table}*"]])
  athena_glue_endpoint   = var.athena_mirror_prepared && var.job_endpoints_enabled
  athena_reader_policy = templatefile("${path.module}/owner_policies/athena_reader.json", {
    WORKGROUP_ARN      = local.athena_workgroup_arn
    GLUE_ARNS          = jsonencode(concat([local.athena_catalog_arn], [for layer, spec in local.athena_layers : "${local.athena_account_arn}:database/${spec.database}"], values(local.athena_table_arns)))
    TABLE_OBJECT_ARNS  = jsonencode([for folder in local.athena_table_folders : "${local.bronze_bucket_arn}/${folder}"])
    LIST_PREFIXES      = jsonencode(concat(local.athena_table_folders, [local.athena_results_path]))
    BUCKET_ARN         = local.bronze_bucket_arn
    RESULTS_OBJECT_ARN = "${local.bronze_bucket_arn}/${local.athena_results_path}"
  })
  athena_gold_writer_arn = "arn:aws:iam::${var.expected_account_id}:role/${format(local.platform_name, "job_write_gold_task")}"
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

  name   = "gold_mirror_review"
  role   = aws_iam_role.athena_reader[0].name
  policy = local.athena_reader_policy

  lifecycle {
    precondition {
      condition     = length(replace(replace(local.athena_reader_policy, " ", ""), "\n", "")) <= 10240
      error_message = "The reader policy is over the 10240 character limit of an inline role policy. Move the table folders into a managed policy."
    }
  }
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

# The private gold writer has no route to the public Glue API. This endpoint is the one way, restricted to the gold writer role and the four mirror
# actions on the mirror database. It follows the reviewed private endpoint switch.
resource "aws_security_group" "athena_glue_endpoint" {
  count = local.athena_glue_endpoint ? 1 : 0

  name        = format(local.platform_name, "athena_glue_endpoint")
  description = "Glue endpoint for the Athena mirror: HTTPS from the lakehouse job group only."
  vpc_id      = aws_vpc.platform.id
  tags        = merge(local.project_tags, { Name = format(local.platform_name, "athena_glue_endpoint") })
}

resource "aws_vpc_endpoint" "athena_glue" {
  count = local.athena_glue_endpoint ? 1 : 0

  vpc_id              = aws_vpc.platform.id
  service_name        = "com.amazonaws.${var.aws_region}.glue"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.public[0].id]
  security_group_ids  = [aws_security_group.athena_glue_endpoint[0].id]
  tags                = merge(local.project_tags, { Name = format(local.platform_name, "athena_glue") })
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
      Resource  = concat([local.athena_catalog_arn], [for layer, spec in local.athena_layers : "${local.athena_account_arn}:database/${spec.database}"], values(local.athena_table_arns))
      Condition = { ArnEquals = { "aws:PrincipalArn" = local.athena_gold_writer_arn } }
    }]
  })
}

resource "aws_vpc_security_group_ingress_rule" "athena_glue_from_lakehouse" {
  count = local.athena_glue_endpoint ? 1 : 0

  security_group_id            = aws_security_group.athena_glue_endpoint[0].id
  description                  = "Glue HTTPS from the lakehouse job group only"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.job["lakehouse"].id
}

resource "aws_vpc_security_group_egress_rule" "athena_glue_from_lakehouse" {
  count = local.athena_glue_endpoint ? 1 : 0

  security_group_id            = aws_security_group.job["lakehouse"].id
  description                  = "Lakehouse HTTPS to the Glue endpoint"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.athena_glue_endpoint[0].id
}

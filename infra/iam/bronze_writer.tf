locals {
  bronze_writer_role_name = format(local.platform_name, "job_write_bronze_task")
  bronze_bucket_arn       = "arn:aws:s3:::${var.data_bucket_name}"
  bronze_namespaces       = ["bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check"]
  bronze_test_namespaces  = [for name in local.bronze_namespaces : "${name}_e2e"]
  # The bronze job writes its two namespaces for a real run and their two test twins for a qualification run.
  bronze_job_twins       = ["bronze_e2e", "bronze_dictionary_e2e"]
  bronze_table_map       = jsondecode(file("${path.module}/../../config/lakehouse/bronze_tables.json"))
  bronze_collections     = sort(distinct([for table in local.bronze_table_map.tables : table.collection]))
  bronze_parameter_names = [var.aws_polaris_bronze_client_id_name, var.aws_polaris_bronze_client_secret_name]
  bronze_parameter_arns  = [for name in local.bronze_parameter_names : "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${name}"]
  bronze_storage_inputs = {
    BUCKET_ARN           = local.bronze_bucket_arn
    TABLE_OBJECT_ARNS    = jsonencode([for name in concat(local.bronze_namespaces, local.bronze_job_twins) : "${local.bronze_bucket_arn}/lakehouse/${name}/*"])
    TABLE_LIST_PREFIXES  = jsonencode([for name in concat(local.bronze_namespaces, local.bronze_job_twins) : "lakehouse/${name}/*"])
    SOURCE_OBJECT_ARNS   = jsonencode([for collection in local.bronze_collections : "${local.bronze_bucket_arn}/${collection}/*"])
    SOURCE_LIST_PREFIXES = jsonencode([for collection in local.bronze_collections : "${collection}/*"])
  }
  bronze_storage_policy = templatefile("${path.module}/owner_policies/bronze_writer_storage.json", local.bronze_storage_inputs)
}

resource "aws_iam_role" "bronze_writer" {
  count = var.bronze_writer_prepared ? 1 : 0

  name                 = local.bronze_writer_role_name
  description          = "Bronze, dictionary and scratch task access with versioned source reads. Catalog writes remain separately gated."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags

  lifecycle {
    precondition {
      condition = var.bronze_live_locations == tomap({
        bronze            = "s3://${var.data_bucket_name}/lakehouse/bronze/"
        bronze_dictionary = "s3://${var.data_bucket_name}/lakehouse/bronze_dictionary/"
      })
      error_message = "Read the two live bronze and dictionary namespace locations before presenting preparation. Either namespace location outside the expected prefixes blocks this scope."
    }

    precondition {
      condition     = local.bronze_table_map.namespace == "bronze" && contains(local.bronze_collections, var.bronze_qualification_collection)
      error_message = "The table map must name bronze and the qualification source must be one mapped collection."
    }

    precondition {
      condition     = length(distinct(local.job_writer_credential_names)) == length(local.job_writer_credential_names)
      error_message = "Every AWS reader and writer pair must be distinct from every other pair and the configured Mac reader, writers and administrator."
    }
  }
}

resource "aws_iam_role_policy" "bronze_writer" {
  count = var.bronze_writer_prepared ? 1 : 0

  name   = "bronze_objects_and_versioned_sources"
  role   = aws_iam_role.bronze_writer[0].name
  policy = local.bronze_storage_policy
}

output "bronze_writer_contract" {
  description = "Inactive bronze identity and the reviewed contract for integration into the writer IAM batch and target file."
  value = {
    principal                = "bronze_writer"
    task_role_key            = "job_write_bronze_task"
    task_role_name           = local.bronze_writer_role_name
    task_role_arn            = try(aws_iam_role.bronze_writer[0].arn, null)
    parameter_names          = local.bronze_parameter_names
    source_collections       = local.bronze_collections
    qualification_collection = var.bronze_qualification_collection
    namespaces               = local.bronze_namespaces
    test_namespaces          = local.bronze_test_namespaces
    production_layer         = "off"
    credentials_owner        = "owner_administrator_profile"
    catalog_grant_contract   = jsondecode(file("${path.module}/../../config/lakehouse/aws_bronze_grants.json"))
  }
}

output "bronze_execution_delta" {
  description = "Bronze slice of the job execution grants attached when bronze is prepared. Only ECS injects this pair."
  value = jsondecode(templatefile("${path.module}/owner_policies/bronze_execution.json", {
    BRONZE_PARAMETER_ARNS = jsonencode(local.bronze_parameter_arns)
  }))
}

output "bronze_project_delta" {
  description = "Bronze slice of the project user's ECS-only writer PassRole policy. The project profile receives no writer-parameter read."
  value = jsondecode(templatefile("${path.module}/owner_policies/bronze_project_delta.json", {
    BRONZE_TASK_ROLE_ARN = jsonencode("arn:aws:iam::${var.expected_account_id}:role/${local.bronze_writer_role_name}")
  }))
}

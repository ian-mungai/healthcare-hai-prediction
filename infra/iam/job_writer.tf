locals {
  job_writer_e2e_namespaces = concat(["silver_e2e", "gold_e2e", "features_e2e"], local.bronze_test_namespaces)
  job_writer_e2e_parameters = [var.aws_polaris_e2e_client_id_name, var.aws_polaris_e2e_client_secret_name]
  job_writer_e2e_inputs = {
    BUCKET_ARN                = local.bronze_bucket_arn
    TEST_OBJECT_ARNS          = jsonencode([for name in local.job_writer_e2e_namespaces : "${local.bronze_bucket_arn}/lakehouse/${name}/*"])
    TEST_LIST_PREFIXES        = jsonencode([for name in local.job_writer_e2e_namespaces : "lakehouse/${name}/*"])
    BRONZE_TEST_OBJECT_ARNS   = jsonencode([for name in local.bronze_test_namespaces : "${local.bronze_bucket_arn}/lakehouse/${name}/*"])
    BRONZE_TEST_LIST_PREFIXES = jsonencode([for name in local.bronze_test_namespaces : "lakehouse/${name}/*"])
    SOURCE_OBJECT_ARNS        = jsonencode(["${local.bronze_bucket_arn}/${var.bronze_qualification_collection}/*"])
    SOURCE_LIST_PREFIXES      = jsonencode(["${var.bronze_qualification_collection}/*"])
    E2E_PARAMETER_ARNS        = jsonencode([for name in local.job_writer_e2e_parameters : "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${name}"])
  }
}

resource "aws_iam_role" "job_write_e2e_task" {
  count = var.job_writer_e2e_prepared ? 1 : 0

  name                 = format(local.platform_name, "job_write_e2e_task")
  description          = "Qualification writer: seven test namespaces and versioned reads of one selected source collection."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags

  lifecycle {
    precondition {
      condition     = local.bronze_table_map.namespace == "bronze" && contains(local.bronze_collections, var.bronze_qualification_collection)
      error_message = "Test writer preparation requires one qualification source collection from the bronze table map."
    }

    precondition {
      condition = alltrue([
        for name in local.job_writer_e2e_parameters : !contains([
          var.aws_polaris_reader_client_id_name, var.aws_polaris_reader_client_secret_name,
          var.polaris_root_secret_name, var.polaris_reader_client_id_name, var.polaris_reader_client_secret_name,
          var.aws_polaris_bronze_client_id_name, var.aws_polaris_bronze_client_secret_name
        ], name)
      ])
      error_message = "The test writer pair must be separate from every configured reader, administrator and bronze writer credential."
    }
  }
}

resource "aws_iam_role_policy" "job_writer_e2e_storage" {
  count = var.job_writer_e2e_prepared ? 1 : 0

  name   = "qualification_objects_and_versioned_source"
  role   = aws_iam_role.job_write_e2e_task[0].name
  policy = templatefile("${path.module}/owner_policies/job_writer_e2e_storage.json", local.job_writer_e2e_inputs)
}

resource "aws_iam_role_policy" "job_writer_e2e_stage" {
  count = var.job_writer_e2e_prepared ? 1 : 0

  name   = "stage_read"
  role   = aws_iam_role.job_write_e2e_task[0].name
  policy = local.job_policy_documents.job_stage_read
}

resource "aws_iam_role_policy" "job_reader_e2e" {
  count = var.job_writer_e2e_prepared ? 1 : 0

  name   = "qualification_read"
  role   = aws_iam_role.job["read_task"].name
  policy = templatefile("${path.module}/owner_policies/job_reader_e2e.json", local.job_writer_e2e_inputs)
}

resource "aws_iam_role_policy" "job_writer_e2e_execution" {
  count = var.job_writer_e2e_prepared ? 1 : 0

  name   = "inject_qualification_pair"
  role   = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/job_writer_e2e_execution.json", local.job_writer_e2e_inputs)
}

output "job_writer_e2e_contract" {
  description = "Prepared qualification identity, scoped parameter names and source selection. Catalog admission remains separate."
  value = {
    prepared              = var.job_writer_e2e_prepared
    principal             = "e2e_writer"
    task_role             = try({ name = aws_iam_role.job_write_e2e_task[0].name, arn = aws_iam_role.job_write_e2e_task[0].arn, unique_id = aws_iam_role.job_write_e2e_task[0].unique_id }, null)
    parameter_names       = local.job_writer_e2e_parameters
    namespaces            = local.job_writer_e2e_namespaces
    source_collection     = var.bronze_qualification_collection
    production_activation = false
  }
}

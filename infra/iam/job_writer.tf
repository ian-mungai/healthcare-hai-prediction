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
      condition     = length(distinct(local.job_writer_credential_names)) == length(local.job_writer_credential_names)
      error_message = "Every AWS reader and writer pair must be distinct from every other pair and the configured Mac reader, writers and administrator."
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

locals {
  job_writer_layer_definitions = {
    silver = {
      prefixes        = ["silver"]
      parameter_names = [var.aws_polaris_silver_client_id_name, var.aws_polaris_silver_client_secret_name]
    }
    gold = {
      prefixes        = ["gold", "catalog_backups"]
      parameter_names = [var.aws_polaris_gold_client_id_name, var.aws_polaris_gold_client_secret_name]
    }
    features = {
      prefixes        = ["features"]
      parameter_names = [var.aws_polaris_features_client_id_name, var.aws_polaris_features_client_secret_name]
    }
  }
  job_writer_credential_names = concat(
    [var.aws_polaris_reader_client_id_name, var.aws_polaris_reader_client_secret_name],
    local.job_writer_e2e_parameters,
    flatten([for layer in ["silver", "gold", "features"] : local.job_writer_layer_definitions[layer].parameter_names]),
    local.bronze_parameter_names,
    [
      var.polaris_root_secret_name,
      var.polaris_reader_client_id_name, var.polaris_reader_client_secret_name,
      var.polaris_silver_client_id_name, var.polaris_silver_client_secret_name,
      var.polaris_gold_client_id_name, var.polaris_gold_client_secret_name
    ]
  )
  job_writer_production_parameters = concat(
    var.job_writer_layers_prepared ? flatten([for layer in ["silver", "gold", "features"] : local.job_writer_layer_definitions[layer].parameter_names]) : [],
    var.bronze_writer_prepared ? local.bronze_parameter_names : []
  )
  job_writer_production_roles = merge(
    { for layer, role in aws_iam_role.job_write_layer_task : layer => role.name },
    { for role in aws_iam_role.bronze_writer : "bronze" => role.name }
  )
}

resource "aws_iam_role" "job_write_layer_task" {
  for_each = var.job_writer_layers_prepared ? local.job_writer_layer_definitions : {}

  name                 = format(local.platform_name, "job_write_${each.key}_task")
  description          = "${title(each.key)} table and backup access. Catalog writes remain separately gated."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags

  lifecycle {
    precondition {
      condition     = length(distinct(local.job_writer_credential_names)) == length(local.job_writer_credential_names)
      error_message = "Every AWS reader and writer pair must be distinct from every other pair and the configured Mac reader, writers and administrator."
    }
  }
}

resource "aws_iam_role_policy" "job_writer_layer_storage" {
  for_each = aws_iam_role.job_write_layer_task

  name = "layer_tables_and_backups"
  role = each.value.name
  policy = templatefile("${path.module}/owner_policies/job_writer_layer_storage.json", {
    BUCKET_ARN          = local.bronze_bucket_arn
    TABLE_OBJECT_ARNS   = jsonencode([for prefix in local.job_writer_layer_definitions[each.key].prefixes : "${local.bronze_bucket_arn}/lakehouse/${prefix}/*"])
    TABLE_LIST_PREFIXES = jsonencode([for prefix in local.job_writer_layer_definitions[each.key].prefixes : "lakehouse/${prefix}/*"])
  })
}

resource "aws_iam_role_policy" "job_writer_stage" {
  for_each = local.job_writer_production_roles

  name   = "stage_read"
  role   = each.value
  policy = local.job_policy_documents.job_stage_read
}

resource "aws_iam_role_policy" "job_writer_execution" {
  count = length(local.job_writer_production_parameters) > 0 ? 1 : 0

  name = "inject_production_writer_pairs"
  role = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/job_writer_execution.json", {
    WRITER_PARAMETER_ARNS = jsonencode([for name in local.job_writer_production_parameters : "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${name}"])
  })
}

output "job_writer_layer_contract" {
  description = "Prepared production roles and pair names with activation off. Fence inputs are requests only and attach no fence statements in this batch."
  value = {
    prepared = {
      silver   = var.job_writer_layers_prepared
      gold     = var.job_writer_layers_prepared
      features = var.job_writer_layers_prepared
      bronze   = var.bronze_writer_prepared
    }
    parameter_names       = merge({ for layer, definition in local.job_writer_layer_definitions : layer => definition.parameter_names }, { bronze = local.bronze_parameter_names })
    production_activation = false
    fence_requests = {
      catalog_cutover_fenced = var.catalog_cutover_fenced
      aws_writers_fenced     = var.aws_writers_fenced
    }
  }
}

locals {
  job_layer_run_inputs         = { silver = "bronze", gold = "silver", features = "gold" }
  job_layer_run_upload_layers  = ["silver", "gold"]
  job_layer_run_registry_key   = "lakehouse/feast/registry.pb"
  job_layer_run_prepared_roles = var.job_layer_runs_prepared ? aws_iam_role.job_write_layer_task : {}
}

resource "aws_iam_role_policy" "job_writer_layer_input" {
  for_each = local.job_layer_run_prepared_roles

  name = "layer_input_read"
  role = each.value.name
  policy = templatefile("${path.module}/owner_policies/job_writer_layer_input.json", {
    BUCKET_ARN          = local.bronze_bucket_arn
    INPUT_OBJECT_ARNS   = jsonencode(["${local.bronze_bucket_arn}/lakehouse/${local.job_layer_run_inputs[each.key]}/*"])
    INPUT_LIST_PREFIXES = jsonencode(["lakehouse/${local.job_layer_run_inputs[each.key]}/*"])
  })
}

resource "aws_iam_role_policy" "job_writer_layer_upload" {
  for_each = { for layer, role in local.job_layer_run_prepared_roles : layer => role if contains(local.job_layer_run_upload_layers, layer) }

  name = "layer_run_upload"
  role = each.value.name
  policy = templatefile("${path.module}/owner_policies/job_writer_layer_upload.json", {
    UPLOAD_OBJECT_ARNS = jsonencode(["${local.bronze_bucket_arn}/lakehouse/layer_runs/${each.key}/*"])
  })
}

resource "aws_iam_role_policy" "job_writer_layer_state" {
  for_each = { for layer, role in local.job_layer_run_prepared_roles : layer => role if layer == "features" }

  name = "layer_run_state"
  role = each.value.name
  policy = templatefile("${path.module}/owner_policies/job_writer_layer_upload.json", {
    UPLOAD_OBJECT_ARNS = jsonencode(["${local.bronze_bucket_arn}/lakehouse/layer_runs/${each.key}/*/state/*"])
  })
}

resource "aws_iam_role_policy" "job_writer_features_feast" {
  for_each = { for layer, role in local.job_layer_run_prepared_roles : layer => role if layer == "features" }

  name = "feast_registry_and_redis_password"
  role = each.value.name
  policy = templatefile("${path.module}/owner_policies/job_writer_features_feast.json", {
    REGISTRY_OBJECT_ARNS = jsonencode(["${local.bronze_bucket_arn}/${local.job_layer_run_registry_key}"])
    REDIS_PARAMETER_ARNS = jsonencode(["arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.redis_password_name}"])
  })
}

output "job_layer_runs_contract" {
  description = "Prepared run grants of the silver, gold and features jobs: the input layer each reads and the upload prefix of each layer that uploads. Activation stays off."
  value = {
    prepared              = var.job_layer_runs_prepared
    inputs                = var.job_layer_runs_prepared ? local.job_layer_run_inputs : {}
    uploads               = var.job_layer_runs_prepared ? { for layer in local.job_layer_run_upload_layers : layer => "lakehouse/layer_runs/${layer}/" } : {}
    production_activation = false
  }
}

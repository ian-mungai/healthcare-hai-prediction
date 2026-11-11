locals {
  bronze_writer_role_name = format(local.platform_name, "job_write_bronze_task")
  bronze_bucket_arn       = "arn:aws:s3:::${var.data_bucket_name}"
  bronze_namespaces       = ["bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check"]
  bronze_test_namespaces  = [for name in local.bronze_namespaces : "${name}_e2e"]
  # The bronze job writes its two namespaces for a real run and their two test twins for a qualification run.
  bronze_job_twins       = ["bronze_e2e", "bronze_dictionary_e2e"]
  bronze_table_map       = jsondecode(file("${path.module}/../../config/lakehouse/bronze_tables.json"))
  bronze_collections     = sort(distinct([for table in local.bronze_table_map.tables : table.collection]))
  bronze_source_roots    = var.source_raw_root_enabled ? ["", "raw/"] : [""]
  bronze_parameter_names = [var.aws_polaris_bronze_client_id_name, var.aws_polaris_bronze_client_secret_name]
  bronze_parameter_arns  = [for name in local.bronze_parameter_names : "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${name}"]
  # The loader reads the pseudonym key of the version its reviewed person-field policy names, at use, with this task role (no ECS secret injection).
  bronze_pseudonym_key_arn = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.pseudonym_key_prefix}${jsondecode(file("${path.module}/../../config/lakehouse/person_policies.json")).key_version}"
  bronze_storage_inputs = {
    BUCKET_ARN          = local.bronze_bucket_arn
    TABLE_OBJECT_ARNS   = jsonencode([for name in concat(local.bronze_namespaces, local.bronze_job_twins) : "${local.bronze_bucket_arn}/lakehouse/${name}/*"])
    TABLE_LIST_PREFIXES = jsonencode([for name in concat(local.bronze_namespaces, local.bronze_job_twins) : "lakehouse/${name}/*"])
  }
  bronze_storage_policy = templatefile("${path.module}/owner_policies/bronze_writer_storage.json", local.bronze_storage_inputs)
  # IAM caps all inline policies of one role together at 10,240 characters and each managed policy at 6,144 (whitespace excluded). The source reads grow with
  # the bucket name length and the raw root, so they live in managed policies, one object policy per root. Table statements stay inline.
  bronze_managed_policies = merge(
    {
      pseudonym_key = {
        description = "Read the one pseudonym key parameter of the reviewed key version, at load. No other parameter and no write."
        document    = templatefile("${path.module}/owner_policies/bronze_writer_pseudonym_key.json", { PSEUDONYM_KEY_ARN = local.bronze_pseudonym_key_arn })
      }
      source_lists = {
        description = "List source versions of each mapped collection at every enabled source root."
        document = templatefile("${path.module}/owner_policies/bronze_writer_source_lists.json", {
          BUCKET_ARN           = local.bronze_bucket_arn
          SOURCE_LIST_PREFIXES = jsonencode(flatten([for root in local.bronze_source_roots : [for collection in local.bronze_collections : "${root}${collection}/*"]]))
        })
      }
    },
    {
      # The single-copy collector stores payloads at bronze_objects/sha256/<digest>. Manifests name them, so the loader reads them by exact version.
      for root in local.bronze_source_roots : "source_objects_${root == "" ? "root" : "raw"}" => {
        description = "Read source object versions of each mapped collection and the content-addressed payloads, ${root == "" ? "at the bucket root" : "under raw"}."
        document = templatefile("${path.module}/owner_policies/bronze_writer_source_objects.json", {
          SOURCE_OBJECT_ARNS = jsonencode(concat([for collection in local.bronze_collections : "${local.bronze_bucket_arn}/${root}${collection}/*"], ["${local.bronze_bucket_arn}/${root}bronze_objects/sha256/*"]))
        })
      }
    }
  )
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

  # Attach the managed source reads before the inline policy drops them, so the role keeps its reads through the apply.
  depends_on = [aws_iam_role_policy_attachment.bronze_writer]

  lifecycle {
    precondition {
      condition     = length(jsonencode(jsondecode(local.bronze_storage_policy))) + length(jsonencode(jsondecode(local.job_policy_documents.job_stage_read))) <= 10240
      error_message = "The inline policies of the bronze role together are over the 10240 character quota of one role. Move statements into a managed policy."
    }
  }
}

resource "aws_iam_policy" "bronze_writer" {
  for_each = var.bronze_writer_prepared ? local.bronze_managed_policies : {}

  name        = format(local.platform_name, "job_write_bronze_${each.key}")
  description = each.value.description
  policy      = each.value.document
  tags        = local.project_tags

  lifecycle {
    precondition {
      condition     = length(jsonencode(jsondecode(each.value.document))) <= 6144
      error_message = "A managed bronze policy is over the 6144 character quota of one managed policy. Split it."
    }
  }
}

resource "aws_iam_role_policy_attachment" "bronze_writer" {
  for_each = aws_iam_policy.bronze_writer

  role       = aws_iam_role.bronze_writer[0].name
  policy_arn = each.value.arn
}

output "bronze_writer_contract" {
  description = "Inactive bronze identity and the reviewed contract for integration into the writer IAM batch and target file."
  value = merge({
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
    }, {
    for key, value in { read_roots = local.bronze_source_roots, read_environment = { HAI_READ_RAW_ROOT = "true" } } : key => value
    if var.source_raw_root_enabled
  })
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

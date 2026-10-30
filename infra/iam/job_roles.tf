locals {
  job_role_names = {
    for name in ["execution", "offline_task", "seal_task", "read_task", "try_writer"] : name => format(local.platform_name, "job_${name}")
  }
  job_policy_inputs = merge(local.owner_policy_values, {
    DATA_BUCKET_NAME                      = var.data_bucket_name
    PROJECT_USER                          = var.aws_profile
    TASK_USER_ID                          = "$${aws:userid}"
    AWS_POLARIS_READER_CLIENT_ID_NAME     = var.aws_polaris_reader_client_id_name
    AWS_POLARIS_READER_CLIENT_SECRET_NAME = var.aws_polaris_reader_client_secret_name
  })
  job_policy_documents = {
    for name in ["job_stage_read", "job_source_read", "job_seal_deny", "job_try_writer", "job_execution", "job_try_trust"] :
    name => templatefile("${path.module}/owner_policies/${name}.json", local.job_policy_inputs)
  }
}

resource "aws_iam_role" "job" {
  for_each = local.job_role_names

  name                 = each.value
  description          = each.key == "try_writer" ? "Upper bound for staged try outputs." : "Private read jobs: ${each.key}."
  max_session_duration = each.key == "try_writer" ? 43200 : 3600
  assume_role_policy   = each.key == "try_writer" ? local.job_policy_documents.job_try_trust : local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "job_stage_read" {
  for_each = toset(["offline_task", "seal_task", "read_task"])

  name   = "stage_read"
  role   = aws_iam_role.job[each.key].name
  policy = local.job_policy_documents.job_stage_read
}

resource "aws_iam_role_policy" "job_source_read" {
  name   = "source_read"
  role   = aws_iam_role.job["read_task"].name
  policy = local.job_policy_documents.job_source_read
}

resource "aws_iam_role_policy" "job_seal_deny" {
  name   = "no_input_tree"
  role   = aws_iam_role.job["seal_task"].name
  policy = local.job_policy_documents.job_seal_deny
}

resource "aws_iam_role_policy" "job_execution" {
  name   = "job_execution"
  role   = aws_iam_role.job["execution"].name
  policy = local.job_policy_documents.job_execution

  lifecycle {
    precondition {
      condition     = var.aws_polaris_reader_client_id_name != var.aws_polaris_reader_client_secret_name
      error_message = "The reader client ID and secret must use different parameter names."
    }
  }
}

resource "aws_iam_role_policy" "job_try_writer" {
  name   = "try_outputs"
  role   = aws_iam_role.job["try_writer"].name
  policy = local.job_policy_documents.job_try_writer
}

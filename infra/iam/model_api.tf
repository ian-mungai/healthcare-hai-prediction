locals {
  # The prediction table lives in the serving namespace, which the catalog places under this prefix of the lakehouse. The catalog hands out no storage credentials
  # (stsUnavailable), so the API writes the table's data and manifest files itself, and the catalog service writes the table metadata with its own role.
  model_api_write_prefix = "lakehouse/serving/"
  model_api_policy_inputs = {
    BUCKET_ARN               = local.bronze_bucket_arn
    REGISTRY_OBJECT_ARNS     = jsonencode(["${local.bronze_bucket_arn}/${var.model_api_registry_key}"])
    MANIFEST_OBJECT_ARNS     = jsonencode(["${local.bronze_bucket_arn}/${var.model_api_manifest_prefix}/*"])
    MANIFEST_LIST_PREFIXES   = jsonencode(["${var.model_api_manifest_prefix}/*"])
    PREDICTION_OBJECT_ARNS   = jsonencode(["${local.bronze_bucket_arn}/${local.model_api_write_prefix}*"])
    PREDICTION_LIST_PREFIXES = jsonencode([local.model_api_write_prefix, "${local.model_api_write_prefix}*"])
  }
}

resource "aws_iam_role" "model_api_task" {
  count = var.model_api_prepared ? 1 : 0

  name                 = format(local.platform_name, "model_api_task")
  description          = "Private model API: reads the Feast registry and the deployment records. Writes only the prediction table objects, and only when model_api_prediction_writes is true."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "model_api_read" {
  count = var.model_api_prepared ? 1 : 0

  name   = "registry_and_deployments"
  role   = aws_iam_role.model_api_task[0].name
  policy = templatefile("${path.module}/owner_policies/model_api_read.json", local.model_api_policy_inputs)
}

resource "aws_iam_role_policy" "model_api_prediction_writes" {
  count = var.model_api_prepared && var.model_api_prediction_writes ? 1 : 0

  name   = "prediction_table_objects"
  role   = aws_iam_role.model_api_task[0].name
  policy = templatefile("${path.module}/owner_policies/model_api_prediction_writes.json", local.model_api_policy_inputs)
}

# The page calls no AWS service, so its role holds no policy at all. It exists to give the page a task role of its own, apart from the model API and the control plane.
resource "aws_iam_role" "streamlit_task" {
  count = var.model_api_prepared ? 1 : 0

  name                 = format(local.platform_name, "streamlit_task")
  description          = "Streamlit page: calls the model API over the host loopback and no AWS service. It holds no policy."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

output "model_api_contract" {
  description = "The prepared model API task role, whether it may write the prediction table objects and the single prefix those writes cover. Starting the service and the catalog principal stay separate."
  value = {
    prepared            = var.model_api_prepared
    prediction_writes   = var.model_api_prepared && var.model_api_prediction_writes
    write_prefix        = local.model_api_write_prefix
    task_role           = try({ name = aws_iam_role.model_api_task[0].name, arn = aws_iam_role.model_api_task[0].arn }, null)
    streamlit_task_role = try({ name = aws_iam_role.streamlit_task[0].name, arn = aws_iam_role.streamlit_task[0].arn }, null)
  }
}

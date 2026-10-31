variable "serve_prepared" {
  description = "Prepare the task role of the serve job that deploys a registered model version beside the production model. It reuses the training network, the training images and the MLflow login of the execution role, so apply it with training_prepared. Off by default."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.serve_prepared || var.training_prepared
    error_message = "The serve job runs in the training image, network and execution role grants, so set training_prepared to true first."
  }

  validation {
    condition     = !var.serve_prepared || var.model_api_manifest_prefix != ""
    error_message = "The serve job writes the deployment records, so set model_api_manifest_prefix to their key prefix first."
  }
}

locals {
  serve_policy_documents = {
    deployments = templatefile("${path.module}/owner_policies/job_serve_deployments.json", merge(local.job_policy_inputs, { MANIFEST_PREFIX = var.model_api_manifest_prefix }))
  }
  serve_policies = var.serve_prepared ? {
    stage_read  = local.job_policy_documents.job_stage_read
    deployments = local.serve_policy_documents.deployments
  } : {}
}

resource "aws_iam_role" "serve_task" {
  count = var.serve_prepared ? 1 : 0

  name                 = format(local.platform_name, "job_serve_task")
  description          = "Serve job: stage reads, and reads and creates the deployment records under their one prefix. It never deletes a record, writes the lakehouse or moves a model alias."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "serve_task" {
  for_each = local.serve_policies

  name   = each.key
  role   = aws_iam_role.serve_task[0].name
  policy = each.value
}

output "serve_roles" {
  description = "The prepared serve role name, ARN and unique ID. Empty until serve_prepared is true. The jobs root takes serve_task as the serve_roles entry of the same name."
  value       = { for role in aws_iam_role.serve_task : "serve_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
}

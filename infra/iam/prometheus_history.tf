variable "prometheus_history_prepared" {
  description = "Prepare the two task roles of Prometheus history in S3: the shipper that creates finished metric blocks under prometheus/blocks/ of the data bucket, and the restore one-shot that reads them back at host start. Off by default."
  type        = bool
  default     = false
  nullable    = false
}

locals {
  prometheus_history_policies = var.prometheus_history_prepared ? {
    shipper = templatefile("${path.module}/owner_policies/prometheus_history_ship.json", { DATA_BUCKET_NAME = var.data_bucket_name })
    restore = templatefile("${path.module}/owner_policies/prometheus_history_restore.json", { DATA_BUCKET_NAME = var.data_bucket_name })
  } : {}
}

# ECS gives every container of a task the task role, so the writer and the reader are two roles: the shipper is the task role of its own service and the restore
# is a one-shot with its own task. The shipper holds no read, delete or overwrite; the restore holds no write.
resource "aws_iam_role" "prometheus_history_task" {
  count = var.prometheus_history_prepared ? 1 : 0

  name                 = format(local.platform_name, "prometheus_history_task")
  description          = "Prometheus history shipper on the container host: creates each finished block once under prometheus/blocks/ of the data bucket and lists that prefix; no read, delete or overwrite."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "prometheus_history_task" {
  count = var.prometheus_history_prepared ? 1 : 0

  name   = "prometheus_blocks_create"
  role   = aws_iam_role.prometheus_history_task[0].name
  policy = local.prometheus_history_policies.shipper
}

resource "aws_iam_role" "prometheus_restore_task" {
  count = var.prometheus_history_prepared ? 1 : 0

  name                 = format(local.platform_name, "prometheus_restore_task")
  description          = "Prometheus history restore one-shot on the container host: reads and lists prometheus/blocks/ of the data bucket and nothing else."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "prometheus_restore_task" {
  count = var.prometheus_history_prepared ? 1 : 0

  name   = "prometheus_blocks_read"
  role   = aws_iam_role.prometheus_restore_task[0].name
  policy = local.prometheus_history_policies.restore
}

output "prometheus_history_roles" {
  description = "The names and ARNs of the Prometheus history shipper and restore task roles. Null until prometheus_history_prepared is true. The services root takes them as prometheus_history_task_role_arn and prometheus_restore_task_role_arn."
  value = var.prometheus_history_prepared ? {
    shipper = { name = aws_iam_role.prometheus_history_task[0].name, arn = aws_iam_role.prometheus_history_task[0].arn }
    restore = { name = aws_iam_role.prometheus_restore_task[0].name, arn = aws_iam_role.prometheus_restore_task[0].arn }
  } : null
}

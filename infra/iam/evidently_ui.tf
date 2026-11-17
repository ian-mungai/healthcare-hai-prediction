variable "evidently_ui_prepared" {
  description = "Prepare the task role of the Evidently UI service on the container host. The UI shows the snapshots of the on-demand live drift check from a read-only host folder. Its restore step rebuilds that folder from S3, so the role may read and list drift_demo/ and holds no other grant. Off by default."
  type        = bool
  default     = false
  nullable    = false
}

# The UI process calls no AWS service. Its restore step reads the completed live runs under drift_demo/ to rebuild the UI workspace at service start, so the role holds
# exactly one policy: read objects and list that prefix. It has no write, no delete and no other prefix: monitoring.live_cases allows no other policy-attaching resource to
# name this role and plants grants to prove the check, and evidently_ui_policy_cases plants write access and wider prefixes against the policy itself.
resource "aws_iam_role" "evidently_ui_task" {
  count = var.evidently_ui_prepared ? 1 : 0

  name                 = format(local.platform_name, "evidently_ui_task")
  description          = "Evidently UI on the container host: serves the live drift check snapshots from a read-only host folder. Its restore step reads and lists drift_demo/ and nothing else."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "evidently_ui_restore_read" {
  count = var.evidently_ui_prepared ? 1 : 0

  name   = "drift_demo_read"
  role   = aws_iam_role.evidently_ui_task[0].name
  policy = templatefile("${path.module}/owner_policies/evidently_ui_restore_read.json", local.job_policy_inputs)
}

output "evidently_ui_role" {
  description = "The Evidently UI task role name and ARN. Null until evidently_ui_prepared is true. The services root takes the ARN as evidently_ui_task_role_arn."
  value       = var.evidently_ui_prepared ? { name = aws_iam_role.evidently_ui_task[0].name, arn = aws_iam_role.evidently_ui_task[0].arn } : null
}

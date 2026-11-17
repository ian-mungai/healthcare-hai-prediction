variable "evidently_ui_prepared" {
  description = "Prepare the task role of the Evidently UI service on the container host. The UI shows the snapshots of the on-demand live drift check from a read-only host folder and calls no AWS service, so the role holds no policy. Off by default."
  type        = bool
  default     = false
  nullable    = false
}

# The UI needs no S3, secret or other grant. The role exists to give the service a task role of its own, apart from every other service. Nothing may attach a
# policy to it: monitoring.live_cases checks that no policy-attaching resource of this stack names it, and plants a grant to prove the check.
resource "aws_iam_role" "evidently_ui_task" {
  count = var.evidently_ui_prepared ? 1 : 0

  name                 = format(local.platform_name, "evidently_ui_task")
  description          = "Evidently UI on the container host: serves the live drift check snapshots from a read-only host folder and calls no AWS service. It holds no policy."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

output "evidently_ui_role" {
  description = "The Evidently UI task role name and ARN. Null until evidently_ui_prepared is true. The services root takes the ARN as evidently_ui_task_role_arn."
  value       = var.evidently_ui_prepared ? { name = aws_iam_role.evidently_ui_task[0].name, arn = aws_iam_role.evidently_ui_task[0].arn } : null
}

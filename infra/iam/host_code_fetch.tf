variable "host_code_fetch_prepared" {
  description = "Prepare the task role of the code one-shot on the container host, which puts the reviewed code archive of a run on the host as /data/repo. Off by default."
  type        = bool
  default     = false
  nullable    = false
}

# The code one-shot reads the staged code archive of the run that host_session start names, and nothing else: no write, no listing, no other prefix.
resource "aws_iam_role" "host_code_fetch_task" {
  count = var.host_code_fetch_prepared ? 1 : 0

  name                 = format(local.platform_name, "host_code_fetch_task")
  description          = "Code one-shot on the container host: reads lakehouse/run_stage/*/inputs/code.tar.gz and nothing else."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "host_code_fetch_task" {
  count = var.host_code_fetch_prepared ? 1 : 0

  name = "code_archive_read"
  role = aws_iam_role.host_code_fetch_task[0].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadStagedCodeArchives"
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = ["arn:aws:s3:::${var.data_bucket_name}/lakehouse/run_stage/*/inputs/code.tar.gz"]
    }]
  })
}

output "host_code_fetch_role" {
  description = "The code one-shot's task role name and ARN. Null until host_code_fetch_prepared is true. The services root takes the ARN as code_fetch_task_role_arn."
  value       = var.host_code_fetch_prepared ? { name = aws_iam_role.host_code_fetch_task[0].name, arn = aws_iam_role.host_code_fetch_task[0].arn } : null
}

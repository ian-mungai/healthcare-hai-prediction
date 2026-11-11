variable "publisher_job_enabled" {
  description = "Create the run 3 publisher job class: its task role and grants. It runs as a host bridge task (infra/jobs/publisher.tf), so it has no security group, subnet or public IP of its own; the host job firewall allows it DNS and TCP 443. Off until the owner approves the redownload route."
  type        = bool
  default     = false
  nullable    = false
}

variable "publisher_keys_enabled" {
  description = "Let the run 3 publisher task role read the three API keys by exact ARN. Off until the owner approves the keyed units on the task."
  type        = bool
  default     = false
  nullable    = false
}

variable "publisher_secret_arns" {
  description = "The exact ARNs of census_api_key, bls_api_key and hud_api_key, with their six-character suffix. Empty until publisher_keys_enabled is true."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = !var.publisher_keys_enabled || (
      toset(keys(var.publisher_secret_arns)) == toset(["bls_api_key", "census_api_key", "hud_api_key"]) &&
      alltrue([for name, arn in var.publisher_secret_arns : can(regex("^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:${name}-[A-Za-z0-9]{6}$", arn))])
    )
    error_message = "Name exactly census_api_key, bls_api_key and hud_api_key, each with its exact ARN and no wildcard."
  }
}

locals {
  publisher_keys_active   = var.publisher_job_enabled && var.publisher_keys_enabled
  publisher_job_role_name = format(local.platform_name, "job_publisher_task")
  publisher_job_inputs = {
    BUCKET_ARN = local.bronze_bucket_arn
  }
}

output "publisher_role" {
  description = "The run 3 publisher task role for the jobs root. It stays out of job_roles, which the closed target schema reads. Null until the class is enabled."
  value = var.publisher_job_enabled ? {
    name      = local.publisher_job_role_name
    arn       = "arn:aws:iam::${var.expected_account_id}:role/${local.publisher_job_role_name}"
    unique_id = aws_iam_role.job_publisher[0].unique_id
  } : null
}

resource "aws_iam_role" "job_publisher" {
  count = var.publisher_job_enabled ? 1 : 0

  name                 = local.publisher_job_role_name
  description          = "Run 3 publisher downloads. Creates objects once under raw and never deletes."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "job_publisher_keys" {
  count = local.publisher_keys_active ? 1 : 0

  name   = "read_three_api_keys"
  role   = aws_iam_role.job_publisher[0].name
  policy = templatefile("${path.module}/owner_policies/job_publisher_keys.json", { SECRET_ARNS = jsonencode(values(var.publisher_secret_arns)) })
}

resource "aws_iam_role_policy" "job_publisher" {
  count = var.publisher_job_enabled ? 1 : 0

  name   = "raw_create_once"
  role   = aws_iam_role.job_publisher[0].name
  policy = templatefile("${path.module}/owner_policies/job_publisher.json", local.publisher_job_inputs)
}

output "publisher_job_contract" {
  description = "Run 3 publisher class for integration into the jobs root. It runs on the host bridge network behind the host job firewall (DNS and TCP 443); the host limit is enforced by the queue gate and the approved hosts file, not the network."
  value = var.publisher_job_enabled ? {
    prepared        = var.publisher_job_enabled
    task_role_name  = local.publisher_job_role_name
    task_role_arn   = try(aws_iam_role.job_publisher[0].arn, null)
    capture_root    = "raw/"
    network         = "host_bridge"
    egress          = "host_job_firewall_tcp_443"
    host_limit      = "application_queue_hosts"
    delete_actions  = []
    writes_to_lakes = false
    secrets         = local.publisher_keys_active ? sort(keys(var.publisher_secret_arns)) : []
  } : null
}

variable "publisher_job_enabled" {
  description = "Create the run 3 publisher job class: its task role, its security group and its HTTPS egress. Off until the owner approves the redownload route."
  type        = bool
  default     = false
  nullable    = false
}

locals {
  publisher_job_role_name = format(local.platform_name, "job_publisher_task")
  publisher_job_inputs = {
    BUCKET_ARN = local.bronze_bucket_arn
  }
}

resource "aws_security_group" "job_publisher" {
  count = var.publisher_job_enabled ? 1 : 0

  name        = format(local.platform_name, "job_publisher")
  description = "Run 3 publisher downloads over HTTPS."
  vpc_id      = aws_vpc.platform.id

  tags = merge(local.project_tags, { Name = format(local.platform_name, "job_publisher") })
}

# Security groups cannot name hosts. The publisher hosts are limited by the queue gate (scripts.acquisition.redownload_route) and the downloader's own host list.
resource "aws_vpc_security_group_egress_rule" "job_publisher_https" {
  count = var.publisher_job_enabled ? 1 : 0

  security_group_id = aws_security_group.job_publisher[0].id
  description       = "Publisher HTTPS; the hosts are limited by the queue gate"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

output "publisher_network" {
  description = "The run 3 publisher class network for the jobs root. It needs a public IP because the platform has no NAT. Null until the class is enabled."
  value = var.publisher_job_enabled ? {
    subnet_id         = aws_subnet.public[0].id
    security_group_id = aws_security_group.job_publisher[0].id
    assign_public_ip  = "ENABLED"
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

resource "aws_iam_role_policy" "job_publisher" {
  count = var.publisher_job_enabled ? 1 : 0

  name   = "raw_create_once"
  role   = aws_iam_role.job_publisher[0].name
  policy = templatefile("${path.module}/owner_policies/job_publisher.json", local.publisher_job_inputs)
}

output "publisher_job_contract" {
  description = "Run 3 publisher class for integration into the jobs root and the target file. The host limit is enforced by the queue gate, not the network."
  value = {
    prepared        = var.publisher_job_enabled
    task_role_name  = local.publisher_job_role_name
    task_role_arn   = try(aws_iam_role.job_publisher[0].arn, null)
    security_group  = try(aws_security_group.job_publisher[0].id, null)
    capture_root    = "raw/"
    egress          = "https_443_any_address"
    host_limit      = "application_queue_hosts"
    delete_actions  = []
    writes_to_lakes = false
  }
}

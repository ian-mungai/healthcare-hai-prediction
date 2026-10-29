output "platform_role_arns" {
  description = "Roles the platform stack and its tasks use; the platform stack builds the same ARNs from the same names."
  value = {
    host               = aws_iam_role.ecs_host.arn
    task_execution     = aws_iam_role.task_execution.arn
    polaris_task       = aws_iam_role.polaris_task.arn
    control_plane_task = aws_iam_role.control_plane_task.arn
    snapshots          = aws_iam_role.snapshots.arn
  }
}

output "host_instance_profile_arn" {
  description = "Instance profile of the container host."
  value       = aws_iam_instance_profile.ecs_host.arn
}

output "host_launch_template" {
  description = "The host's launch template and the version the Auto Scaling group runs, as a number."
  value = {
    id      = aws_launch_template.host.id
    name    = aws_launch_template.host.name
    version = tostring(aws_launch_template.host.latest_version)
  }
}

output "host_autoscaling_group_name" {
  description = "Auto Scaling group that runs the container host: 0 or 1 instances, set by the project user with SetDesiredCapacity."
  value       = aws_autoscaling_group.host.name
}

output "platform_network" {
  description = "The network the platform stack finds by tag: the VPC, the two public subnets and the host and run-task security groups."
  value = {
    vpc_id                 = aws_vpc.platform.id
    cidr_block             = aws_vpc.platform.cidr_block
    public_subnet_ids      = aws_subnet.public[*].id
    host_security_group_id = aws_security_group.host.id
    task_security_group_id = aws_security_group.task.id
  }
}

output "budget" {
  description = "The quarterly cost budget and the topic that carries its alerts."
  value = {
    name          = aws_budgets_budget.platform.name
    alerts_topic  = aws_sns_topic.budget_alerts.arn
    project_tag   = local.platform_project_tag
    limit_dollars = var.budget_limit_usd
  }
}

output "lakehouse_role_arns" {
  description = "The two roles whose short sessions the launcher gives to pipeline jobs."
  value = {
    reader = aws_iam_role.lakehouse_reader.arn
    writer = aws_iam_role.lakehouse_writer.arn
  }
}

output "project_secret_names" {
  description = "The names the project user's policy files grant: the shared API keys in Secrets Manager (read only), the project parameters in Parameter Store (read), the managed ones among them (written) and the ten AWS Polaris pair names the project user sets up."
  value = {
    shared_secrets     = local.shared_secret_names
    read_parameters    = local.read_parameter_names
    managed_parameters = local.managed_parameter_names
    aws_polaris_pairs = [
      var.aws_polaris_reader_client_id_name, var.aws_polaris_reader_client_secret_name,
      var.aws_polaris_e2e_client_id_name, var.aws_polaris_e2e_client_secret_name,
      var.aws_polaris_bronze_client_id_name, var.aws_polaris_bronze_client_secret_name,
      var.aws_polaris_silver_client_id_name, var.aws_polaris_silver_client_secret_name,
      var.aws_polaris_gold_client_id_name, var.aws_polaris_gold_client_secret_name,
    ]
  }

  precondition {
    condition = length(distinct(concat(
      local.shared_secret_names, local.read_parameter_names,
      [var.aws_polaris_silver_client_id_name, var.aws_polaris_silver_client_secret_name, var.aws_polaris_gold_client_id_name, var.aws_polaris_gold_client_secret_name],
    ))) == length(local.shared_secret_names) + length(local.read_parameter_names) + 4
    error_message = "Each secret name must be different."
  }
}

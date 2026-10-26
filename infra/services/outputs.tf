output "service_names" {
  description = "ECS service name of each control-plane service."
  value       = { for key, service in aws_ecs_service.service : key => service.name }
}

output "task_definition_arns" {
  description = "Task definition ARN of each control-plane service and of the one-shot host preparation and Polaris bootstrap tasks."
  value       = merge({ for key, definition in aws_ecs_task_definition.service : key => definition.arn }, { for key, definition in aws_ecs_task_definition.one_shot : key => definition.arn })
}

output "parameter_names_used" {
  description = "Every Parameter Store name the task definitions read; the execution role needs ssm:GetParameters on exactly these."
  value       = local.secret_names_used
}

output "parameter_arns_used" {
  description = "The full Parameter Store ARNs the task definitions inject (arn:aws:ssm:<region>:<account>:parameter/<name without its leading slash>), built from the names."
  value       = local.secret_arns_used
}

output "one_shot_run_task" {
  description = "Arguments for each manual aws ecs run-task call. IAM requires the explicit Project request tag. Propagation also copies the task definition tags."
  value = {
    for key, definition in aws_ecs_task_definition.one_shot : key => {
      cluster                 = var.cluster_name
      task_definition         = definition.family
      capacity_provider       = var.ec2_capacity_provider_name
      propagate_tags          = local.task_tag_source
      enable_ecs_managed_tags = true
      tags                    = { Project = var.project_tag }
    }
  }
}

output "host_ports_reachable_from_jobs" {
  description = "Host ports the foundation security group opens to the job tasks: Polaris REST (8181) and Redis (6379). Every other port is loopback or closed."
  value       = { polaris = 8181, redis = 6379 }
}

output "reserved_memory_mib" {
  description = "Sum of the memory reservations of the always-on control-plane containers."
  value       = local.reserved_memory_mib
}

output "container_definitions" {
  description = "The container definitions JSON of every task definition, keyed by task name, for review and for the secret-flow test."
  value       = { for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : key => definition.container_definitions }
}

locals {
  # AWS or the provider assigns these at apply, so they are unknown at plan and are not configuration.
  task_attributes_assigned_at_apply = ["arn", "arn_without_revision", "id", "region", "revision", "tags_all"]
}

output "task_definition_settings" {
  description = "Every configured attribute of every task definition except container_definitions (see container_definitions), keyed by task name, for the approval pin. Nothing is picked by name: only the attributes AWS assigns at apply are left out. Sets (volumes, compatibilities) are lists in Terraform's stable set order."
  value = {
    for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : key => {
      for attribute, value in definition : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

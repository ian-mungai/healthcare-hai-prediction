output "task_definition_arn" {
  description = "Fixed diagnostic revision. Null while the independent probe is disabled."
  value       = try(aws_ecs_task_definition.probe[0].arn, null)
}

output "container_definition" {
  description = "Fixed bounded seed command for exact plan review."
  value       = try(aws_ecs_task_definition.probe[0].container_definitions, null)
}

output "dispatch_contract" {
  description = "Separate one-task diagnostic launch settings. No task, grant or application resource is launched by this root."
  value = var.probe_enabled ? {
    assign_public_ip   = "ENABLED"
    network            = var.foundation_network
    propagate_tags     = "TASK_DEFINITION"
    capacity_provider  = "FARGATE"
    hard_stop_seconds  = 60
    outer_slot_seconds = 1800
  } : null
}

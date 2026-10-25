output "task_definition_arns" {
  description = "Fixed registered read task revisions, keyed by definition name."
  value       = { for name, task in aws_ecs_task_definition.read : name => task.arn }
}

output "container_definitions" {
  description = "Complete read container JSON for the accepted target renderer and secret-flow checks."
  value       = { for name, task in aws_ecs_task_definition.read : name => task.container_definitions }
}

output "task_definition_settings" {
  description = "Every configured task attribute except container JSON and attributes assigned at apply."
  value       = local.task_settings
}

output "execution_digests" {
  description = "Complete execution pins from the shared service definition flow module."
  value       = module.definition_flow.execution_digests
}

output "secret_flow" {
  description = "Secret-flow and pin mutation results for review and mocked plan assertions."
  value = {
    violations             = module.definition_flow.violations
    alias_chains_converged = module.definition_flow.alias_chains_converged
    containers_checked     = module.definition_flow.containers_checked
    mutation_mismatches    = module.definition_flow.mutation_mismatches
    mutations_checked      = module.definition_flow.mutations_checked
  }
}

output "dispatch_contract" {
  description = "Required runtime network, admission and stop settings. Registration does not dispatch tasks or enforce aggregate admission."
  value = {
    assign_public_ip   = "DISABLED"
    propagate_tags     = "TASK_DEFINITION"
    capacity_providers = ["FARGATE_SPOT", "FARGATE"]
    class_networks     = { offline = "offline", seal = "offline", lakehouse_read = "lakehouse" }
    definition_classes = { for name, spec in local.selected : name => spec.class }
    cloud_limits       = var.cloud_limits
    deadlines = {
      hard_stop_required         = true
      pending_seconds            = 600
      stop_grace_seconds         = 90
      cleanup_seconds            = 120
      budget_alert_is_spend_stop = false
    }
  }
}

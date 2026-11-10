output "task_definition_arns" {
  description = "Fixed registered application task revisions, keyed by definition name."
  value       = { for name, task in local.task_definitions : name => task.arn }
}

output "container_definitions" {
  description = "Complete application container JSON for the accepted target renderer and secret-flow checks."
  value       = { for name, task in local.task_definitions : name => task.container_definitions }
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
  description = "Per-class capacity and required runtime network, admission and stop settings. Writers require explicit on-demand capacity. Registration does not dispatch tasks or enforce aggregate admission."
  value = {
    assign_public_ip   = "DISABLED"
    propagate_tags     = "TASK_DEFINITION"
    capacity_providers = ["FARGATE_SPOT", "FARGATE"]
    class_capacity_providers = merge(
      { for name in ["offline", "seal", "lakehouse_read"] : name => ["FARGATE_SPOT", "FARGATE"] },
      length(local.selected_writers) > 0 ? { lakehouse_write = ["FARGATE"] } : {}
    )
    class_networks     = merge({ offline = "offline", seal = "offline", lakehouse_read = "lakehouse" }, length(local.selected_writers) > 0 ? { lakehouse_write = "lakehouse" } : {})
    definition_classes = { for name, spec in local.selected_definitions : name => spec.class }
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

output "writer_dispatch_contracts" {
  description = "Closed writer placement and parameter-name bindings. Dispatch must enforce these on-demand private contracts before RunTask. Registration activates no writer."
  value = {
    for name, spec in local.selected_writers : name => {
      job_class          = "lakehouse_write"
      task_role_key      = spec.role
      parameter_names    = lookup(var.writer_parameter_names, spec.role, [])
      capacity_providers = ["FARGATE"]
      network            = "lakehouse"
      assign_public_ip   = "DISABLED"
    }
  }
}

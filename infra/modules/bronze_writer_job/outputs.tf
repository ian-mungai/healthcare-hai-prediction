output "task_definition_arn" {
  description = "Registered candidate ARN, or null while registration remains disabled."
  value       = try(aws_ecs_task_definition.bronze_writer[0].arn, null)
}

output "dispatch_contract" {
  description = "Inactive dispatch contract. RunTask must propagate tags and replace the container command after admission."
  value = {
    role_key                  = "job_write_bronze_task"
    principal                 = "bronze_writer"
    definition_key            = "bronze_writer"
    capacity_provider         = "FARGATE"
    assign_public_ip          = "DISABLED"
    retry_after_start         = false
    production_layer          = "off"
    binding_path              = var.run_binding_path
    worker_command            = local.bronze_worker_command
    writer_job_class          = "lakehouse_write"
    required_dispatch_inputs  = ["current_authority_epoch", "job", "tables_or_fixture_action"]
    journal_owner             = "launcher"
    container_template        = local.bronze_container
    propagate_tags            = "TASK_DEFINITION"
    command_override_required = true
    command_override = {
      container_name = local.bronze_container.name
      command_prefix = local.bronze_worker_command
      required_flags = ["--job", "--authority-epoch"]
      optional_flags = ["--tables", "--fixture-action"]
    }
  }
}

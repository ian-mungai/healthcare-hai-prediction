variable "bronze_job_enabled" {
  description = "Register the permanent bronze task definition that the lean job broker starts. It needs the spark image digest, the execution role, the bronze writer role and its pair, and the jobs log group. The tables, the target and the dry run flag arrive at dispatch."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition = !var.bronze_job_enabled || (
      contains(keys(var.images), "spark") &&
      contains(keys(var.job_role_arns), "execution") &&
      contains(keys(var.job_role_arns), "write_bronze_task") &&
      contains(keys(var.writer_parameter_names), "write_bronze_task") &&
      var.log_group_name != null
    )
    error_message = "The bronze job needs the spark image digest, the execution role, the bronze writer role and pair, and the jobs log group."
  }
}

variable "bronze_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of the bronze task."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 4096, memory = 16384, ephemeral_storage_gib = 50 }

  validation {
    condition = try(
      var.bronze_resources.memory >= local.memory_shapes[tostring(var.bronze_resources.cpu)].min &&
      var.bronze_resources.memory <= local.memory_shapes[tostring(var.bronze_resources.cpu)].max &&
      var.bronze_resources.memory % local.memory_shapes[tostring(var.bronze_resources.cpu)].step == 0 &&
      var.bronze_resources.ephemeral_storage_gib >= 20 && var.bronze_resources.ephemeral_storage_gib <= 200 &&
      var.bronze_resources.ephemeral_storage_gib == floor(var.bronze_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

locals {
  bronze_job_enabled = var.bronze_job_enabled ? toset(["bronze"]) : toset([])
  bronze_role        = "write_bronze_task"
  # The container names and their empty variables equal config/orchestration/ecs_broker_jobs.json; a Terraform test compares them with the file.
  bronze_run_variables = {
    fetch = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "HAI_DEADLINE_UTC"]
    pin   = ["RUN_ID", "CODE_SHA256", "BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "BRONZE_INVENTORY_SHA256", "HAI_DEADLINE_UTC"]
    load  = ["RUN_ID", "POLARIS_URI", "BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "HAI_DEADLINE_UTC"]
    check = ["RUN_ID", "BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "HAI_DEADLINE_UTC"]
  }
  bronze_driver = "cd /workspace/code && python -m scripts.lakehouse.bronze_run %s --run \"$RUN_ID\""
  bronze_steps = length(local.bronze_job_enabled) == 0 ? [] : concat(
    [{ name = "init", script = "chown 10001:10001 /workspace /tmp" }],
    [for phase in ["fetch", "pin", "load", "check"] : {
      name   = phase
      script = phase == "fetch" ? "python -I -c '${local.layer_fetch_python}'" : format(local.bronze_driver, phase)
    }]
  )
  bronze_containers = [
    for index, step in local.bronze_steps : {
      name                   = step.name
      image                  = lookup(var.images, "spark", "invalid")
      essential              = index == length(local.bronze_steps) - 1
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      # Every phase starts through the in-image deadline runner, which stops the process group at the one absolute deadline the broker sets in HAI_DEADLINE_UTC.
      entryPoint      = step.name == "init" ? ["sh", "-c"] : ["/opt/hai/deadline_run", "sh", "-c"]
      command         = [step.script]
      stopTimeout     = 90
      linuxParameters = { capabilities = { add = [], drop = step.name == "init" ? local.train_init_drop : ["ALL"] }, initProcessEnabled = true }
      ulimits         = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn       = index == 0 ? [] : [{ containerName = local.bronze_steps[index - 1].name, condition = "SUCCESS" }]
      environment = concat(
        [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
          { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONHASHSEED", value = "0" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "SPARK_JOB_MEMORY", value = "10g" },
          { name = "JOB_THREADS", value = "4" },
          { name = "HAI_SPARK_LOCAL_DIRECTORY", value = "/workspace/spill" },
        ],
        [for name in lookup(local.bronze_run_variables, step.name, []) : { name = name, value = "" }]
      )
      secrets = step.name == "load" ? lookup(local.writer_parameters, local.bronze_role, []) : []
      mountPoints = [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "bronze"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ]
}

resource "aws_ecs_task_definition" "bronze" {
  count = length(local.bronze_job_enabled)

  family                   = "${var.project_name}_${var.environment}_job_bronze"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.bronze_resources.cpu)
  memory                   = tostring(var.bronze_resources.memory)
  execution_role_arn       = lookup(var.job_role_arns, "execution", "")
  task_role_arn            = lookup(var.job_role_arns, local.bronze_role, "")
  container_definitions    = jsonencode(local.bronze_containers)
  skip_destroy             = true
  enable_fault_injection   = false
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  volume {
    name                = "workspace"
    configure_at_launch = false
  }

  volume {
    name                = "tmp"
    configure_at_launch = false
  }

  dynamic "ephemeral_storage" {
    for_each = var.bronze_resources.ephemeral_storage_gib > 20 ? [var.bronze_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "bronze_flow" {
  source      = "../services/tests/flow"
  definitions = { for task in aws_ecs_task_definition.bronze : "bronze" => task.container_definitions }
  tasks = {
    for task in aws_ecs_task_definition.bronze : "bronze" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "bronze_task_definition_arn" {
  description = "Registered bronze task revision. Null while bronze_job_enabled is false."
  value       = try(aws_ecs_task_definition.bronze[0].arn, null)
}

output "bronze_container_definitions" {
  description = "Complete container JSON of the bronze task for review and mocked plan assertions. Null while bronze_job_enabled is false."
  value       = try(aws_ecs_task_definition.bronze[0].container_definitions, null)
}

output "bronze_secret_flow" {
  description = "Secret-flow and pin results of the shared flow module for the bronze task."
  value = {
    violations          = module.bronze_flow.violations
    containers_checked  = module.bronze_flow.containers_checked
    mutation_mismatches = module.bronze_flow.mutation_mismatches
  }
}

output "bronze_run_task" {
  description = "What the broker's run-task call needs for the bronze task. Registration starts nothing. The tables, the target and the dry run flag come from the staged request; the dry run variable is the one override that makes a hand-started run a dry run."
  value = merge([
    for task in aws_ecs_task_definition.bronze : {
      task_definition       = task.family
      task_role_key         = local.bronze_role
      assign_public_ip      = "DISABLED"
      capacity_providers    = ["FARGATE"]
      network               = "lakehouse"
      propagate_tags        = "TASK_DEFINITION"
      container_environment = local.bronze_run_variables
      dry_run_variable      = "BRONZE_DRY_RUN"
      dry_run_value         = "1"
    }
  ]...)
}

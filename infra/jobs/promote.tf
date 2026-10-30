variable "promote_enabled" {
  description = "Register the promotion task that the lean broker starts for model_promotion_dag. It reuses the training image, the execution role, the training task role and the MLflow administrator password, and it needs the jobs log group."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.promote_enabled || (var.train_enabled && var.log_group_name == "${var.project_name}_jobs_${var.environment}")
    error_message = "The promotion task needs the training task registered and the merged platform jobs log group."
  }
}

variable "promote_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of the promotion task. It moves a registry alias, so it is small."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 1024, memory = 4096, ephemeral_storage_gib = 21 }

  validation {
    condition = try(
      var.promote_resources.memory >= local.memory_shapes[tostring(var.promote_resources.cpu)].min &&
      var.promote_resources.memory <= local.memory_shapes[tostring(var.promote_resources.cpu)].max &&
      var.promote_resources.memory % local.memory_shapes[tostring(var.promote_resources.cpu)].step == 0 &&
      var.promote_resources.ephemeral_storage_gib >= 20 && var.promote_resources.ephemeral_storage_gib <= 200 &&
      var.promote_resources.ephemeral_storage_gib == floor(var.promote_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

locals {
  # Two steps after the volume hand-over: fetch the run's code, then move the alias. The promote step logs in to MLflow as the train step does.
  promote_phase = "cd /workspace/code && python -m scripts.training.aws_run promote --run \"$RUN_ID\""
  promote_steps = [
    { name = "init", script = "chown 10001:10001 /workspace /tmp", secrets = [], variables = [] },
    { name = "fetch", script = "python -I /opt/hai/fetch_code.py", secrets = [], variables = ["RUN_ID", "CODE_SHA256"] },
    {
      name      = "promote"
      script    = local.promote_phase
      secrets   = [{ name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.mlflow_admin_password_name}" }]
      variables = ["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS", "MLFLOW_TRACKING_URI"]
    },
  ]
  promote_containers = var.promote_enabled ? [
    for index, step in local.promote_steps : {
      name                   = step.name
      image                  = lookup(var.train_images, "training", "invalid")
      essential              = step.name == "promote"
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = ["sh", "-c"]
      command                = [step.script]
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.train_init_drop : ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = index == 0 ? [] : [{ containerName = local.promote_steps[index - 1].name, condition = "SUCCESS" }]
      environment = concat(
        [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
          { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONHASHSEED", value = "0" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
        ],
        [for name in step.variables : { name = name, value = "" }],
        step.name == "promote" ? [{ name = "MLFLOW_TRACKING_USERNAME", value = "admin" }] : []
      )
      secrets = step.secrets
      mountPoints = [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "promote"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ] : []
}

resource "aws_ecs_task_definition" "promote" {
  count = var.promote_enabled ? 1 : 0

  family                   = "${var.project_name}_${var.environment}_job_promote"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.promote_resources.cpu)
  memory                   = tostring(var.promote_resources.memory)
  execution_role_arn       = lookup(var.train_roles, "execution", "")
  task_role_arn            = lookup(var.train_roles, "train_task", "")
  container_definitions    = jsonencode(local.promote_containers)
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
    for_each = var.promote_resources.ephemeral_storage_gib > 20 ? [var.promote_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "promote_flow" {
  source      = "../services/tests/flow"
  definitions = { for task in aws_ecs_task_definition.promote : "promote" => task.container_definitions }
  tasks = {
    for task in aws_ecs_task_definition.promote : "promote" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "promote_container_definitions" {
  description = "Complete container JSON of the promotion task, or null while promote_enabled is false."
  value       = one([for task in aws_ecs_task_definition.promote : task.container_definitions])
}

output "promote_secret_flow" {
  description = "Secret-flow and pin mutation results of the promotion task for review and mocked plan assertions."
  value = {
    violations          = module.promote_flow.violations
    containers_checked  = module.promote_flow.containers_checked
    mutation_mismatches = module.promote_flow.mutation_mismatches
  }
}

output "promote_run_task" {
  description = "What a run-task call for the promotion task needs, or null while promote_enabled is false. The broker supplies the variables. Registration starts nothing."
  value = var.promote_enabled ? {
    task_definition       = aws_ecs_task_definition.promote[0].family
    assign_public_ip      = "DISABLED"
    capacity_providers    = ["FARGATE"]
    propagate_tags        = "TASK_DEFINITION"
    network_source        = "training_network output of the IAM stack"
    container_environment = { for step in local.promote_steps : step.name => step.variables if length(step.variables) > 0 }
  } : null
}

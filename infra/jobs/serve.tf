variable "serve_enabled" {
  description = "Register the serve task that the lean broker starts for serving_deploy_dag. It reuses the training image, the execution role and the MLflow administrator password. It has its own task role and needs the jobs log group."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.serve_enabled || (var.train_enabled && var.log_group_name == "${var.project_name}_jobs_${var.environment}")
    error_message = "The serve task needs the training task registered and the merged platform jobs log group."
  }
}

variable "serve_roles" {
  description = "The merged IAM role ARN of the serve task, keyed serve_task. The execution role comes from train_roles."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, arn in var.serve_roles : name == "serve_task" && arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_serve_task_${var.environment}"]) &&
      (!var.serve_enabled || contains(keys(var.serve_roles), "serve_task"))
    )
    error_message = "Bind only the exact merged serve task role. Another job's role is refused."
  }
}

variable "serve_manifest_prefix" {
  description = "Key prefix of the deployment records in the data bucket, without a trailing slash, for example serving/deployments. It is fixed in the task definition, so a caller cannot choose where records go."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition = !var.serve_enabled || (
      can(regex("^[A-Za-z0-9_][A-Za-z0-9_./-]{0,200}[A-Za-z0-9_]$", var.serve_manifest_prefix)) &&
      !startswith(var.serve_manifest_prefix, "lakehouse/") && !strcontains(var.serve_manifest_prefix, "..")
    )
    error_message = "Set serve_manifest_prefix to one key prefix without wildcards, a leading or trailing slash or a parent step, outside the lakehouse prefix."
  }
}

variable "serve_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of the serve task. It writes two small records and reads the model registry, so it is small."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 1024, memory = 4096, ephemeral_storage_gib = 21 }

  validation {
    condition = try(
      var.serve_resources.memory >= local.memory_shapes[tostring(var.serve_resources.cpu)].min &&
      var.serve_resources.memory <= local.memory_shapes[tostring(var.serve_resources.cpu)].max &&
      var.serve_resources.memory % local.memory_shapes[tostring(var.serve_resources.cpu)].step == 0 &&
      var.serve_resources.ephemeral_storage_gib >= 20 && var.serve_resources.ephemeral_storage_gib <= 200 &&
      var.serve_resources.ephemeral_storage_gib == floor(var.serve_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

locals {
  # Two steps after the volume hand-over: fetch the run's code, then deploy. The deploy step logs in to MLflow as the train step does. The checked request arrives in
  # SERVING_REQUEST, which the broker read once from the staged record, and the tool reads it from there. The time limit is shorter than the broker deadline, so the
  # task ends itself first, and every MLflow request has its own short limit.
  serve_phase = "cd /workspace/code && timeout 600 python -m scripts.serving.deploy_job --run \"$RUN_ID\""
  serve_steps = [
    { name = "init", script = "chown 10001:10001 /workspace /tmp", secrets = [], variables = [] },
    { name = "fetch", script = "python -I /opt/hai/fetch_code.py", secrets = [], variables = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"] },
    {
      name      = "serve"
      script    = local.serve_phase
      secrets   = [{ name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.mlflow_admin_password_name}" }]
      variables = ["RUN_ID", "SERVING_REQUEST", "MLFLOW_TRACKING_URI"]
    },
  ]
  serve_containers = var.serve_enabled ? [
    for index, step in local.serve_steps : {
      name                   = step.name
      image                  = lookup(var.train_images, "training", "invalid")
      essential              = step.name == "serve"
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = ["sh", "-c"]
      command                = [step.script]
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.train_init_drop : ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = index == 0 ? [] : [{ containerName = local.serve_steps[index - 1].name, condition = "SUCCESS" }]
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
        step.name == "serve" ? [
          { name = "MLFLOW_TRACKING_USERNAME", value = "admin" },
          { name = "MLFLOW_HTTP_REQUEST_TIMEOUT", value = "10" },
          { name = "MLFLOW_HTTP_REQUEST_MAX_RETRIES", value = "2" },
          { name = "MODEL_API_MANIFEST_URI", value = "s3://${var.data_bucket_name}/${var.serve_manifest_prefix}" },
        ] : []
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
          awslogs-stream-prefix = "serve"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ] : []
}

resource "aws_ecs_task_definition" "serve" {
  count = var.serve_enabled ? 1 : 0

  family                   = "${var.project_name}_${var.environment}_job_serve"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.serve_resources.cpu)
  memory                   = tostring(var.serve_resources.memory)
  execution_role_arn       = lookup(var.train_roles, "execution", "")
  task_role_arn            = lookup(var.serve_roles, "serve_task", "")
  container_definitions    = jsonencode(local.serve_containers)
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
    for_each = var.serve_resources.ephemeral_storage_gib > 20 ? [var.serve_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "serve_flow" {
  source      = "../services/tests/flow"
  definitions = { for task in aws_ecs_task_definition.serve : "serve" => task.container_definitions }
  tasks = {
    for task in aws_ecs_task_definition.serve : "serve" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "serve_container_definitions" {
  description = "Complete container JSON of the serve task, or null while serve_enabled is false."
  value       = one([for task in aws_ecs_task_definition.serve : task.container_definitions])
}

output "serve_secret_flow" {
  description = "Secret-flow and pin mutation results of the serve task for review and mocked plan assertions."
  value = {
    violations          = module.serve_flow.violations
    containers_checked  = module.serve_flow.containers_checked
    mutation_mismatches = module.serve_flow.mutation_mismatches
  }
}

output "serve_run_task" {
  description = "What a run-task call for the serve task needs, or null while serve_enabled is false. The broker supplies the variables. Registration starts nothing."
  value = var.serve_enabled ? {
    task_definition       = aws_ecs_task_definition.serve[0].family
    assign_public_ip      = "DISABLED"
    capacity_providers    = ["FARGATE"]
    propagate_tags        = "TASK_DEFINITION"
    network_source        = "training_network output of the IAM stack"
    container_environment = { for step in local.serve_steps : step.name => step.variables if length(step.variables) > 0 }
  } : null
}

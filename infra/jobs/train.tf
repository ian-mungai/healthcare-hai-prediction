variable "train_enabled" {
  description = "Register the owner-run training task (decision 56). It needs the training and feature store image digests, the execution and training roles and the jobs log group."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.train_enabled || var.log_group_name == "${var.project_name}_jobs_${var.environment}"
    error_message = "The training task needs the merged platform jobs log group."
  }
}

variable "train_images" {
  description = "Training and feature store image URIs from the project ECR repositories, pinned by digest."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, image in var.train_images :
        contains(["training", "feature_store"], name) &&
        can(regex("^${var.expected_account_id}\\.dkr\\.ecr\\.${var.aws_region}\\.amazonaws\\.com/${var.project_name}_${name}_${var.environment}@sha256:[a-f0-9]{64}$", image))
      ]) &&
      (!var.train_enabled || alltrue([for name in ["training", "feature_store"] : contains(keys(var.train_images), name)]))
    )
    error_message = "Supply the training and feature store images from their project ECR repositories with an immutable SHA-256 digest."
  }
}

variable "train_roles" {
  description = "The merged IAM role ARNs of the training task, keyed execution and train_task."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, arn in var.train_roles :
        contains(["execution", "train_task"], name) && arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_${name}_${var.environment}"
      ]) &&
      (!var.train_enabled || alltrue([for name in ["execution", "train_task"] : contains(keys(var.train_roles), name)]))
    )
    error_message = "Bind only the exact merged execution role and training task role. Another layer's role is refused."
  }
}

variable "train_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of the training task."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 4096, memory = 16384, ephemeral_storage_gib = 50 }

  validation {
    condition = try(
      var.train_resources.memory >= local.memory_shapes[tostring(var.train_resources.cpu)].min &&
      var.train_resources.memory <= local.memory_shapes[tostring(var.train_resources.cpu)].max &&
      var.train_resources.memory % local.memory_shapes[tostring(var.train_resources.cpu)].step == 0 &&
      var.train_resources.ephemeral_storage_gib >= 20 && var.train_resources.ephemeral_storage_gib <= 200 &&
      var.train_resources.ephemeral_storage_gib == floor(var.train_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

locals {
  train_run_variables = {
    fetch   = ["RUN_ID", "CODE_SHA256"]
    prepare = ["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS"]
    extract = ["RUN_ID", "POLARIS_URI"]
    train   = ["RUN_ID", "MLFLOW_TRACKING_URI"]
  }
  train_phase = "cd /workspace/code && python -m scripts.training.aws_run %s --run \"$RUN_ID\""
  train_steps = [
    { name = "fetch", image = "training", script = "python -I /opt/hai/fetch_code.py", secrets = [] },
    { name = "prepare", image = "training", script = format(local.train_phase, "prepare"), secrets = [] },
    { name = "extract", image = "feature_store", script = format(local.train_phase, "extract"), secrets = local.reader_parameters },
    { name = "train", image = "training", script = format(local.train_phase, "train"), secrets = [] },
  ]
  train_containers = var.train_enabled ? [
    for index, step in local.train_steps : {
      name                   = step.name
      image                  = lookup(var.train_images, step.image, "invalid")
      essential              = step.name == "train"
      privileged             = false
      readonlyRootFilesystem = true
      user                   = "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = ["sh", "-c"]
      command                = [step.script]
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = index == 0 ? [] : [{ containerName = local.train_steps[index - 1].name, condition = "SUCCESS" }]
      environment = concat(
        [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
          { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONHASHSEED", value = "0" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "JOB_THREADS", value = "4" },
          { name = "OMP_NUM_THREADS", value = "4" },
          { name = "OPENBLAS_NUM_THREADS", value = "4" },
          { name = "MKL_NUM_THREADS", value = "4" },
          { name = "DUCKDB_MEMORY_LIMIT", value = "12GB" },
        ],
        [for name in local.train_run_variables[step.name] : { name = name, value = "" }],
        step.name == "train" ? [{ name = "TRAINING_IMAGE_DIGEST", value = split("@", lookup(var.train_images, step.image, "invalid@invalid"))[1] }] : []
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
          awslogs-stream-prefix = "train"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ] : []
}

resource "aws_ecs_task_definition" "train" {
  count = var.train_enabled ? 1 : 0

  family                   = "${var.project_name}_${var.environment}_job_train"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.train_resources.cpu)
  memory                   = tostring(var.train_resources.memory)
  execution_role_arn       = lookup(var.train_roles, "execution", "")
  task_role_arn            = lookup(var.train_roles, "train_task", "")
  container_definitions    = jsonencode(local.train_containers)
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
    for_each = var.train_resources.ephemeral_storage_gib > 20 ? [var.train_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "train_flow" {
  source      = "../services/tests/flow"
  definitions = { for task in aws_ecs_task_definition.train : "train" => task.container_definitions }
  tasks = {
    for task in aws_ecs_task_definition.train : "train" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "train_container_definitions" {
  description = "Complete container JSON of the training task, or null while train_enabled is false."
  value       = one([for task in aws_ecs_task_definition.train : task.container_definitions])
}

output "train_secret_flow" {
  description = "Secret-flow and pin mutation results of the training task for review and mocked plan assertions."
  value = {
    violations          = module.train_flow.violations
    containers_checked  = module.train_flow.containers_checked
    mutation_mismatches = module.train_flow.mutation_mismatches
  }
}

output "train_run_task" {
  description = "What the owner's run-task call for the training task needs, or null while train_enabled is false. Registration starts nothing."
  value = var.train_enabled ? {
    task_definition       = aws_ecs_task_definition.train[0].family
    assign_public_ip      = "DISABLED"
    capacity_providers    = ["FARGATE"]
    propagate_tags        = "TASK_DEFINITION"
    network_source        = "training_network output of the IAM stack"
    container_environment = local.train_run_variables
  } : null
}

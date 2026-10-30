variable "drift_enabled" {
  description = "Register the drift task that the broker starts for drift_monitoring_dag (decision 21). It needs the training, feature store and drift image digests, the execution and drift roles and the jobs log group."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.drift_enabled || var.log_group_name == "${var.project_name}_jobs_${var.environment}"
    error_message = "The drift task needs the merged platform jobs log group."
  }
}

variable "drift_images" {
  description = "Training, feature store and drift image URIs from the project ECR repositories, pinned by digest."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, image in var.drift_images :
        contains(["training", "feature_store", "drift"], name) &&
        can(regex("^${var.expected_account_id}\\.dkr\\.ecr\\.${var.aws_region}\\.amazonaws\\.com/${var.project_name}_${name}_${var.environment}@sha256:[a-f0-9]{64}$", image))
      ]) &&
      (!var.drift_enabled || alltrue([for name in ["training", "feature_store", "drift"] : contains(keys(var.drift_images), name)]))
    )
    error_message = "Supply the training, feature store and drift images from their project ECR repositories with an immutable SHA-256 digest."
  }
}

variable "drift_roles" {
  description = "The merged IAM role ARNs of the drift task, keyed execution and drift_task."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, arn in var.drift_roles :
        contains(["execution", "drift_task"], name) && arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_${name}_${var.environment}"
      ]) &&
      (!var.drift_enabled || alltrue([for name in ["execution", "drift_task"] : contains(keys(var.drift_roles), name)]))
    )
    error_message = "Bind only the exact merged execution role and drift task role. Another layer's role is refused."
  }
}

variable "drift_mlflow_password_name" {
  description = "Parameter Store SecureString name of the MLflow administrator password. The baseline container logs in with it. Terraform references the name and never reads the value."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_admin_password"
  nullable    = false

  validation {
    condition     = !var.drift_enabled || var.drift_mlflow_password_name == "/${var.project_name}/mlflow_admin_password"
    error_message = "Inject only the project MLflow administrator password parameter."
  }
}

variable "drift_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of the drift task."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 2048, memory = 8192, ephemeral_storage_gib = 30 }

  validation {
    condition = try(
      var.drift_resources.memory >= local.memory_shapes[tostring(var.drift_resources.cpu)].min &&
      var.drift_resources.memory <= local.memory_shapes[tostring(var.drift_resources.cpu)].max &&
      var.drift_resources.memory % local.memory_shapes[tostring(var.drift_resources.cpu)].step == 0 &&
      var.drift_resources.ephemeral_storage_gib >= 20 && var.drift_resources.ephemeral_storage_gib <= 200 &&
      var.drift_resources.ephemeral_storage_gib == floor(var.drift_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

locals {
  drift_run_variables = {
    fetch    = ["RUN_ID", "CODE_SHA256"]
    baseline = ["RUN_ID", "MLFLOW_TRACKING_URI"]
    current  = ["RUN_ID", "POLARIS_URI"]
    check    = ["RUN_ID"]
    publish  = ["RUN_ID"]
  }
  drift_phase = "cd /workspace/code && python -m monitoring.aws_run %s --run \"$RUN_ID\""
  # Fargate bind mounts are created owned by root, and the job user is 10001. The init step runs first as root, with only the capability to change owners
  # (Fargate lets a container drop default capabilities but not add one), and hands the two volumes to the job user.
  drift_init_drop = ["DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "NET_RAW", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE"]
  drift_steps = [
    { name = "init", image = "training", script = "chown 10001:10001 /workspace /tmp", secrets = [] },
    { name = "fetch", image = "training", script = "python -I /opt/hai/fetch_code.py", secrets = [] },
    {
      name    = "baseline"
      image   = "training"
      script  = format(local.drift_phase, "baseline")
      secrets = [{ name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.drift_mlflow_password_name}" }]
    },
    { name = "current", image = "feature_store", script = format(local.drift_phase, "current"), secrets = local.reader_parameters },
    { name = "check", image = "drift", script = format(local.drift_phase, "check"), secrets = [] },
    { name = "publish", image = "training", script = format(local.drift_phase, "publish"), secrets = [] },
  ]
  drift_containers = var.drift_enabled ? [
    for index, step in local.drift_steps : {
      name                   = step.name
      image                  = lookup(var.drift_images, step.image, "invalid")
      essential              = step.name == "publish"
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = ["sh", "-c"]
      command                = [step.script]
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.drift_init_drop : ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = index == 0 ? [] : [{ containerName = local.drift_steps[index - 1].name, condition = "SUCCESS" }]
      environment = concat(
        [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
          { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONHASHSEED", value = "0" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "JOB_THREADS", value = "2" },
          { name = "OMP_NUM_THREADS", value = "2" },
          { name = "OPENBLAS_NUM_THREADS", value = "2" },
          { name = "MKL_NUM_THREADS", value = "2" },
          { name = "DUCKDB_MEMORY_LIMIT", value = "5GB" },
        ],
        [for name in lookup(local.drift_run_variables, step.name, []) : { name = name, value = "" }],
        step.name == "baseline" ? [{ name = "MLFLOW_TRACKING_USERNAME", value = "admin" }] : []
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
          awslogs-stream-prefix = "drift"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ] : []
}

resource "aws_ecs_task_definition" "drift" {
  count = var.drift_enabled ? 1 : 0

  family                   = "${var.project_name}_${var.environment}_job_drift"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.drift_resources.cpu)
  memory                   = tostring(var.drift_resources.memory)
  execution_role_arn       = lookup(var.drift_roles, "execution", "")
  task_role_arn            = lookup(var.drift_roles, "drift_task", "")
  container_definitions    = jsonencode(local.drift_containers)
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
    for_each = var.drift_resources.ephemeral_storage_gib > 20 ? [var.drift_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "drift_flow" {
  source      = "../services/tests/flow"
  definitions = { for task in aws_ecs_task_definition.drift : "drift" => task.container_definitions }
  tasks = {
    for task in aws_ecs_task_definition.drift : "drift" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "drift_container_definitions" {
  description = "Complete container JSON of the drift task, or null while drift_enabled is false."
  value       = one([for task in aws_ecs_task_definition.drift : task.container_definitions])
}

output "drift_secret_flow" {
  description = "Secret-flow and pin mutation results of the drift task for review and mocked plan assertions."
  value = {
    violations          = module.drift_flow.violations
    containers_checked  = module.drift_flow.containers_checked
    mutation_mismatches = module.drift_flow.mutation_mismatches
  }
}

output "drift_run_task" {
  description = "What a run-task call for the drift task needs, or null while drift_enabled is false. Registration starts nothing."
  value = var.drift_enabled ? {
    task_definition       = aws_ecs_task_definition.drift[0].family
    assign_public_ip      = "DISABLED"
    capacity_providers    = ["FARGATE"]
    propagate_tags        = "TASK_DEFINITION"
    network_source        = "training_network output of the IAM stack"
    container_environment = local.drift_run_variables
  } : null
}

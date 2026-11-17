variable "train_enabled" {
  description = "Supply the inputs of the training host definition. It needs the training and feature store image digests, the execution and training roles and the jobs log group."
  type        = bool
  default     = true
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

variable "mlflow_admin_password_name" {
  description = "Parameter Store SecureString name of the MLflow administrator password. The train container logs in with it. Terraform references the name and never reads the value."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_admin_password"
  nullable    = false

  validation {
    condition     = !var.train_enabled || var.mlflow_admin_password_name == "/${var.project_name}/mlflow_admin_password"
    error_message = "Inject only the project MLflow administrator password parameter."
  }
}

locals {
  train_run_variables = {
    fetch   = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]
    prepare = ["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS", "POLARIS_URI", "MLFLOW_TRACKING_URI"]
    extract = ["RUN_ID", "POLARIS_URI"]
    train   = ["RUN_ID", "MLFLOW_TRACKING_URI"]
  }
  train_phase = "cd /workspace/code && python -m scripts.training.aws_run %s --run \"$RUN_ID\""
  train_steps = [
    { name = "init", image = "training", script = "chown 10001:10001 /workspace /tmp", secrets = [] },
    { name = "fetch", image = "training", script = "python -I /opt/hai/fetch_code.py", secrets = [] },
    { name = "prepare", image = "training", script = format(local.train_phase, "prepare"), secrets = [] },
    { name = "extract", image = "feature_store", script = format(local.train_phase, "extract"), secrets = local.reader_parameters },
    {
      name    = "train"
      image   = "training"
      script  = format(local.train_phase, "train")
      secrets = [{ name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.mlflow_admin_password_name}" }]
    },
  ]
  train_containers = var.train_enabled ? [
    for index, step in local.train_steps : {
      name                   = step.name
      image                  = lookup(var.train_images, step.image, "invalid")
      essential              = step.name == "train"
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = ["sh", "-c"]
      command                = [step.script]
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.host_init_drop : ["ALL"] }, initProcessEnabled = true }
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
        ],
        [for name in lookup(local.train_run_variables, step.name, []) : { name = name, value = "" }],
        step.name == "train" ? [
          { name = "TRAINING_IMAGE_DIGEST", value = split("@", lookup(var.train_images, step.image, "invalid@invalid"))[1] },
          { name = "MLFLOW_TRACKING_USERNAME", value = "admin" },
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
          awslogs-stream-prefix = "train"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ] : []
}

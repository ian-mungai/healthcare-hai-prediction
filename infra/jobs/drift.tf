variable "drift_enabled" {
  description = "Supply the inputs of the drift host definition that drift_monitoring_dag launches. It needs the training, feature store and drift image digests, the execution and drift roles and the jobs log group."
  type        = bool
  default     = true
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

locals {
  drift_run_variables = {
    fetch    = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]
    baseline = ["RUN_ID", "MLFLOW_TRACKING_URI", "POLARIS_URI", "FEATURE_SET"]
    current  = ["RUN_ID", "POLARIS_URI", "INPUT_RUN_ID"]
    check    = ["RUN_ID"]
    publish  = ["RUN_ID"]
  }
  # The baseline phase gates the model named in FEATURE_SET and the current phase reads the model run named in INPUT_RUN_ID (the set the drift check covers).
  drift_phase = "cd /workspace/code && python -m monitoring.aws_run %s --run \"$RUN_ID\"%s"
  drift_steps = [
    { name = "init", image = "training", script = "chown 10001:10001 /workspace /tmp", secrets = [] },
    { name = "fetch", image = "training", script = "python -I /opt/hai/fetch_code.py", secrets = [] },
    {
      name    = "baseline"
      image   = "training"
      script  = format(local.drift_phase, "baseline", " --model \"$FEATURE_SET\"")
      secrets = [{ name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.drift_mlflow_password_name}" }]
    },
    { name = "current", image = "feature_store", script = format(local.drift_phase, "current", " --features-run \"$INPUT_RUN_ID\""), secrets = local.reader_parameters },
    { name = "check", image = "drift", script = format(local.drift_phase, "check", ""), secrets = [] },
    { name = "publish", image = "training", script = format(local.drift_phase, "publish", ""), secrets = [] },
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
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.host_init_drop : ["ALL"] }, initProcessEnabled = true }
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

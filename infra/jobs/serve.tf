variable "serve_enabled" {
  description = "Supply the inputs of the serve host definition that serving_deploy_dag and the promotion chain of research_dag launch. It reuses the training image, the execution role and the MLflow administrator password. It has its own task role and needs the jobs log group."
  type        = bool
  default     = true
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
  default     = "serving/deployments"
  nullable    = false

  validation {
    condition = !var.serve_enabled || (
      can(regex("^[A-Za-z0-9_][A-Za-z0-9_./-]{0,200}[A-Za-z0-9_]$", var.serve_manifest_prefix)) &&
      !startswith(var.serve_manifest_prefix, "lakehouse/") && !strcontains(var.serve_manifest_prefix, "..")
    )
    error_message = "Set serve_manifest_prefix to one key prefix without wildcards, a leading or trailing slash or a parent step, outside the lakehouse prefix."
  }
}

locals {
  # Two steps after the volume hand-over: fetch the run's code, then deploy. The deploy step logs in to MLflow as the train step does. The derived request arrives in
  # SERVING_REQUEST, which the launch preflight built from the closed launch request (the type and the mode), and the tool reads it from there. The time limit is
  # shorter than the job deadline, so the task ends itself first, and every MLflow request has its own short limit.
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
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.host_init_drop : ["ALL"] }, initProcessEnabled = true }
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

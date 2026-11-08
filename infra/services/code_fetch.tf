variable "code_fetch_task_role_arn" {
  description = "IAM output host_code_fetch_role: the task role of the code one-shot. It may read only lakehouse/run_stage/*/inputs/code.tar.gz. Null plans no code one-shot, and host_session start then leaves /data/repo as it is."
  type        = string
  default     = null

  validation {
    condition     = var.code_fetch_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.code_fetch_task_role_arn, "none")))
    error_message = "Supply an IAM role ARN for the code one-shot, or leave code_fetch_task_role_arn null."
  }

  validation {
    condition     = var.code_fetch_task_role_arn == null || var.code_bucket_name != null
    error_message = "The code one-shot needs code_bucket_name, the data bucket that holds the staged code archives."
  }

  validation {
    condition = var.code_fetch_task_role_arn == null || !contains(
      [var.control_plane_task_role_arn, var.polaris_task_role_arn, var.mlflow_task_role_arn, var.broker_task_role_arn, var.openmetadata_task_role_arn], var.code_fetch_task_role_arn
    )
    error_message = "The code one-shot must have its own task role, so no service holds its read of the code archives and it holds none of theirs."
  }
}

variable "code_bucket_name" {
  description = "Name of the data bucket whose lakehouse/run_stage/<run>/inputs/ holds the staged code archives. Required while code_fetch_task_role_arn is set."
  type        = string
  default     = null

  validation {
    condition     = var.code_bucket_name == null || can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", coalesce(var.code_bucket_name, "x")))
    error_message = "Use an S3 bucket name of lowercase letters, digits and dashes."
  }
}

locals {
  code_fetch_on = var.code_fetch_task_role_arn != null

  # The code one-shot runs infra/services/scripts/code_fetch.py, which calls the training image's own /opt/hai/fetch_code.py: the same download,
  # hash check and unpack as the first container of a training task. host_session start passes CODE_RUN_ID and CODE_SHA256 as RunTask overrides.
  code_fetch_task = {
    network_mode  = "host"
    task_role_arn = var.code_fetch_task_role_arn
    volumes       = { data = var.data_mount_path }
    containers = [
      merge(local.container_defaults, {
        name                   = "code_fetch"
        image                  = lookup(local.image, "training", "")
        user                   = "0"
        readonlyRootFilesystem = true
        memory                 = 512
        memoryReservation      = 256
        stopTimeout            = 30
        entryPoint             = ["python", "-I", "-c"]
        command                = [file("${path.module}/scripts/code_fetch.py")]
        environment = [
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_REGION", value = var.aws_region },
          { name = "DATA_BUCKET_NAME", value = coalesce(var.code_bucket_name, "unset") },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "PYTHONUNBUFFERED", value = "1" },
        ]
        mountPoints = [{ sourceVolume = "data", containerPath = "/host", readOnly = false }]
        linuxParameters = {
          capabilities = { add = [], drop = ["ALL"] }
          tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
        }
        restartPolicy    = { enabled = false }
        logConfiguration = lookup(local.log_configuration, "code_fetch", null)
      }),
    ]
  }
}

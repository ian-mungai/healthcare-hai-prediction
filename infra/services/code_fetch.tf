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
      [var.control_plane_task_role_arn, var.polaris_task_role_arn, var.mlflow_task_role_arn, var.openmetadata_task_role_arn], var.code_fetch_task_role_arn
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

  # The code one-shot runs infra/services/scripts/code_fetch.py, which calls the training image's own /opt/hai/fetch_code.py on an empty private folder: the
  # same download, hash check and unpack as the first container of a training task. host_session start passes CODE_RUN_ID and CODE_SHA256 as RunTask
  # overrides, with the same script text as the container command, so the reviewed installer runs even while an older revision is the registered one.
  # CODE_MODE install (the default) renames the private folder to repo_releases/<SHA> only when it holds every member of the archive, then switches repo
  # to it by one rename and keeps the previous link as repo.previous; a refused, failed or interrupted fetch leaves the previous checkout. A selected
  # release that lacks a file is repaired the same way into repo_releases/<SHA>.r<N>, so repo never points at a missing folder. CODE_MODE verify
  # checks that repo shows the release of CODE_SHA256 and that the release holds every file of its archive. CODE_MODE restore shows repo.previous again
  # when repo shows that release or any release that is not complete, and leaves an empty repo folder when no verified release exists. One lock file in
  # /host serializes every mode of every code task on the host; a task that cannot take it within 600 seconds fails and changes nothing.
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

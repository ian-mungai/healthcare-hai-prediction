variable "evidently_ui_enabled" {
  description = "Run the Evidently UI on the host. It shows the snapshots of the on-demand live drift check (drift_live_check_dag, monitoring.live_check), one project for each model and HAI type. It runs the legacy UI of the pinned drift image (Evidently 0.7.23, the report API that drift_check uses), binds the host loopback only, so Session Manager port forwarding is the one way in, and reads the workspace folder of the drift job family read-only. Off by default."
  type        = bool
  default     = false
  nullable    = false
}

variable "evidently_ui_task_role_arn" {
  description = "Task role of the Evidently UI. The UI calls no AWS service, so the role grants nothing. Required when evidently_ui_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.evidently_ui_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.evidently_ui_task_role_arn, "x")))
    error_message = "Supply an IAM role ARN."
  }

  validation {
    condition     = !var.evidently_ui_enabled || var.evidently_ui_task_role_arn != null
    error_message = "Set evidently_ui_task_role_arn when evidently_ui_enabled is true."
  }

  validation {
    condition = var.evidently_ui_task_role_arn == null || !contains(
      compact([var.polaris_task_role_arn, var.control_plane_task_role_arn, var.model_api_task_role_arn, var.streamlit_task_role_arn, var.mlflow_task_role_arn, var.openmetadata_task_role_arn]),
      coalesce(var.evidently_ui_task_role_arn, "x")
    )
    error_message = "The Evidently UI needs its own task role, so it holds none of the permissions of another service."
  }
}

variable "evidently_ui_bucket_name" {
  description = "Name of the data bucket whose drift_demo/ prefix holds the completed live drift check runs. The restore step of the Evidently UI rebuilds the UI workspace from it each time the service starts. Required when evidently_ui_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.evidently_ui_bucket_name == null || can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", coalesce(var.evidently_ui_bucket_name, "!")))
    error_message = "Use an S3 bucket name of lowercase letters, digits, dots and dashes."
  }

  validation {
    condition     = !var.evidently_ui_enabled || var.evidently_ui_bucket_name != null
    error_message = "Set evidently_ui_bucket_name, the data bucket, when evidently_ui_enabled is true."
  }
}

locals {
  evidently_ui_port = 8600
  # The workspace is a folder of the drift job family on the host's scratch volume: infra/jobs mounts <scratch root>/drift/evidently_workspace into the drift check
  # (the one writer) and this service mounts the same folder read-only. The services stack's data mount and scratch folder name and the jobs stack's host_scratch_root
  # (/data/job_scratch) name the same place.
  evidently_ui_workspace = "${var.data_mount_path}/${var.host_scratch_folder}/drift/evidently_workspace"

  # The memory sizes of the UI are measured: the legacy UI idled at 237 MiB with four snapshots and at 232 MiB with none, under a 512 MiB limit, in the pinned drift
  # image (docker stats, Nov 17 2026). The restore step runs once before the UI and holds a Python process and boto3 for a few seconds, so it reserves 16 MiB (like
  # the monitoring preparation step) and may use 256 MiB. Both reservations come off the host job room (host_job_cap_mib).
  evidently_ui_containers = [for container in [
    # The restore step rebuilds the workspace folder from the completed runs under drift_demo/ in S3 (monitoring.live_restore, run from the reviewed checkout). It runs as
    # root with only the capabilities to write and hand over the folder, and the folder then belongs to the user that the UI and the drift check run as. The task role
    # lets it read and list drift_demo/ and nothing else, and the root file system is read-only.
    merge(local.container_defaults, {
      name                   = "evidently_ui_restore"
      image                  = lookup(local.image, "training", "")
      essential              = false
      user                   = "0:0"
      readonlyRootFilesystem = true
      memory                 = 256
      memoryReservation      = 16
      stopTimeout            = 30
      entryPoint             = ["sh", "-c"]
      command                = ["cd /opt/hai/repo && python -m monitoring.live_restore --bucket \"$DATA_BUCKET_NAME\" --workspace /workspace && chown -R 10001:10001 /workspace"]
      environment = [
        for name, value in {
          AWS_REGION                = var.aws_region
          AWS_DEFAULT_REGION        = var.aws_region
          AWS_EC2_METADATA_DISABLED = "true"
          DATA_BUCKET_NAME          = var.evidently_ui_bucket_name
          HOME                      = "/tmp"
          PYTHONDONTWRITEBYTECODE   = "1"
        } : { name = name, value = value }
      ]
      mountPoints = [
        { sourceVolume = "evidently_workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "code_repo", containerPath = "/opt/hai/repo", readOnly = true },
      ]
      linuxParameters = {
        initProcessEnabled = true
        capabilities       = { drop = ["ALL"], add = ["CHOWN", "DAC_OVERRIDE", "FOWNER"] }
        tmpfs              = [{ containerPath = "/tmp", size = 64, mountOptions = ["noexec", "nosuid"] }]
      }
      logConfiguration = lookup(local.log_configuration, "evidently_ui_restore", null)
    }),
    merge(local.container_defaults, {
      name                   = "evidently_ui"
      image                  = lookup(local.image, "drift", "")
      user                   = "10001:10001"
      readonlyRootFilesystem = true
      memory                 = 512
      memoryReservation      = 256
      stopTimeout            = 30
      command                = ["python", "-c", "from evidently.legacy.cli import app; app()", "ui", "--host", "127.0.0.1", "--port", tostring(local.evidently_ui_port), "--workspace", "/workspace"]
      environment            = [for name, value in { DO_NOT_TRACK = "1", EVIDENTLY_DISABLE_TELEMETRY = "1", HOME = "/tmp" } : { name = name, value = value }]
      mountPoints            = [{ sourceVolume = "evidently_workspace", containerPath = "/workspace", readOnly = true }]
      portMappings           = [{ containerPort = local.evidently_ui_port, hostPort = local.evidently_ui_port, protocol = "tcp" }]
      dependsOn              = [{ containerName = "evidently_ui_restore", condition = "SUCCESS" }]
      linuxParameters = {
        initProcessEnabled = true
        tmpfs              = [{ containerPath = "/tmp", size = 64, mountOptions = ["noexec", "nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request as u; u.urlopen('http://127.0.0.1:${local.evidently_ui_port}/api/version', timeout=3)"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 30
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "evidently_ui", null)
    }),
  ] : container if var.evidently_ui_enabled]
}

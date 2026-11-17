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

locals {
  evidently_ui_port = 8600
  # The workspace is a folder of the drift job family on the host's scratch volume: infra/jobs mounts <scratch root>/drift/evidently_workspace into the drift check
  # (the one writer) and this service mounts the same folder read-only. The services stack's data mount and scratch folder name and the jobs stack's host_scratch_root
  # (/data/job_scratch) name the same place.
  evidently_ui_workspace = "${var.data_mount_path}/${var.host_scratch_folder}/drift/evidently_workspace"

  # The memory sizes are measured: the legacy UI idled at 237 MiB with four snapshots and at 232 MiB with none, under a 512 MiB limit, in the pinned drift image
  # (docker stats, Nov 17 2026). The reservation comes off the host job room (host_job_cap_mib).
  evidently_ui_containers = var.evidently_ui_enabled ? [
    merge(local.container_defaults, {
      name                   = "evidently_ui"
      image                  = local.image["drift"]
      user                   = "10001:10001"
      readonlyRootFilesystem = true
      memory                 = 512
      memoryReservation      = 256
      stopTimeout            = 30
      command                = ["python", "-c", "from evidently.legacy.cli import app; app()", "ui", "--host", "127.0.0.1", "--port", tostring(local.evidently_ui_port), "--workspace", "/workspace"]
      environment            = [for name, value in { DO_NOT_TRACK = "1", EVIDENTLY_DISABLE_TELEMETRY = "1", HOME = "/tmp" } : { name = name, value = value }]
      mountPoints            = [{ sourceVolume = "evidently_workspace", containerPath = "/workspace", readOnly = true }]
      portMappings           = [{ containerPort = local.evidently_ui_port, hostPort = local.evidently_ui_port, protocol = "tcp" }]
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
      logConfiguration = local.log_configuration["evidently_ui"]
    }),
  ] : []
}

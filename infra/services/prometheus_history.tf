locals {
  prometheus_history_on = var.monitoring_enabled && var.prometheus_history_task_role_arn != null

  # Prometheus keeps its blocks at 2h (min and max block duration the same), so a finished block is never rewritten into a larger one and the shipper can create it once.
  prometheus_history_flags = local.prometheus_history_on ? ["--storage.tsdb.min-block-duration=2h", "--storage.tsdb.max-block-duration=2h"] : []

  prometheus_history_environment = [
    { name = "AWS_REGION", value = var.aws_region },
    { name = "HISTORY_BUCKET_NAME", value = coalesce(var.prometheus_history_bucket_name, "unset") },
    { name = "HOME", value = "/tmp" },
    { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
    { name = "PYTHONUNBUFFERED", value = "1" },
  ]

  # The shipper reads the Prometheus data folder read only and creates each finished block once in S3 (monitoring/prometheus_history.py, from the code checkout in the
  # Airflow image). It is its own service so that its task role is the only one with the write. It exits when a pass fails, and ECS restarts it.
  prometheus_history_containers = [
    merge(local.container_defaults, {
      name                   = "prometheus_history"
      image                  = lookup(local.image, "airflow", "")
      user                   = "65534:65534"
      readonlyRootFilesystem = true
      memory                 = var.prometheus_history_memory_mib.limit
      memoryReservation      = var.prometheus_history_memory_mib.reservation
      stopTimeout            = 30
      workingDirectory       = "/opt/hai"
      entryPoint             = ["python"]
      command                = ["-m", "monitoring.prometheus_history", "ship", "--data-dir", "/prometheus", "--interval", tostring(var.prometheus_history_interval_seconds)]
      environment            = local.prometheus_history_environment
      mountPoints = [
        { sourceVolume = "prometheus_data", containerPath = "/prometheus", readOnly = true },
        { sourceVolume = "code_monitoring_package", containerPath = "/opt/hai/monitoring", readOnly = true },
      ]
      linuxParameters = {
        capabilities = { drop = ["ALL"] }
        tmpfs        = [{ containerPath = "/tmp", size = 16, mountOptions = ["rw", "noexec", "nosuid"] }]
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "prometheus_history", null)
    }),
  ]

  # The restore one-shot (host_session start runs it after the code checkout, while the monitoring service and the shipper are held at zero and verified stopped).
  # It has the read-only role and downloads, verifies and places the blocks the data folder lacks, then hands them to the Prometheus user. The data volume holds both /host/prometheus and the scratch folder, so each block moves by one rename.
  prometheus_restore_task = {
    network_mode  = "host"
    task_role_arn = var.prometheus_restore_task_role_arn
    volumes = {
      data                    = var.data_mount_path
      code_monitoring_package = "${local.code_path}/monitoring"
    }
    containers = [
      merge(local.container_defaults, {
        name                   = "prometheus_restore"
        image                  = lookup(local.image, "airflow", "")
        user                   = "0"
        readonlyRootFilesystem = true
        memory                 = 256
        memoryReservation      = 64
        stopTimeout            = 30
        workingDirectory       = "/opt/hai"
        entryPoint             = ["python"]
        command                = ["-m", "monitoring.prometheus_history", "restore", "--data-dir", "/host/prometheus", "--scratch-dir", "/host/prometheus_restore", "--owner", "65534:65534"]
        environment            = local.prometheus_history_environment
        mountPoints = [
          { sourceVolume = "data", containerPath = "/host", readOnly = false },
          { sourceVolume = "code_monitoring_package", containerPath = "/opt/hai/monitoring", readOnly = true },
        ]
        linuxParameters = {
          capabilities = { drop = ["ALL"], add = ["CHOWN", "DAC_OVERRIDE", "FOWNER"] }
          tmpfs        = [{ containerPath = "/tmp", size = 16, mountOptions = ["rw", "noexec", "nosuid"] }]
        }
        restartPolicy    = { enabled = false }
        logConfiguration = lookup(local.log_configuration, "prometheus_restore", null)
      }),
    ]
  }
}

locals {
  broker_port         = 18790
  broker_state_path   = "${var.data_mount_path}/broker"
  broker_code_mount   = "/opt/hai/repo"
  broker_state_mount  = "/var/lib/hai/broker"
  broker_session_path = "/opt/hai/broker"

  # The broker runs the Airflow image on the host with the repository checkout mounted read-only. Its listener binds loopback and every request needs the token from Parameter Store.
  # It starts job tasks on Fargate through the task role broker_task, which names the job families it may run. The state folder holds the ledger.
  broker_containers = var.broker_enabled ? [
    merge(local.container_defaults, {
      name                   = "broker"
      image                  = local.image["airflow"]
      user                   = "50000:0"
      readonlyRootFilesystem = true
      memory                 = 256
      memoryReservation      = 128
      stopTimeout            = 30
      workingDirectory       = local.broker_code_mount
      entryPoint             = ["python", "-m", "scripts.orchestration.ecs_broker"]
      command                = []
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "BROKER_BUCKET", value = try(var.broker_jobs.bucket_name, "unset") },
        { name = "BROKER_CLUSTER", value = var.cluster_name },
        { name = "BROKER_FAMILIES", value = join(",", try(var.broker_jobs.families, [])) },
        { name = "BROKER_FAMILY_PREFIX", value = "${var.project_name}_${var.environment}_job_" },
        { name = "BROKER_LOG_GROUP", value = try(var.broker_jobs.log_group_name, "unset") },
        { name = "BROKER_MLFLOW_URI", value = "http://${try(var.broker_jobs.host_ip, "unset")}:5100" },
        { name = "BROKER_POLARIS_URI", value = "http://${try(var.broker_jobs.host_ip, "unset")}:8181/api/catalog" },
        { name = "BROKER_PORT", value = tostring(local.broker_port) },
        { name = "BROKER_PROJECT_TAG", value = var.project_tag },
        { name = "BROKER_SECURITY_GROUPS", value = try(var.broker_jobs.security_group_id, "unset") },
        { name = "BROKER_STATE", value = local.broker_state_mount },
        { name = "BROKER_SUBNETS", value = try(var.broker_jobs.subnet_id, "unset") },
        { name = "HOME", value = "/tmp" },
        { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
        { name = "PYTHONPATH", value = local.broker_code_mount },
      ]
      secrets = [{ name = "BROKER_TOKEN", valueFrom = local.secret_arn["broker_token"] }]
      mountPoints = [
        { sourceVolume = "code_repo", containerPath = local.broker_code_mount, readOnly = true },
        { sourceVolume = "broker_state", containerPath = local.broker_state_mount, readOnly = false },
      ]
      linuxParameters = {
        capabilities = { add = [], drop = ["ALL"] }
        tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD", "python", "-c", "import socket\nsocket.create_connection(('127.0.0.1', ${local.broker_port}), 2).close()"]
        interval    = 15
        timeout     = 5
        retries     = 10
        startPeriod = 30
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "broker", null)
    }),
  ] : []

  # The unchanged DAG client reads /opt/hai/broker/client.json and broker_token. The scheduler writes both into a private memory-backed folder when it starts.
  broker_session_script = <<-EOT
    umask 077
    printf '{"url": "http://127.0.0.1:${local.broker_port}"}' > ${local.broker_session_path}/client.json
    printf '%s' "$BROKER_TOKEN" > ${local.broker_session_path}/broker_token
  EOT
}

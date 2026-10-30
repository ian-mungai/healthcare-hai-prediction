locals {
  mlflow_port = 5100

  # The server proxies artifact uploads (--serve-artifacts), so a training job never holds an S3 permission on the mlflow/ prefix. The backend store is the host
  # Postgres on loopback. The database password reaches the server only as the task secret PGPASSWORD, which libpq reads. Telemetry stays off.
  mlflow_containers = [
    merge(local.container_defaults, {
      name                   = "mlflow"
      image                  = lookup(local.image, "mlflow", "")
      user                   = "10005:10005"
      readonlyRootFilesystem = true
      memory                 = 640
      memoryReservation      = 384
      stopTimeout            = 60
      entryPoint             = ["mlflow", "server"]
      command = [
        "--host", "0.0.0.0",
        "--port", tostring(local.mlflow_port),
        "--workers", "1",
        "--serve-artifacts",
        "--artifacts-destination", "s3://${coalesce(var.mlflow_artifact_bucket, "unset")}/mlflow/",
        "--allowed-hosts", "127.0.0.1:${local.mlflow_port},localhost:${local.mlflow_port},${coalesce(var.mlflow_host_ip, "unset")}:${local.mlflow_port}",
      ]
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "DO_NOT_TRACK", value = "true" },
        { name = "HOME", value = "/tmp" },
        { name = "MLFLOW_BACKEND_STORE_URI", value = "postgresql+psycopg2://mlflow@127.0.0.1:5432/mlflow" },
        { name = "MLFLOW_DISABLE_TELEMETRY", value = "true" },
      ]
      secrets      = [{ name = "PGPASSWORD", valueFrom = local.secret_arn["mlflow_db_password"] }]
      portMappings = [{ containerPort = local.mlflow_port, hostPort = local.mlflow_port, protocol = "tcp" }]
      linuxParameters = {
        capabilities = { add = [], drop = ["ALL"] }
        tmpfs        = [{ containerPath = "/tmp", size = 256, mountOptions = ["nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${local.mlflow_port}/health', timeout=2).close()"]
        interval    = 15
        timeout     = 5
        retries     = 10
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "mlflow", null)
    }),
  ]
}

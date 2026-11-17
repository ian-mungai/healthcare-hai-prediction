locals {
  # Configuration comes from the repository checkout the Airflow task already mounts (dag_code_path), as read-only folders. State lives on the data volume. A new
  # dashboard or alert is a change in the checkout and a restart of the monitoring service.
  monitoring_volumes = {
    code_monitoring_grafana_dashboards   = "${local.code_path}/monitoring/grafana/dashboards"
    code_monitoring_grafana_provisioning = "${local.code_path}/monitoring/grafana/provisioning"
    code_monitoring_prometheus           = "${local.code_path}/monitoring/prometheus"
    code_monitoring_statsd               = "${local.code_path}/monitoring/statsd"
    code_monitoring_package              = "${local.code_path}/monitoring"
    grafana_data                         = "${var.data_mount_path}/grafana"
    host_data                            = var.data_mount_path
    node_data                            = var.data_mount_path
    node_scratch                         = "${var.data_mount_path}/${var.host_scratch_folder}"
    prometheus_data                      = "${var.data_mount_path}/prometheus"
    pushgateway_data                     = "${var.data_mount_path}/pushgateway"
  }

  # The Go runtime collects garbage harder as the heap nears this soft limit, which keeps each process below its hard limit. It is 80 percent of the hard limit.
  monitoring_gomemlimit = {
    for name, memory in var.monitoring_memory_mib : name => "${floor(memory.limit * 0.8)}MiB"
  }

  # The folders are created by Docker as root when the task first starts. The preparation container hands each one to the user of its service and exits, and
  # Prometheus and Grafana wait for it.
  monitoring_prepare_script = <<-EOT
    chown 65534:65534 /prometheus
    chown 472:0 /grafana
    chown 65534:65534 /pushgateway
    chmod 0750 /prometheus /grafana /pushgateway
  EOT

  monitoring_containers = [for container in [
    merge(local.container_defaults, {
      name                   = "monitoring_prepare"
      image                  = lookup(local.image, "prometheus", "")
      essential              = false
      user                   = "0"
      readonlyRootFilesystem = true
      memory                 = 32
      memoryReservation      = 16
      stopTimeout            = 30
      entryPoint             = ["sh", "-ec"]
      command                = [local.monitoring_prepare_script]
      mountPoints = [
        { sourceVolume = "prometheus_data", containerPath = "/prometheus", readOnly = false },
        { sourceVolume = "grafana_data", containerPath = "/grafana", readOnly = false },
        { sourceVolume = "pushgateway_data", containerPath = "/pushgateway", readOnly = false },
      ]
      linuxParameters = {
        capabilities = { drop = ["ALL"], add = ["CHOWN", "DAC_OVERRIDE", "FOWNER"] }
      }
      logConfiguration = lookup(local.log_configuration, "monitoring_prepare", null)
    }),
    merge(local.container_defaults, {
      name              = "prometheus"
      image             = lookup(local.image, "prometheus", "")
      user              = "65534:65534"
      memory            = var.monitoring_memory_mib.prometheus.limit
      memoryReservation = var.monitoring_memory_mib.prometheus.reservation
      stopTimeout       = 60
      command = [
        "--config.file=/etc/prometheus/prometheus.yml",
        "--storage.tsdb.path=/prometheus",
        "--storage.tsdb.retention.time=${var.prometheus_retention_days}d",
        "--storage.tsdb.retention.size=${var.prometheus_retention_size_mb}MB",
        "--web.listen-address=127.0.0.1:9090",
        "--query.max-concurrency=4",
      ]
      environment = [{ name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.prometheus }]
      mountPoints = [
        { sourceVolume = "code_monitoring_prometheus", containerPath = "/etc/prometheus", readOnly = true },
        { sourceVolume = "prometheus_data", containerPath = "/prometheus", readOnly = false },
      ]
      portMappings = [{ containerPort = 9090, hostPort = 9090, protocol = "tcp" }]
      dependsOn    = [{ containerName = "monitoring_prepare", condition = "SUCCESS" }]
      healthCheck = {
        command     = ["CMD", "wget", "--quiet", "--output-document=/dev/null", "http://127.0.0.1:9090/-/healthy"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 30
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "prometheus", null)
    }),
    merge(local.container_defaults, {
      name              = "grafana"
      image             = lookup(local.image, "grafana", "")
      user              = "472:0"
      memory            = var.monitoring_memory_mib.grafana.limit
      memoryReservation = var.monitoring_memory_mib.grafana.reservation
      stopTimeout       = 30
      environment = [
        { name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.grafana },
        { name = "GF_SERVER_HTTP_ADDR", value = "127.0.0.1" },
        { name = "GF_SERVER_HTTP_PORT", value = "3001" },
        { name = "GF_SECURITY_ADMIN_USER", value = "admin" },
        { name = "GF_SECURITY_DISABLE_GRAVATAR", value = "true" },
        { name = "GF_USERS_ALLOW_SIGN_UP", value = "false" },
        { name = "GF_AUTH_ANONYMOUS_ENABLED", value = "false" },
        { name = "GF_ANALYTICS_REPORTING_ENABLED", value = "false" },
        { name = "GF_ANALYTICS_CHECK_FOR_UPDATES", value = "false" },
        { name = "GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES", value = "false" },
        { name = "GF_PLUGINS_PREINSTALL_DISABLED", value = "true" },
        { name = "GF_SNAPSHOTS_EXTERNAL_ENABLED", value = "false" },
        { name = "GF_PATHS_PROVISIONING", value = "/etc/grafana/provisioning" },
        { name = "GF_LOG_MODE", value = "console" },
        # The alert contact point (provisioning/alerting/contact_points.yml) reads the topic and the region from these two variables.
        { name = "ALERTS_TOPIC_ARN", value = var.alerts_topic_arn },
        { name = "AWS_REGION", value = var.aws_region },
      ]
      secrets = [{ name = "GF_SECURITY_ADMIN_PASSWORD", valueFrom = local.secret_arn["grafana_admin_password"] }]
      mountPoints = [
        { sourceVolume = "code_monitoring_grafana_provisioning", containerPath = "/etc/grafana/provisioning", readOnly = true },
        { sourceVolume = "code_monitoring_grafana_dashboards", containerPath = "/etc/grafana/dashboards", readOnly = true },
        { sourceVolume = "grafana_data", containerPath = "/var/lib/grafana", readOnly = false },
      ]
      portMappings = [{ containerPort = 3001, hostPort = 3001, protocol = "tcp" }]
      dependsOn    = [{ containerName = "monitoring_prepare", condition = "SUCCESS" }]
      healthCheck = {
        command     = ["CMD", "bash", "-ec", "exec 3<>/dev/tcp/127.0.0.1/3001; printf 'GET /api/health HTTP/1.0\\r\\nHost: localhost\\r\\n\\r\\n' >&3; read -r status <&3; [[ $status == *' 200 '* ]]"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "grafana", null)
    }),
    merge(local.container_defaults, {
      name              = "statsd_exporter"
      image             = lookup(local.image, "statsd_exporter", "")
      user              = "65534:65534"
      memory            = var.monitoring_memory_mib.statsd_exporter.limit
      memoryReservation = var.monitoring_memory_mib.statsd_exporter.reservation
      stopTimeout       = 30
      command = [
        "--statsd.mapping-config=/etc/statsd/mapping.yml",
        "--statsd.listen-udp=127.0.0.1:9125",
        "--statsd.listen-tcp=",
        "--statsd.listen-unixgram=",
        "--web.listen-address=127.0.0.1:9102",
      ]
      environment = [{ name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.statsd_exporter }]
      mountPoints = [{ sourceVolume = "code_monitoring_statsd", containerPath = "/etc/statsd", readOnly = true }]
      portMappings = [
        { containerPort = 9102, hostPort = 9102, protocol = "tcp" },
        { containerPort = 9125, hostPort = 9125, protocol = "udp" },
      ]
      healthCheck = {
        command     = ["CMD", "wget", "--quiet", "--output-document=/dev/null", "http://127.0.0.1:9102/metrics"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 15
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "statsd_exporter", null)
    }),
    # Host jobs push their own metrics here at start, by heartbeat and at exit (scripts/metrics_push.py). The bridge jobs reach it at the host private address, which
    # the job firewall opens (infra/iam/host_job_firewall.nft); Prometheus scrapes it on loopback. It has no authentication, so only the host and the job bridge
    # may reach it. The last values persist on the data volume across a restart.
    merge(local.container_defaults, {
      name              = "pushgateway"
      image             = lookup(local.image, "pushgateway", "")
      user              = "65534:65534"
      memory            = var.monitoring_memory_mib.pushgateway.limit
      memoryReservation = var.monitoring_memory_mib.pushgateway.reservation
      stopTimeout       = 30
      command = [
        "--web.listen-address=0.0.0.0:9091",
        "--persistence.file=/pushgateway/pushgateway.db",
        "--persistence.interval=1m",
      ]
      environment  = [{ name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.pushgateway }]
      mountPoints  = [{ sourceVolume = "pushgateway_data", containerPath = "/pushgateway", readOnly = false }]
      portMappings = [{ containerPort = 9091, hostPort = 9091, protocol = "tcp" }]
      dependsOn    = [{ containerName = "monitoring_prepare", condition = "SUCCESS" }]
      healthCheck = {
        command     = ["CMD", "wget", "--quiet", "--output-document=/dev/null", "http://127.0.0.1:9091/-/healthy"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 15
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "pushgateway", null)
    }),
    # Live reads of the host capacity (ECS) and of the OpenMetadata health, exposed on loopback (monitoring/host_state_exporter.py). It runs from the code checkout in the
    # Airflow image and reads AWS with the task role.
    merge(local.container_defaults, {
      name                   = "host_state_exporter"
      image                  = lookup(local.image, "airflow", "")
      user                   = "50000:0"
      readonlyRootFilesystem = true
      memory                 = var.monitoring_memory_mib.host_state_exporter.limit
      memoryReservation      = var.monitoring_memory_mib.host_state_exporter.reservation
      stopTimeout            = 30
      workingDirectory       = "/opt/hai"
      entryPoint             = ["python"]
      command                = ["-m", "monitoring.host_state_exporter", "--listen", "127.0.0.1:9103", "--interval", "120"]
      environment = concat([
        { name = "ECS_CLUSTER", value = var.cluster_name },
        { name = "AWS_REGION", value = var.aws_region },
        { name = "HAI_LAUNCH_HOST_JOB_CAP_MIB", value = tostring(local.host_job_memory_mib) },
        { name = "ECS_SERVICE_PREFIX", value = "${local.name_prefix}_" },
        { name = "HOST_DATA_PATH", value = "/host/data" },
        { name = "HOST_SCRATCH_PATH", value = "/host/data/${var.host_scratch_folder}" },
        { name = "MLFLOW_METRICS_URL", value = "http://127.0.0.1:${local.mlflow_port}/metrics" },
        { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
        { name = "HOME", value = "/tmp" },
      ], local.monitoring_gap_environment)
      # MLflow answers /metrics to its admin user only, so the exporter relays that one path with the admin credential, which it reads from Parameter Store at
      # start and keeps in memory. Only this container of the monitoring task holds it; the MLflow service holds it already.
      secrets = var.mlflow_enabled ? [{ name = "MLFLOW_ADMIN_PASSWORD", valueFrom = local.secret_arn["mlflow_admin_password"] }] : []
      mountPoints = [
        { sourceVolume = "code_monitoring_package", containerPath = "/opt/hai/monitoring", readOnly = true },
        { sourceVolume = "host_data", containerPath = "/host/data", readOnly = true },
      ]
      portMappings = [{ containerPort = 9103, hostPort = 9103, protocol = "tcp" }]
      linuxParameters = {
        capabilities = { drop = ["ALL"] }
        tmpfs        = [{ containerPath = "/tmp", size = 16, mountOptions = ["rw", "noexec", "nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9103/metrics', timeout=3)"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 30
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "host_state_exporter", null)
    }),
    # The three exporters of the services that have no metrics endpoint of their own. Each is optional (a failure never stops the task), restarts and listens on
    # loopback. The Redis and Postgres exporters log in with their own users, and the password comes from Parameter Store at start. The images have no shell, so
    # none has a container health check. Verified Nov 17 2026 (data/local_keep/builds_20261117/verification_child).
    merge(local.container_defaults, {
      name                   = "redis_exporter"
      image                  = lookup(local.image, "redis_exporter", "")
      essential              = false
      user                   = "65534:65534"
      readonlyRootFilesystem = true
      memory                 = var.monitoring_memory_mib.redis_exporter.limit
      memoryReservation      = var.monitoring_memory_mib.redis_exporter.reservation
      stopTimeout            = 30
      command                = ["--web.listen-address=127.0.0.1:9121", "--config-command=-"]
      environment = [
        { name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.redis_exporter },
        { name = "REDIS_ADDR", value = "redis://127.0.0.1:6379" },
        { name = "REDIS_USER", value = "exporter" },
      ]
      secrets          = [{ name = "REDIS_PASSWORD", valueFrom = local.secret_arn["redis_exporter_password"] }]
      portMappings     = [{ containerPort = 9121, hostPort = 9121, protocol = "tcp" }]
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "redis_exporter", null)
    }),
    merge(local.container_defaults, {
      name                   = "postgres_exporter"
      image                  = lookup(local.image, "postgres_exporter", "")
      essential              = false
      user                   = "65534:65534"
      readonlyRootFilesystem = true
      memory                 = var.monitoring_memory_mib.postgres_exporter.limit
      memoryReservation      = var.monitoring_memory_mib.postgres_exporter.reservation
      stopTimeout            = 30
      command = [
        "--web.listen-address=127.0.0.1:9187",
        "--collector.settings",
        "--no-collector.stat_user_tables",
        "--no-collector.statio_user_tables",
        "--no-collector.replication",
        "--no-collector.replication_slots",
        "--no-collector.stat_replication",
        "--no-collector.stat_archiver",
        "--no-collector.stat_progress_vacuum",
        "--no-collector.roles",
        "--collector.database_wraparound",
        "--collector.long_running_transactions",
        "--collector.postmaster",
      ]
      environment = [
        { name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.postgres_exporter },
        { name = "DATA_SOURCE_URI", value = "127.0.0.1:5432/polaris?sslmode=disable" },
        { name = "DATA_SOURCE_USER", value = "postgres_exporter" },
      ]
      secrets          = [{ name = "DATA_SOURCE_PASS", valueFrom = local.secret_arn["postgres_exporter_password"] }]
      portMappings     = [{ containerPort = 9187, hostPort = 9187, protocol = "tcp" }]
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "postgres_exporter", null)
    }),
    merge(local.container_defaults, {
      name                   = "node_exporter"
      image                  = lookup(local.image, "node_exporter", "")
      essential              = false
      user                   = "65534:65534"
      readonlyRootFilesystem = true
      memory                 = var.monitoring_memory_mib.node_exporter.limit
      memoryReservation      = var.monitoring_memory_mib.node_exporter.reservation
      stopTimeout            = 30
      command = [
        "--web.listen-address=127.0.0.1:9100",
        "--path.rootfs=/host",
        "--collector.disable-defaults",
        "--collector.cpu",
        "--collector.meminfo",
        "--collector.loadavg",
        "--collector.diskstats",
        "--collector.filesystem",
        "--collector.vmstat",
        "--collector.vmstat.fields=^(oom_kill|pgmajfault|pswpin|pswpout)$",
        "--collector.filesystem.mount-points-include=^/(data|scratch)$",
        "--collector.diskstats.device-include=^(nvme[0-9]+n[0-9]+|xvd[a-z]+)$",
      ]
      environment = [{ name = "GOMEMLIMIT", value = local.monitoring_gomemlimit.node_exporter }]
      mountPoints = [
        { sourceVolume = "node_data", containerPath = "/host/data", readOnly = true },
        { sourceVolume = "node_scratch", containerPath = "/host/scratch", readOnly = true },
      ]
      portMappings     = [{ containerPort = 9100, hostPort = 9100, protocol = "tcp" }]
      linuxParameters  = { capabilities = { drop = ["ALL"] } }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "node_exporter", null)
    }),
  ] : container if var.monitoring_enabled]
}

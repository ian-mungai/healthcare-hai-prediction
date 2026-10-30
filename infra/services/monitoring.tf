locals {
  # Configuration comes from the repository checkout the Airflow task already mounts (dag_code_path), as read-only folders. State lives on the data volume. A new
  # dashboard or alert is a change in the checkout and a restart of the monitoring service.
  monitoring_volumes = {
    code_monitoring_grafana_dashboards   = "${local.code_path}/monitoring/grafana/dashboards"
    code_monitoring_grafana_provisioning = "${local.code_path}/monitoring/grafana/provisioning"
    code_monitoring_prometheus           = "${local.code_path}/monitoring/prometheus"
    code_monitoring_statsd               = "${local.code_path}/monitoring/statsd"
    grafana_data                         = "${var.data_mount_path}/grafana"
    prometheus_data                      = "${var.data_mount_path}/prometheus"
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
    chmod 0750 /prometheus /grafana
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
  ] : container if var.monitoring_enabled]
}

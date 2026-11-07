locals {
  om_on           = var.openmetadata_persistent_enabled
  om_api_port     = 8585
  om_admin_port   = 8586
  om_search_port  = 9200
  om_search_data  = "${var.data_mount_path}/opensearch"
  om_code_mount   = "/opt/hai/repo"
  om_bootstrap_py = "${local.om_code_mount}/scripts/infrastructure/openmetadata_persistent_bootstrap.py"

  # Memory profile. The seven host services reserve their measured use rounded up to 64 MiB and keep the limits they had (D19, measured during the real gold run on Nov 3 2026,
  # data/local_keep/gold_20261103/host_memory_sample.txt; the host is r7g.xlarge with 32 GiB), in both profiles. The services that were not measured (the Airflow migration, Marquez and the
  # Polaris bootstrap) take the lowered values below when the flag is on and the deployed ones when it is off. A peak sample under load on the new host sets the final limits.
  memory_profile = {
    postgres = { limit = 2048, reservation = 128 }
    # Redis keeps maxmemory 2048mb with noeviction for the online feature store, so its limit is that plus 256 MiB.
    redis             = { limit = 2304, reservation = 64 }
    polaris           = { limit = 2048, reservation = 320 }
    airflow_migrate   = { limit = local.om_on ? 512 : 640, reservation = 256 }
    airflow_apiserver = { limit = 1536, reservation = 256 }
    airflow_scheduler = { limit = 1536, reservation = 320 }
    airflow_dag_proc  = { limit = 1280, reservation = 192 }
    airflow_triggerer = { limit = 768, reservation = 256 }
    marquez_api       = { limit = local.om_on ? 1024 : 1280, reservation = local.om_on ? 512 : 768 }
    marquez_web       = { limit = local.om_on ? 192 : 256, reservation = local.om_on ? 96 : 128 }
    polaris_bootstrap = { limit = local.om_on ? 512 : 1024, reservation = local.om_on ? 256 : 512 }
  }

  om_log_options = { for name in ["opensearch", "openmetadata_migrate", "openmetadata_server", "openmetadata_bootstrap"] : name => lookup(local.log_configuration, name, null) }

  om_database_environment = [
    { name = "DB_HOST", value = "127.0.0.1" },
    { name = "DB_PORT", value = "5432" },
    { name = "DB_SCHEME", value = "postgresql" },
    { name = "DB_DRIVER_CLASS", value = "org.postgresql.Driver" },
    { name = "DB_USER", value = "openmetadata_user" },
    { name = "OM_DATABASE", value = "openmetadata_db" },
    { name = "DB_USE_SSL", value = "false" },
  ]

  om_server_environment = concat(local.om_database_environment, [
    { name = "SEARCH_TYPE", value = "opensearch" },
    { name = "ELASTICSEARCH_HOST", value = "127.0.0.1" },
    { name = "ELASTICSEARCH_PORT", value = tostring(local.om_search_port) },
    { name = "ELASTICSEARCH_SCHEME", value = "http" },
    { name = "AUTHENTICATION_PROVIDER", value = "basic" },
    { name = "AUTHORIZER_ADMIN_PRINCIPALS", value = "[admin]" },
    { name = "AUTHORIZER_PRINCIPAL_DOMAIN", value = "open-metadata.org" },
    { name = "OPENMETADATA_HEAP_OPTS", value = "-Xmx${var.openmetadata_memory_mib.server.heap_mib}m -Xms${var.openmetadata_memory_mib.server.heap_mib}m" },
    # Without a pipeline service client the server cannot deploy or trigger ingestion pipelines, so nobody can start a profiler, sampling or auto-classification run from the UI.
    { name = "PIPELINE_SERVICE_CLIENT_ENABLED", value = "false" },
  ])

  om_db_secret = [{ name = "DB_USER_PASSWORD", valueFrom = local.secret_arn["openmetadata_db_password"] }]

  # The task runs OpenSearch first, then the schema migration once, then the server. A restart of any essential container restarts the task, so the order always holds.
  openmetadata_containers = [for container in [
    merge(local.container_defaults, {
      name              = "opensearch"
      image             = lookup(local.image, "openmetadata_search", "")
      user              = "1000:1000"
      memory            = var.openmetadata_memory_mib.search.limit
      memoryReservation = var.openmetadata_memory_mib.search.reservation
      stopTimeout       = 60
      entryPoint        = var.openmetadata_commands.search
      command           = []
      environment = [
        { name = "discovery.type", value = "single-node" },
        { name = "network.host", value = "127.0.0.1" },
        { name = "http.port", value = tostring(local.om_search_port) },
        { name = "DISABLE_SECURITY_PLUGIN", value = "true" },
        { name = "DISABLE_INSTALL_DEMO_CONFIG", value = "true" },
        { name = "bootstrap.memory_lock", value = "false" },
        { name = "node.store.allow_mmap", value = "false" },
        { name = "OPENSEARCH_JAVA_OPTS", value = "-Xms${var.openmetadata_memory_mib.search.heap_mib}m -Xmx${var.openmetadata_memory_mib.search.heap_mib}m" },
      ]
      mountPoints = [{ sourceVolume = "opensearch_data", containerPath = "/usr/share/opensearch/data", readOnly = false }]
      ulimits = [
        { name = "core", softLimit = 0, hardLimit = 0 },
        { name = "nofile", softLimit = 65536, hardLimit = 65536 },
      ]
      healthCheck = {
        command     = ["CMD", "curl", "--fail", "--silent", "--max-time", "8", "http://127.0.0.1:${local.om_search_port}/_cluster/health?wait_for_status=yellow&timeout=5s"]
        interval    = 15
        timeout     = 10
        retries     = 10
        startPeriod = 90
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.om_log_options["opensearch"]
    }),
    merge(local.container_defaults, {
      name              = "openmetadata_migrate"
      image             = lookup(local.image, "openmetadata_server", "")
      user              = "1000:1000"
      essential         = false
      memory            = var.openmetadata_memory_mib.migrate.limit
      memoryReservation = var.openmetadata_memory_mib.migrate.reservation
      stopTimeout       = 60
      entryPoint        = var.openmetadata_commands.migrate
      command           = []
      environment       = local.om_server_environment
      secrets           = local.om_db_secret
      dependsOn         = [{ containerName = "opensearch", condition = "HEALTHY" }]
      logConfiguration  = local.om_log_options["openmetadata_migrate"]
    }),
    merge(local.container_defaults, {
      name              = "openmetadata_server"
      image             = lookup(local.image, "openmetadata_server", "")
      user              = "1000:1000"
      memory            = var.openmetadata_memory_mib.server.limit
      memoryReservation = var.openmetadata_memory_mib.server.reservation
      stopTimeout       = 60
      entryPoint        = var.openmetadata_commands.server
      command           = []
      environment       = local.om_server_environment
      secrets           = local.om_db_secret
      dependsOn         = [{ containerName = "openmetadata_migrate", condition = "SUCCESS" }]
      portMappings = [
        { containerPort = local.om_api_port, hostPort = local.om_api_port, protocol = "tcp" },
        { containerPort = local.om_admin_port, hostPort = local.om_admin_port, protocol = "tcp" },
      ]
      healthCheck = {
        command     = ["CMD", "curl", "--fail", "--silent", "--max-time", "8", "http://127.0.0.1:${local.om_admin_port}/healthcheck"]
        interval    = 30
        timeout     = 10
        retries     = 10
        startPeriod = 180
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.om_log_options["openmetadata_server"]
    }),
  ] : container if local.om_on]

  # The bootstrap runs the tracked script from the mounted checkout in the ingestion image. It reads the administrator password as a secret, changes the factory password,
  # issues one bot token per caller and writes each token straight to its Parameter Store parameter with the task role. It is told the parameter names, never a token.
  openmetadata_bootstrap_containers = [for container in [
    merge(local.container_defaults, {
      name                   = "openmetadata_bootstrap"
      image                  = lookup(local.image, "openmetadata_ingestion", "")
      user                   = "10001:10001"
      readonlyRootFilesystem = true
      memory                 = var.openmetadata_memory_mib.bootstrap.limit
      memoryReservation      = var.openmetadata_memory_mib.bootstrap.reservation
      stopTimeout            = 30
      entryPoint             = ["python", local.om_bootstrap_py]
      command                = []
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "AWS_DEFAULT_REGION", value = var.aws_region },
        { name = "HOME", value = "/tmp" },
        { name = "OM_API", value = "http://127.0.0.1:${local.om_api_port}/api" },
        { name = "OM_BOT_PARAMETERS", value = jsonencode(var.openmetadata_bot_token_names) },
        { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
        { name = "PYTHONUNBUFFERED", value = "1" },
      ]
      secrets     = [{ name = "OM_ADMIN_PASSWORD", valueFrom = local.secret_arn["openmetadata_admin_password"] }]
      mountPoints = [{ sourceVolume = "code_repo", containerPath = local.om_code_mount, readOnly = true }]
      linuxParameters = {
        capabilities = { add = [], drop = ["ALL"] }
        tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD-SHELL", "kill -0 1"]
        interval    = 10
        timeout     = 5
        retries     = 3
        startPeriod = 30
      }
      restartPolicy    = { enabled = false }
      logConfiguration = local.om_log_options["openmetadata_bootstrap"]
    }),
  ] : container if local.om_on]
}

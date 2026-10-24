locals {
  airflow_code_volumes = {
    code_dags               = "${local.code_path}/dags"
    code_orchestration_init = "${local.code_path}/orchestration/__init__.py"
    code_interface          = "${local.code_path}/orchestration/interface.py"
    code_client             = "${local.code_path}/orchestration/client.py"
    code_dag_runtime        = "${local.code_path}/orchestration/dag_runtime.py"
    code_lineage            = "${local.code_path}/services/lineage"
  }

  airflow_state_volumes = {
    airflow_logs             = "${var.data_mount_path}/airflow/logs"
    airflow_lineage_provider = "${var.data_mount_path}/airflow/lineage_provider"
  }

  airflow_code_mounts = [
    { sourceVolume = "code_dags", containerPath = "/opt/airflow/dags", readOnly = true },
    { sourceVolume = "code_orchestration_init", containerPath = "/opt/hai/orchestration/__init__.py", readOnly = true },
    { sourceVolume = "code_interface", containerPath = "/opt/hai/orchestration/interface.py", readOnly = true },
    { sourceVolume = "code_client", containerPath = "/opt/hai/orchestration/client.py", readOnly = true },
    { sourceVolume = "code_dag_runtime", containerPath = "/opt/hai/orchestration/dag_runtime.py", readOnly = true },
    { sourceVolume = "code_lineage", containerPath = "/opt/hai/hai_lineage", readOnly = true },
    { sourceVolume = "airflow_logs", containerPath = "/opt/airflow/logs", readOnly = false },
  ]

  airflow_lineage_provider_mount = { sourceVolume = "airflow_lineage_provider", containerPath = "/opt/hai/lineage_provider", readOnly = false }

  airflow_environment_map = {
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION             = "true"
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL                = "http://127.0.0.1:8080/execution/"
    AIRFLOW__CORE__EXECUTOR                                = "LocalExecutor"
    AIRFLOW__CORE__LOAD_EXAMPLES                           = "false"
    AIRFLOW__CORE__PARALLELISM                             = tostring(var.airflow_parallelism)
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS          = "true"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD                = "bash -c 'printf %s \"postgresql+psycopg2://airflow:$AIRFLOW_DB_PASSWORD@127.0.0.1:5432/airflow\"'"
    AIRFLOW__OPENLINEAGE__CONFIG_CONN_ID                   = ""
    AIRFLOW__OPENLINEAGE__CONFIG_PATH                      = "/opt/hai/hai_lineage/openlineage.yml"
    AIRFLOW__OPENLINEAGE__DEBUG_MODE                       = "false"
    AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE              = "true"
    AIRFLOW__OPENLINEAGE__EXECUTION_TIMEOUT                = "5"
    AIRFLOW__OPENLINEAGE__INCLUDE_FULL_TASK_INFO           = "false"
    AIRFLOW__OPENLINEAGE__NAMESPACE                        = "hai-prediction-aws"
    AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT                 = "false"
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK                = "true"
    AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_HOST = "127.0.0.1"
    PYTHONPATH                                             = "/opt/hai"
  }

  airflow_environment = [for name, value in local.airflow_environment_map : { name = name, value = value }]

  airflow_secrets = [
    { name = "AIRFLOW__API__SECRET_KEY", valueFrom = local.secret_arn["airflow_api_secret_key"] },
    { name = "AIRFLOW__API_AUTH__JWT_SECRET", valueFrom = local.secret_arn["airflow_jwt_secret"] },
    { name = "AIRFLOW__CORE__FERNET_KEY", valueFrom = local.secret_arn["airflow_fernet_key"] },
    { name = "AIRFLOW_DB_PASSWORD", valueFrom = local.secret_arn["airflow_db_password"] },
  ]

  airflow_container_defaults = merge(local.container_defaults, {
    image       = local.image["airflow"]
    user        = "50000:0"
    environment = local.airflow_environment
    secrets     = local.airflow_secrets
    mountPoints = local.airflow_code_mounts
    stopTimeout = 60
    linuxParameters = {
      capabilities = { drop = ["ALL"] }
    }
  })

  airflow_wait_for_migrations = "airflow db check-migrations --migration-wait-timeout 600"

  airflow_apiserver_script = "exec airflow api-server --host 127.0.0.1 --port 8080"

  airflow_migrate_script = <<-EOT
    for attempt in $(seq 1 60); do airflow db check && break; sleep 5; done
    exec airflow db migrate
  EOT

  airflow_apiserver_containers = [
    merge(local.airflow_container_defaults, {
      name              = "airflow_migrate"
      essential         = false
      memory            = 640
      memoryReservation = 256
      command           = ["bash", "-ec", local.airflow_migrate_script]
      mountPoints       = []
      logConfiguration  = local.log_configuration["airflow_migrate"]
    }),
    merge(local.airflow_container_defaults, {
      name              = "airflow_apiserver"
      memory            = 1536
      memoryReservation = 1024
      command           = ["bash", "-ec", local.airflow_apiserver_script]
      mountPoints       = concat(local.airflow_code_mounts, [local.airflow_lineage_provider_mount])
      portMappings      = [{ containerPort = 8080, hostPort = 8080, protocol = "tcp" }]
      dependsOn         = [{ containerName = "airflow_migrate", condition = "SUCCESS" }]
      healthCheck = {
        command     = ["CMD", "curl", "--fail", "--silent", "http://127.0.0.1:8080/api/v2/version"]
        interval    = 30
        timeout     = 5
        retries     = 10
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["airflow_apiserver"]
    }),
  ]

  airflow_scheduler_containers = [
    merge(local.airflow_container_defaults, {
      name              = "airflow_scheduler"
      memory            = 1536
      memoryReservation = 1024
      command           = ["bash", "-ec", "${local.airflow_wait_for_migrations}\nexec airflow scheduler --skip-serve-logs"]
      mountPoints       = concat(local.airflow_code_mounts, [local.airflow_lineage_provider_mount])
      portMappings      = [{ containerPort = 8974, hostPort = 8974, protocol = "tcp" }]
      healthCheck = {
        command     = ["CMD", "curl", "--fail", "--silent", "http://127.0.0.1:8974/health"]
        interval    = 30
        timeout     = 5
        retries     = 10
        startPeriod = 90
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["airflow_scheduler"]
    }),
  ]

  airflow_dag_processor_containers = [
    merge(local.airflow_container_defaults, {
      name              = "airflow_dag_processor"
      memory            = 1280
      memoryReservation = 768
      command           = ["bash", "-ec", "${local.airflow_wait_for_migrations}\nexec airflow dag-processor"]
      healthCheck = {
        command     = ["CMD-SHELL", "airflow jobs check --job-type DagProcessorJob --local"]
        interval    = 30
        timeout     = 20
        retries     = 10
        startPeriod = 90
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["airflow_dag_processor"]
    }),
  ]

  airflow_triggerer_containers = [
    merge(local.airflow_container_defaults, {
      name              = "airflow_triggerer"
      memory            = 768
      memoryReservation = 512
      command           = ["bash", "-ec", "${local.airflow_wait_for_migrations}\nexec airflow triggerer --skip-serve-logs"]
      healthCheck = {
        command     = ["CMD-SHELL", "airflow jobs check --job-type TriggererJob --local"]
        interval    = 30
        timeout     = 20
        retries     = 10
        startPeriod = 90
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["airflow_triggerer"]
    }),
  ]
}

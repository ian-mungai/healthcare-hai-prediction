locals {
  # Every DAG step launches its job through the direct ECS launch library (orchestration/airflow_steps.py). It imports these modules, reads these tables and
  # measures the free space of the scratch volume, so every Airflow container receives them.
  airflow_launch_files = {
    code_airflow_steps       = "orchestration/airflow_steps.py"
    code_ecs_preflight       = "orchestration/ecs_preflight.py"
    code_ecs_adapter         = "orchestration/ecs_adapter.py"
    code_ecs_launch          = "orchestration/ecs_launch.py"
    code_ecs_launch_operator = "orchestration/ecs_launch_operator.py"
    code_ecs_launch_provider = "orchestration/ecs_launch_provider.py"
    code_serving_contract    = "orchestration/serving_contract.py"
    code_training_approvals  = "orchestration/training_approvals.py"
    code_jobs_table          = "config/orchestration/ecs_broker_jobs.json"
    code_jobs_schema         = "config/orchestration/ecs_broker_jobs.schema.json"
    code_host_jobs           = "config/orchestration/host_jobs.json"
    code_direct_jobs         = "config/orchestration/direct_jobs.json"
    code_launch_config       = "config/orchestration/ecs_launch.json"
    code_bronze_tables       = "config/lakehouse/bronze_tables.json"
    code_lakehouse_init      = "scripts/lakehouse/__init__.py"
    code_bronze_dispatch     = "scripts/lakehouse/bronze_dispatch.py"
  }

  airflow_launch_volumes = merge({ for name, path in local.airflow_launch_files : name => "${local.code_path}/${path}" }, { job_scratch = var.airflow_launch_scratch_path })

  # The one-slot Airflow pool that serializes the tasks launching a host job. The migrate container creates it (airflow pools set is an upsert) and every
  # Airflow container names it in HAI_LAUNCH_POOL, which orchestration/dag_runtime.py reads.
  airflow_launch_pool = "host_jobs"
  airflow_launch_host = var.host_private_ip

  # What orchestration/airflow_steps.py reads to launch a job (its SETTINGS), all from existing variables. None is a secret: the step signs with the task
  # role and reads the record prefix from config/orchestration/ecs_launch.json. A value with no source yet is empty, and the step refuses by the setting's
  # name. The bucket is airflow_launch_bucket_name and the host address host_private_ip, both required once the
  # scheduler runs as the launch role; broker_jobs feeds nothing here.
  airflow_launch_environment = {
    AWS_REGION               = var.aws_region
    HAI_LAUNCH_POOL          = local.airflow_launch_pool
    HAI_LAUNCH_CLUSTER       = var.cluster_name
    HAI_LAUNCH_FAMILY_PREFIX = "${var.project_name}_${var.environment}_job_"
    HAI_LAUNCH_PROJECT_TAG   = var.project_tag
    HAI_LAUNCH_LOG_GROUP     = "${var.project_name}_jobs_${var.environment}"
    HAI_LAUNCH_HOST_PROVIDER = var.ec2_capacity_provider_name
    HAI_LAUNCH_BUCKET        = var.airflow_launch_bucket_name == null ? "" : var.airflow_launch_bucket_name
    HAI_LAUNCH_POLARIS_URI   = local.airflow_launch_host == null ? "" : "http://${local.airflow_launch_host}:8181/api/catalog"
    HAI_LAUNCH_MLFLOW_URI    = local.airflow_launch_host == null ? "" : "http://${local.airflow_launch_host}:5100"
    HAI_LAUNCH_SCRATCH_PATH  = "/opt/hai/job_scratch"
    # The usable-memory inputs a host revision must be bound to (owner A, Nov 10 2026). Variables only: the job room also counts the reservations of
    # these Airflow containers, so it cannot be named here without a cycle.
    # The parameter that holds the current host job cap (owner A, Nov 10 2026), read by the preflight at every host launch.
    HAI_LAUNCH_HOST_CAP_PARAMETER  = var.host_job_cap_parameter_name
    HAI_LAUNCH_HOST_HOLD_PARAMETER = var.host_launch_hold_parameter_name
    # A present or unreadable services apply lock refuses every host launch (round 4, owner A, Nov 10 2026).
    HAI_LAUNCH_APPLY_LOCK_PARAMETER = var.services_apply_lock_parameter_name
    HAI_LAUNCH_HOST_CAPACITY        = format("usable_memory_mib=%d system_reserve_mib=%d", var.host_usable_memory_mib, var.host_system_reserve_mib)
  }
  airflow_migrate_last = "airflow db migrate\nexec airflow pools set ${local.airflow_launch_pool} 1 'One host job at a time'"

  airflow_launch_mounts = concat(
    [for name, path in local.airflow_launch_files : { sourceVolume = name, containerPath = "/opt/hai/${path}", readOnly = true }],
    [{ sourceVolume = "job_scratch", containerPath = "/opt/hai/job_scratch", readOnly = true }]
  )

  airflow_code_volumes = merge({
    code_dags               = "${local.code_path}/dags"
    code_orchestration_init = "${local.code_path}/orchestration/__init__.py"
    code_interface          = "${local.code_path}/orchestration/interface.py"
    code_client             = "${local.code_path}/orchestration/client.py"
    code_dag_runtime        = "${local.code_path}/orchestration/dag_runtime.py"
    code_training_contract  = "${local.code_path}/orchestration/training_contract.py"
    code_lineage            = "${local.code_path}/services/lineage"
  }, local.airflow_launch_volumes)

  airflow_state_volumes = {
    airflow_logs             = "${var.data_mount_path}/airflow/logs"
    airflow_lineage_provider = "${var.data_mount_path}/airflow/lineage_provider"
  }

  airflow_code_mounts = concat([
    { sourceVolume = "code_dags", containerPath = "/opt/airflow/dags", readOnly = true },
    { sourceVolume = "code_orchestration_init", containerPath = "/opt/hai/orchestration/__init__.py", readOnly = true },
    { sourceVolume = "code_interface", containerPath = "/opt/hai/orchestration/interface.py", readOnly = true },
    { sourceVolume = "code_client", containerPath = "/opt/hai/orchestration/client.py", readOnly = true },
    { sourceVolume = "code_dag_runtime", containerPath = "/opt/hai/orchestration/dag_runtime.py", readOnly = true },
    { sourceVolume = "code_training_contract", containerPath = "/opt/hai/orchestration/training_contract.py", readOnly = true },
    { sourceVolume = "code_lineage", containerPath = "/opt/hai/hai_lineage", readOnly = true },
    { sourceVolume = "airflow_logs", containerPath = "/opt/airflow/logs", readOnly = false },
  ], local.airflow_launch_mounts)

  airflow_lineage_provider_mount = { sourceVolume = "airflow_lineage_provider", containerPath = "/opt/hai/lineage_provider", readOnly = false }

  # Airflow sends its metrics over UDP to the StatsD exporter on this host. The settings exist only while monitoring_enabled is true, so the flag off leaves the
  # Airflow task definitions as they were.
  airflow_statsd_environment = {
    AIRFLOW__METRICS__STATSD_ON     = "true"
    AIRFLOW__METRICS__STATSD_HOST   = "127.0.0.1"
    AIRFLOW__METRICS__STATSD_PORT   = "9125"
    AIRFLOW__METRICS__STATSD_PREFIX = "airflow"
  }

  airflow_environment_map = merge({
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
  }, var.monitoring_enabled ? local.airflow_statsd_environment : {}, local.airflow_launch_environment)

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
      capabilities = { add = [], drop = ["ALL"] }
    }
    systemControls = []
    volumesFrom    = []
  })

  airflow_wait_for_migrations = "airflow db check-migrations --migration-wait-timeout 600"

  airflow_apiserver_script = "exec airflow api-server --host 127.0.0.1 --port 8080"

  airflow_migrate_script = <<-EOT
    for attempt in $(seq 1 60); do airflow db check && break; sleep 5; done
    ${local.airflow_migrate_last}
  EOT

  airflow_apiserver_containers = [
    merge(local.airflow_container_defaults, {
      name              = "airflow_migrate"
      essential         = false
      memory            = local.memory_profile.airflow_migrate.limit
      memoryReservation = local.memory_profile.airflow_migrate.reservation
      command           = ["bash", "-ec", local.airflow_migrate_script]
      mountPoints       = []
      logConfiguration  = local.log_configuration["airflow_migrate"]
    }),
    merge(local.airflow_container_defaults, {
      name              = "airflow_apiserver"
      memory            = local.memory_profile.airflow_apiserver.limit
      memoryReservation = local.memory_profile.airflow_apiserver.reservation
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
      memory            = local.memory_profile.airflow_scheduler.limit
      memoryReservation = local.memory_profile.airflow_scheduler.reservation
      command           = ["bash", "-ec", "${var.broker_enabled ? local.broker_session_script : ""}${local.airflow_wait_for_migrations}\nexec airflow scheduler --skip-serve-logs"]
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
      },
      # With the broker on, the scheduler also holds the broker token and a memory-backed folder for the client files the unchanged DAG client reads.
      {
        for key, value in {
          secrets = concat(local.airflow_secrets, [{ name = "BROKER_TOKEN", valueFrom = local.secret_arn["broker_token"] }])
          linuxParameters = {
            capabilities = { add = [], drop = ["ALL"] }
            tmpfs        = [{ containerPath = local.broker_session_path, size = 1, mountOptions = ["nosuid"] }]
          }
        } : key => value if var.broker_enabled
    }),
  ]

  airflow_dag_processor_containers = [
    merge(local.airflow_container_defaults, {
      name              = "airflow_dag_processor"
      memory            = local.memory_profile.airflow_dag_proc.limit
      memoryReservation = local.memory_profile.airflow_dag_proc.reservation
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
      memory            = local.memory_profile.airflow_triggerer.limit
      memoryReservation = local.memory_profile.airflow_triggerer.reservation
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

provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  name_prefix = "${var.project_name}_${var.environment}"

  # Set on each resource as well as in default_tags: a plan test cannot read the provider block.
  tags = {
    Project     = var.project_tag
    Environment = var.environment
    ManagedBy   = "Terraform"
  }

  # Tasks carry the Project tag only when the service (or the RunTask call) copies the task definition's tags onto them, and IAM has no condition key
  # that requires it. Every service and the one-shot launch arguments therefore use this one value.
  task_tag_source = "TASK_DEFINITION"

  image_keys = concat(["postgres", "redis", "polaris", "polaris_admin", "airflow"], var.marquez_enabled ? ["marquez", "marquez_web"] : [], var.mlflow_enabled ? ["mlflow"] : [], var.model_api_enabled ? ["model_api"] : [], var.streamlit_enabled ? ["streamlit"] : [], var.monitoring_enabled ? ["prometheus", "grafana", "statsd_exporter"] : [], local.om_on ? ["openmetadata_server", "openmetadata_ingestion", "openmetadata_search"] : [], local.code_fetch_on ? ["training"] : [])
  image      = { for key in local.image_keys : key => "${var.ecr_repository_urls[key]}@${var.image_digests[key]}" }

  code_path = coalesce(var.dag_code_path, "${var.data_mount_path}/repo")

  # The monitoring containers are added only while monitoring_enabled is true, so the default plan needs no monitoring log group.
  container_component = merge({
    "postgres"                              = "postgres"
    "postgres_databases"                    = "postgres"
    "redis"                                 = "redis"
    "polaris"                               = "polaris"
    "airflow_migrate"                       = "airflow"
    "airflow_apiserver"                     = "airflow"
    "airflow_scheduler"                     = "airflow"
    "airflow_dag_processor"                 = "airflow"
    "airflow_triggerer"                     = "airflow"
    "marquez_api"                           = "marquez"
    "marquez_web"                           = "marquez"
    "model_api"                             = "airflow"
    "streamlit"                             = "airflow"
    "host_prepare"                          = "airflow"
    "polaris_bootstrap"                     = "polaris"
    }, local.code_fetch_on ? { "code_fetch" = "airflow" } : {}, var.mlflow_enabled ? { "mlflow" = "mlflow" } : {}, local.om_on ? { for name in ["opensearch", "openmetadata_migrate", "openmetadata_server", "openmetadata_bootstrap"] : name => "marquez" } : {}, var.broker_enabled ? { "broker" = "airflow" } : {}, {
    for name in ["monitoring_prepare", "prometheus", "grafana", "statsd_exporter"] : name => "monitoring" if var.monitoring_enabled
  })

  log_configuration = {
    for container, component in local.container_component : container => {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = var.log_group_names[component]
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = container
        "mode"                  = "non-blocking"
        "max-buffer-size"       = "16m"
      }
    }
  }

  container_defaults = {
    essential              = true
    privileged             = false
    readonlyRootFilesystem = false
    dockerSecurityOptions  = ["no-new-privileges"]
    ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
  }

  restart_policy = {
    enabled              = true
    restartAttemptPeriod = 60
  }

  secret_names = merge({
    postgres_password      = var.postgres_password_name
    airflow_db_password    = var.airflow_db_password_name
    marquez_db_password    = var.marquez_db_password_name
    airflow_fernet_key     = var.airflow_fernet_key_name
    airflow_jwt_secret     = var.airflow_jwt_secret_name
    airflow_api_secret_key = var.airflow_api_secret_key_name
    redis_password         = var.redis_password_name
    polaris_root_secret    = var.polaris_root_secret_name
    mlflow_db_password     = var.mlflow_db_password_name
    mlflow_admin_password  = var.mlflow_admin_password_name
    broker_token           = var.broker_token_name
    grafana_admin_password = var.grafana_admin_password_name
    # These two names are used only by the OpenMetadata containers, so they appear in the task definitions and in the grant only while the flag is on.
    openmetadata_admin_password = var.openmetadata_admin_password_name
    openmetadata_db_password    = var.openmetadata_db_password_name
  }, local.model_api_secret_names, local.streamlit_secret_names)

  # A SecureString parameter ARN is the name without its leading slash after "parameter/". It is built from the name: no data source reads the parameter, so no value
  # enters the plan or the state. The variable validation guarantees the leading slash.
  secret_arn = { for key, name in local.secret_names : key => "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${name}" }

  services = merge(
    {
      postgres = {
        task_role_arn = var.control_plane_task_role_arn
        containers    = local.postgres_containers
        volumes       = { postgres_data = "${var.data_mount_path}/postgres" }
      }
      redis = {
        task_role_arn = var.control_plane_task_role_arn
        containers    = local.redis_containers
        volumes       = {}
      }
      polaris = {
        task_role_arn = var.polaris_task_role_arn
        containers    = local.polaris_containers
        volumes       = {}
      }
      airflow_apiserver = {
        task_role_arn = var.control_plane_task_role_arn
        containers    = local.airflow_apiserver_containers
        volumes       = merge(local.airflow_code_volumes, local.airflow_state_volumes)
      }
      # Under the LocalExecutor every DAG step runs in the scheduler's task, so the scheduler alone takes the launch role (owner, Nov 7 2026).
      airflow_scheduler = {
        task_role_arn = coalesce(var.airflow_launch_task_role_arn, var.control_plane_task_role_arn)
        containers    = local.airflow_scheduler_containers
        volumes       = merge(local.airflow_code_volumes, local.airflow_state_volumes)
      }
      airflow_dag_processor = {
        task_role_arn = var.control_plane_task_role_arn
        containers    = local.airflow_dag_processor_containers
        volumes       = merge(local.airflow_code_volumes, { airflow_logs = local.airflow_state_volumes.airflow_logs })
      }
      airflow_triggerer = {
        task_role_arn = var.control_plane_task_role_arn
        containers    = local.airflow_triggerer_containers
        volumes       = merge(local.airflow_code_volumes, { airflow_logs = local.airflow_state_volumes.airflow_logs })
      }
    },
    {
      for key, service in {
        marquez = {
          task_role_arn = var.control_plane_task_role_arn
          containers    = local.marquez_containers
          volumes       = {}
        }
      } : key => service if var.marquez_enabled
    },
    {
      for key, service in {
        openmetadata = {
          task_role_arn = var.control_plane_task_role_arn
          containers    = local.openmetadata_containers
          volumes       = { opensearch_data = local.om_search_data }
        }
      } : key => service if local.om_on
    },
    {
      for key, service in {
        mlflow = {
          task_role_arn = var.mlflow_task_role_arn
          containers    = local.mlflow_containers
          volumes       = {}
        }
      } : key => service if var.mlflow_enabled
    },
    {
      for key, service in {
        broker = {
          task_role_arn = var.broker_task_role_arn
          containers    = local.broker_containers
          volumes       = { code_repo = local.code_path, broker_state = local.broker_state_path }
        }
      } : key => service if var.broker_enabled
    },
    {
      for key, service in {
        model_api = {
          task_role_arn = var.model_api_task_role_arn
          containers    = local.model_api_containers
          volumes       = {}
        }
      } : key => service if var.model_api_enabled
    },
    {
      for key, service in {
        streamlit = {
          task_role_arn = var.streamlit_task_role_arn
          containers    = local.streamlit_containers
          volumes       = {}
        }
      } : key => service if var.streamlit_enabled
    },
    {
      for key, service in {
        monitoring = {
          task_role_arn = var.control_plane_task_role_arn
          containers    = local.monitoring_containers
          volumes       = local.monitoring_volumes
        }
      } : key => service if var.monitoring_enabled
    }
  )

  serving_keys        = ["model_api", "streamlit"]
  serving_containers  = flatten([for key, service in local.services : service.containers if contains(local.serving_keys, key)])
  other_containers    = concat(flatten([for key, service in local.services : service.containers if !contains(local.serving_keys, key)]), flatten([for task in local.one_shot_tasks : task.containers]))
  all_containers      = concat(flatten([for service in local.services : service.containers]), flatten([for task in local.one_shot_tasks : task.containers]))
  reserved_memory_mib = sum([for service in local.services : sum([for container in service.containers : container.memoryReservation])])
  hard_memory_mib     = sum([for container in local.all_containers : container.memory])
  memory_budget_mib   = var.host_memory_mib - var.other_services_reserve_mib
  secret_arns_used    = sort(distinct(flatten([for container in local.all_containers : [for secret in lookup(container, "secrets", []) : secret.valueFrom]])))
  serving_secret_arns = sort(distinct(flatten([for container in local.serving_containers : [for secret in lookup(container, "secrets", []) : secret.valueFrom]])))
  other_secret_arns   = sort(distinct(flatten([for container in local.other_containers : [for secret in lookup(container, "secrets", []) : secret.valueFrom]])))
  secret_names_used   = sort([for key, arn in local.secret_arn : local.secret_names[key] if contains(local.secret_arns_used, arn)])
}

resource "aws_ecs_task_definition" "service" {
  #checkov:skip=CKV_AWS_336:Postgres, Redis, Polaris, Airflow and Marquez write to their own paths; a read-only root file system would stop them.
  for_each = local.services

  # The project user cannot deregister task definitions (foundation policy), so Terraform keeps a replaced or destroyed
  # revision registered (it stays active until deregistered separately) instead of failing the apply.
  skip_destroy = true

  family                   = "${local.name_prefix}_${each.key}"
  requires_compatibilities = ["EC2"]
  network_mode             = "host"
  execution_role_arn       = contains(local.serving_keys, each.key) ? var.serving_execution_role_arn : var.execution_role_arn
  task_role_arn            = each.value.task_role_arn
  container_definitions    = jsonencode(local.service_containers[each.key])
  tags                     = local.tags

  # Set to the provider defaults so that the plan knows them: the approval pin of the task settings cannot bind a value that is unknown.
  enable_fault_injection = false

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  dynamic "volume" {
    for_each = each.value.volumes

    content {
      name                = volume.key
      host_path           = volume.value
      configure_at_launch = false
    }
  }

  lifecycle {
    precondition {
      condition     = local.reserved_memory_mib <= local.memory_budget_mib
      error_message = "The control-plane memory reservations exceed the host memory minus the reserve kept for the operating system and the later services."
    }

    precondition {
      condition     = local.hard_memory_mib <= var.host_memory_mib - 2048
      error_message = "If every container, including the one-shot tasks, reached its hard memory limit, the host would keep less than 2 GiB for the operating system and the ECS agent."
    }

    precondition {
      condition     = local.reserved_cpu_units <= var.host_cpu_units - var.job_cpu_units
      error_message = "The services reserve more CPU units than the host keeps beside the largest job, so the job would not be placed or would starve the services."
    }

    precondition {
      condition     = alltrue([for container in each.value.containers : container.memory >= container.memoryReservation])
      error_message = "Every container hard memory limit must be at least its reservation."
    }
  }
}

resource "aws_ecs_service" "service" {
  for_each = { for key, service in local.services : key => service if key != "openmetadata" && !contains(local.serving_keys, key) }

  name                               = "${local.name_prefix}_${each.key}"
  cluster                            = var.cluster_name
  task_definition                    = aws_ecs_task_definition.service[each.key].arn
  desired_count                      = 1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  enable_ecs_managed_tags            = true
  propagate_tags                     = local.task_tag_source
  tags                               = local.tags

  capacity_provider_strategy {
    capacity_provider = var.ec2_capacity_provider_name
    weight            = 1
    base              = 1
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
}

# The catalog service has its own resource so that the desired_count exception covers it alone: the fixed services keep their count reconciled.
resource "aws_ecs_service" "catalog" {
  for_each = { for key, value in local.services : key => value if key == "openmetadata" }

  name                               = "${local.name_prefix}_${each.key}"
  cluster                            = var.cluster_name
  task_definition                    = aws_ecs_task_definition.service[each.key].arn
  desired_count                      = 1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  enable_ecs_managed_tags            = true
  propagate_tags                     = local.task_tag_source
  tags                               = local.tags

  capacity_provider_strategy {
    capacity_provider = var.ec2_capacity_provider_name
    weight            = 1
    base              = 1
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  # host_session stop scales this service to zero before the host goes off, and start sets it back to one. Terraform must not read that as drift.
  lifecycle {
    ignore_changes = [desired_count]
  }
}

# The model API and the dashboard are released by GitHub (owner decision D18): its deploy workflow registers a new task definition revision and rolls the service to it.
# An owner apply must not roll such a release back, so these two services ignore changes to task_definition. They were services of the shared resource above, and the
# moved blocks carry them over without a destroy or a replacement.
# The revision that an existing service runs was registered before this change: it names the shared task execution role and, for the model API, the shared Redis
# password. GitHub renders its next revision from the running one, and the release check refuses that password, so the first release would fail. So the first owner
# apply after this change replaces each service once: replace_triggered_by watches the execution role of the stack's own task definition, which changes from the
# shared role to the serving execution role in that apply and in no later one, and the new service starts on the stack's revision (the serving execution role and
# the model API's own Redis user). The services stop for the drain and the start of that one apply, as the Redis service restarts in it too. Every later apply
# leaves the running revision alone again, because the execution role stays the same.
resource "aws_ecs_service" "serving" {
  for_each = { for key, service in local.services : key => service if contains(local.serving_keys, key) }

  name                               = "${local.name_prefix}_${each.key}"
  cluster                            = var.cluster_name
  task_definition                    = aws_ecs_task_definition.service[each.key].arn
  desired_count                      = 1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  enable_ecs_managed_tags            = true
  propagate_tags                     = local.task_tag_source
  tags                               = local.tags

  capacity_provider_strategy {
    capacity_provider = var.ec2_capacity_provider_name
    weight            = 1
    base              = 1
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  lifecycle {
    ignore_changes       = [task_definition]
    replace_triggered_by = [aws_ecs_task_definition.service[each.key].execution_role_arn]
  }
}

moved {
  from = aws_ecs_service.service["model_api"]
  to   = aws_ecs_service.serving["model_api"]
}

moved {
  from = aws_ecs_service.service["streamlit"]
  to   = aws_ecs_service.serving["streamlit"]
}

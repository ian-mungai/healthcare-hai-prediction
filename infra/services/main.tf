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

  image_keys = concat(["postgres", "redis", "polaris", "polaris_admin", "airflow"], var.marquez_enabled ? ["marquez", "marquez_web"] : [], var.mlflow_enabled ? ["mlflow"] : [], var.model_api_enabled ? ["model_api"] : [], var.streamlit_enabled ? ["streamlit"] : [])
  image      = { for key in local.image_keys : key => "${var.ecr_repository_urls[key]}@${var.image_digests[key]}" }

  code_path = coalesce(var.dag_code_path, "${var.data_mount_path}/repo")

  container_component = merge({
    "postgres"              = "postgres"
    "postgres_databases"    = "postgres"
    "redis"                 = "redis"
    "polaris"               = "polaris"
    "airflow_migrate"       = "airflow"
    "airflow_apiserver"     = "airflow"
    "airflow_scheduler"     = "airflow"
    "airflow_dag_processor" = "airflow"
    "airflow_triggerer"     = "airflow"
    "marquez_api"           = "marquez"
    "marquez_web"           = "marquez"
    "model_api"             = "airflow"
    "streamlit"             = "airflow"
    "host_prepare"          = "airflow"
    "polaris_bootstrap"     = "polaris"
  }, var.mlflow_enabled ? { "mlflow" = "mlflow" } : {}, var.broker_enabled ? { "broker" = "airflow" } : {})

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
        volumes       = { redis_data = "${var.data_mount_path}/redis" }
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
      airflow_scheduler = {
        task_role_arn = var.control_plane_task_role_arn
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
    }
  )

  all_containers      = concat(flatten([for service in local.services : service.containers]), flatten([for task in local.one_shot_tasks : task.containers]))
  reserved_memory_mib = sum([for service in local.services : sum([for container in service.containers : container.memoryReservation])])
  hard_memory_mib     = sum([for container in local.all_containers : container.memory])
  memory_budget_mib   = var.host_memory_mib - var.other_services_reserve_mib
  secret_arns_used    = sort(distinct(flatten([for container in local.all_containers : [for secret in lookup(container, "secrets", []) : secret.valueFrom]])))
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
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = each.value.task_role_arn
  container_definitions    = jsonencode(each.value.containers)
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
      condition     = alltrue([for container in each.value.containers : container.memory >= container.memoryReservation])
      error_message = "Every container hard memory limit must be at least its reservation."
    }
  }
}

resource "aws_ecs_service" "service" {
  for_each = local.services

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

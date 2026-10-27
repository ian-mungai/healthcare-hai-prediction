locals {
  host_prepare_script = <<-EOT
    mkdir -p /host/postgres /host/redis /host/airflow/logs /host/airflow/lineage_provider /host/repo
    chown 999:999 /host/postgres /host/redis
    chown 50000:0 /host/airflow/logs /host/airflow/lineage_provider
    chmod 0700 /host/postgres
    chmod 0750 /host/airflow/logs /host/airflow/lineage_provider /host/redis
  EOT

  polaris_bootstrap_script = <<-EOT
    secret=$${POLARIS_ROOT_SECRET//\\/\\\\}
    secret=$${secret//\"/\\\"}
    exec timeout 600 /opt/jboss/container/java/run/run-java.sh bootstrap --credentials-file /dev/stdin < <(
      printf '{"POLARIS": {"client-id": "root", "client-secret": "%s"}}\n' "$secret"
    )
  EOT

  one_shot_restart_policy = {
    enabled              = true
    ignoredExitCodes     = [0]
    restartAttemptPeriod = 60
  }

  one_shot_tasks = {
    host_prepare = {
      network_mode  = "none"
      task_role_arn = var.control_plane_task_role_arn
      volumes       = { data = var.data_mount_path }
      containers = [
        merge(local.container_defaults, {
          name                   = "host_prepare"
          image                  = local.image["airflow"]
          user                   = "0"
          readonlyRootFilesystem = true
          memory                 = 128
          memoryReservation      = 64
          stopTimeout            = 30
          entryPoint             = ["bash", "-ec"]
          command                = [local.host_prepare_script]
          mountPoints            = [{ sourceVolume = "data", containerPath = "/host", readOnly = false }]
          linuxParameters = {
            capabilities = { drop = ["ALL"], add = ["CHOWN", "DAC_OVERRIDE", "FOWNER"] }
          }
          healthCheck = {
            command     = ["CMD-SHELL", "test -d /host/postgres && test -d /host/redis && test -d /host/airflow/logs"]
            interval    = 5
            timeout     = 2
            retries     = 2
            startPeriod = 0
          }
          restartPolicy    = local.one_shot_restart_policy
          logConfiguration = local.log_configuration["host_prepare"]
        }),
      ]
    }

    polaris_bootstrap = {
      network_mode  = "host"
      task_role_arn = var.control_plane_task_role_arn
      volumes       = {}
      containers = [
        merge(local.container_defaults, {
          name              = "polaris_bootstrap"
          image             = local.image["polaris_admin"]
          user              = "10000:10001"
          memory            = 1024
          memoryReservation = 512
          stopTimeout       = 60
          entryPoint        = ["bash", "-ec"]
          command           = [local.polaris_bootstrap_script]
          environment = [
            { name = "AWS_REGION", value = var.aws_region },
            { name = "POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE", value = "postgresql" },
            { name = "POLARIS_PERSISTENCE_TYPE", value = "relational-jdbc" },
            { name = "QUARKUS_DATASOURCE_JDBC_URL", value = "jdbc:postgresql://127.0.0.1:5432/polaris" },
            { name = "QUARKUS_DATASOURCE_USERNAME", value = "polaris" },
            { name = "QUARKUS_HTTP_HOST_ENABLED", value = "false" },
          ]
          secrets = [
            { name = "POLARIS_ROOT_SECRET", valueFrom = local.secret_arn["polaris_root_secret"] },
            { name = "QUARKUS_DATASOURCE_PASSWORD", valueFrom = local.secret_arn["postgres_password"] },
          ]
          healthCheck = {
            command     = ["CMD-SHELL", "kill -0 1"]
            interval    = 10
            timeout     = 5
            retries     = 3
            startPeriod = 30
          }
          restartPolicy    = { enabled = false }
          logConfiguration = local.log_configuration["polaris_bootstrap"]
        }),
      ]
    }
  }
}

resource "aws_ecs_task_definition" "one_shot" {
  #checkov:skip=CKV_AWS_336:The Polaris bootstrap JVM writes temporary files; host preparation sets its own read-only root file system and the container definition test asserts it.
  for_each = local.one_shot_tasks

  # The project user cannot deregister task definitions (foundation policy), so Terraform keeps a replaced or destroyed
  # revision registered (it stays active until deregistered separately) instead of failing the apply.
  skip_destroy = true

  family                   = "${local.name_prefix}_${each.key}"
  requires_compatibilities = ["EC2"]
  network_mode             = each.value.network_mode
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
}

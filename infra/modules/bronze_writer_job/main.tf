locals {
  bronze_family = "${var.project_name}_${var.environment}_job_write_bronze"
  bronze_worker_command = [
    "python", "-B", "-m", "scripts.lakehouse.aws_bronze", "--binding", var.run_binding_path
  ]
  bronze_container = {
    name                   = "bronze_writer"
    image                  = var.image
    essential              = true
    readonlyRootFilesystem = true
    entryPoint             = var.bootstrap_entrypoint
    command                = local.bronze_worker_command
    stopTimeout            = 120
    environment = [
      { name = "AWS_REGION", value = var.aws_region },
      { name = "POLARIS_URI", value = var.polaris_uri },
      { name = "SPARK_JOB_MEMORY", value = var.spark_memory },
      { name = "JOB_THREADS", value = tostring(var.job_threads) }
    ]
    secrets = [
      {
        name      = "POLARIS_CLIENT_ID"
        valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.aws_polaris_bronze_client_id_name}"
      },
      {
        name      = "POLARIS_CLIENT_SECRET"
        valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.aws_polaris_bronze_client_secret_name}"
      }
    ]
    mountPoints = [
      { sourceVolume = "scratch", containerPath = "/tmp", readOnly = false },
      { sourceVolume = "run", containerPath = "/run/job", readOnly = false }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = var.log_group_name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "bronze_writer"
      }
    }
  }
  bronze_memory_shapes = {
    "1024"  = { min = 2048, max = 8192, step = 1024 }
    "2048"  = { min = 4096, max = 16384, step = 1024 }
    "4096"  = { min = 8192, max = 30720, step = 1024 }
    "8192"  = { min = 16384, max = 61440, step = 4096 }
    "16384" = { min = 32768, max = 122880, step = 8192 }
  }
}

resource "aws_ecs_task_definition" "bronze_writer" {
  count = var.register_definition ? 1 : 0

  family                   = local.bronze_family
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.cpu)
  memory                   = tostring(var.memory)
  task_role_arn            = var.task_role_arn
  execution_role_arn       = var.execution_role_arn
  container_definitions    = jsonencode([local.bronze_container])
  tags                     = var.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  volume {
    name = "scratch"
  }

  volume {
    name = "run"
  }

  lifecycle {
    precondition {
      condition     = var.task_role_arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_write_bronze_task_${var.environment}"
      error_message = "The production bronze definition must use its prepared bronze task role."
    }

    precondition {
      condition = (
        var.aws_polaris_bronze_client_id_name != var.aws_polaris_bronze_client_secret_name &&
        endswith(var.aws_polaris_bronze_client_id_name, "/aws_polaris_bronze_client_id") &&
        endswith(var.aws_polaris_bronze_client_secret_name, "/aws_polaris_bronze_client_secret")
      )
      error_message = "Inject only the bronze pair, never the administrator, reader or test pair."
    }

    precondition {
      condition = (
        var.memory >= local.bronze_memory_shapes[tostring(var.cpu)].min &&
        var.memory <= local.bronze_memory_shapes[tostring(var.cpu)].max &&
        var.memory % local.bronze_memory_shapes[tostring(var.cpu)].step == 0 &&
        var.job_threads >= 1 && var.job_threads == floor(var.job_threads) && var.job_threads <= var.cpu / 1024 &&
        tonumber(trimsuffix(var.spark_memory, "g")) * 1024 < var.memory
      )
      error_message = "Use a valid Fargate memory shape, bounded Spark threads and a heap smaller than task memory."
    }
  }
}

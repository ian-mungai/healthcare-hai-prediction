provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
  container = {
    name                   = "identity_probe"
    image                  = var.seed_image
    essential              = true
    privileged             = false
    readonlyRootFilesystem = true
    user                   = "10001:10001"
    entryPoint             = ["timeout", "--signal=KILL", "60", "python", "-I", "-c"]
    command                = [file("${path.module}/probe.py")]
    stopTimeout            = 30
    linuxParameters        = { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true }
    ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
    environment = [
      { name = "AWS_REGION", value = var.aws_region },
      { name = "AWS_DEFAULT_REGION", value = var.aws_region },
      { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
      { name = "EXPECTED_ROLE_ID", value = var.task_role_unique_id }
    ]
    secrets = []
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = var.log_group_name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "identity_probe"
        mode                  = "non-blocking"
        max-buffer-size       = "1m"
      }
    }
  }
}

resource "aws_ecs_task_definition" "probe" {
  count = var.probe_enabled ? 1 : 0

  family                   = "${var.project_name}_${var.environment}_job_identity_probe"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn
  container_definitions    = jsonencode([local.container])
  skip_destroy             = true
  enable_fault_injection   = false
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
}

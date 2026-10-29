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
  definition_specs = {
    analytics_offline = { class = "offline", image = "analytics", role = "offline_task" }
    quality_offline   = { class = "offline", image = "quality", role = "offline_task" }
    seal              = { class = "seal", image = "analytics", role = "seal_task" }
    analytics_read    = { class = "lakehouse_read", image = "analytics", role = "read_task" }
    spark_read        = { class = "lakehouse_read", image = "spark", role = "read_task" }
  }
  selected = {
    for name, spec in local.definition_specs : name => spec
    if var.jobs_enabled && contains(var.enabled_definitions, name)
  }
  writer_specs = {
    spark_write_e2e      = { class = "lakehouse_write", image = "spark", role = "write_e2e_task", layer = "e2e" }
    spark_write_silver   = { class = "lakehouse_write", image = "spark", role = "write_silver_task", layer = "silver" }
    spark_write_gold     = { class = "lakehouse_write", image = "spark", role = "write_gold_task", layer = "gold" }
    spark_write_features = { class = "lakehouse_write", image = "spark", role = "write_features_task", layer = "features" }
    spark_write_bronze   = { class = "lakehouse_write", image = "spark", role = "write_bronze_task", layer = "bronze" }
  }
  writer_role_layers   = { for spec in values(local.writer_specs) : spec.role => spec.layer }
  all_definition_specs = merge(local.definition_specs, local.writer_specs)
  selected_writers = {
    for name, spec in local.writer_specs : name => spec
    if var.jobs_enabled && contains(var.enabled_definitions, name)
  }
  selected_definitions = merge(local.selected, local.selected_writers)
  writer_parameters = {
    for role, names in var.writer_parameter_names : role => [
      { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${try(names[0], "invalid")}" },
      { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${try(names[1], "invalid")}" }
    ]
  }
  memory_shapes = {
    "1024"  = { min = 2048, max = 8192, step = 1024 }
    "2048"  = { min = 4096, max = 16384, step = 1024 }
    "4096"  = { min = 8192, max = 30720, step = 1024 }
    "8192"  = { min = 16384, max = 61440, step = 4096 }
    "16384" = { min = 32768, max = 122880, step = 8192 }
  }
  resources = {
    for name, spec in local.selected_definitions : name => lookup(var.task_resources, name, { cpu = 1024, memory = 2048, ephemeral_storage_gib = 20 })
  }
  reader_parameters = [
    { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.aws_polaris_reader_client_id_name}" },
    { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.aws_polaris_reader_client_secret_name}" }
  ]
  containers = {
    for name, spec in local.selected_definitions : name => [{
      name                   = name
      image                  = lookup(var.images, spec.image, "invalid")
      essential              = true
      privileged             = false
      readonlyRootFilesystem = true
      user                   = "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = ["/opt/hai/deadline_run", "python", "-I", "/opt/hai/bootstrap/run.py"]
      command                = []
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "AWS_DEFAULT_REGION", value = var.aws_region },
        { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
        { name = "DATA_BUCKET_NAME", value = var.data_bucket_name }
      ]
      secrets = spec.class == "lakehouse_write" ? lookup(local.writer_parameters, spec.role, []) : (spec.class == "lakehouse_read" ? local.reader_parameters : [])
      mountPoints = [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = name
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }]
  }
}

resource "aws_ecs_task_definition" "read" {
  for_each = local.selected

  family                   = "${var.project_name}_${var.environment}_job_${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(local.resources[each.key].cpu)
  memory                   = tostring(local.resources[each.key].memory)
  execution_role_arn       = lookup(var.job_role_arns, "execution", "")
  task_role_arn            = lookup(var.job_role_arns, each.value.role, "")
  container_definitions    = jsonencode(local.containers[each.key])
  skip_destroy             = true
  enable_fault_injection   = false
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  volume {
    name                = "workspace"
    configure_at_launch = false
  }

  volume {
    name                = "tmp"
    configure_at_launch = false
  }

  dynamic "ephemeral_storage" {
    for_each = local.resources[each.key].ephemeral_storage_gib > 20 ? [local.resources[each.key].ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

locals {
  task_definitions                  = merge(aws_ecs_task_definition.read, aws_ecs_task_definition.writer)
  task_attributes_assigned_at_apply = ["arn", "arn_without_revision", "id", "region", "revision", "tags_all"]
  task_settings = {
    for name, task in local.task_definitions : name => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

module "definition_flow" {
  source      = "../services/tests/flow"
  definitions = { for name, task in local.task_definitions : name => task.container_definitions }
  tasks       = local.task_settings
}

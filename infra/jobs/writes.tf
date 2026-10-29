resource "aws_ecs_task_definition" "writer" {
  for_each = local.selected_writers

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

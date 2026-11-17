variable "publisher_job_enabled" {
  description = "Register the Run 3 publisher host task definition <project>_<environment>_job_publisher_host, which the direct ECS launch starts on the host (orchestration/publisher_launch.py). It needs the analytics image digest, the execution role, the publisher task role and the jobs log group."
  type        = bool
  default     = true
  nullable    = false

  validation {
    condition = !var.publisher_job_enabled || (
      contains(keys(var.images), "analytics") &&
      contains(keys(var.job_role_arns), "execution") &&
      contains(keys(var.job_role_arns), "publisher_task") &&
      var.log_group_name != null
    )
    error_message = "The publisher job needs the analytics image digest, the execution and publisher task roles and the jobs log group."
  }
}

variable "publisher_keys_enabled" {
  description = "Let the publisher job take the units that need an API key. The publisher task role must hold the key grant in infra/iam first."
  type        = bool
  default     = true
  nullable    = false
}

locals {
  publisher_selected = var.publisher_job_enabled ? toset(["publisher"]) : toset([])
  publisher_role     = "publisher_task"
  # The containers' run variables, which the launch request sets (orchestration/publisher_launch.py); each exists empty in its container. The capture root is the
  # one exception: it defaults to raw/, and the request may override it with raw/ or one ingestion_test/<run ID>/ folder (owner decision D4).
  publisher_run_variables = {
    fetch = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "HAI_DEADLINE_UTC"]
    run   = ["RUN_ID", "CODE_SHA256", "PUBLISHER_RUN_KIND", "HAI_CAPTURE_ROOT", "PUBLISHER_ORIGINAL_RUN", "PUBLISHER_RETRY_DAY", "HAI_DEADLINE_UTC"]
  }
  # The host firewall lets a job reach DNS and TCP 443 only, and has no rule per host. The approved hosts file is the control on where the task
  # connects; the downloader enforces it. The file and its approval are tracked in the code archive (config/acquisition/baseline_run/), and the run names them.
  publisher_settings = concat(
    [
      { name = "EXPECTED_ACCOUNT_ID", value = var.expected_account_id },
      { name = "TASK_ROLE_NAME", value = "${var.project_name}_job_publisher_task_${var.environment}" },
    ],
    var.publisher_keys_enabled ? [{ name = "HAI_TASK_KEYS", value = "1" }] : []
  )
  # Like every host job, init deletes what the last task left in the family's two folders, and the last phase deletes them again only after it succeeds.
  publisher_steps = [
    { name = "init", script = "${local.host_init_wipe}chown 10001:10001 /workspace /tmp" },
    { name = "fetch", script = "timeout 1800 python -I -c '${local.layer_fetch_python}'" },
    { name = "run", script = "(cd /workspace/code && python -m scripts.acquisition.publisher_run) && ${local.host_scratch_wipe}" },
  ]
  # Like every host revision, the init container carries the capacity binding the revision was sized from (orchestration/ecs_preflight.py checked_revision).
  publisher_containers = [
    for index, step in local.publisher_steps : merge({
      name                   = step.name
      image                  = lookup(var.images, "analytics", "invalid")
      essential              = index == length(local.publisher_steps) - 1
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      entryPoint             = step.name == "init" ? ["sh", "-c"] : ["/opt/hai/deadline_run", "sh", "-c"]
      command                = [step.script]
      stopTimeout            = 90
      linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.host_init_drop : ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = index == 0 ? [] : [{ containerName = local.publisher_steps[index - 1].name, condition = "SUCCESS" }]
      environment = concat(
        [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
          { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
        ],
        step.name == "run" ? local.publisher_settings : [],
        [for name in lookup(local.publisher_run_variables, step.name, []) : { name = name, value = name == "HAI_CAPTURE_ROOT" ? "raw/" : "" }],
        # The publisher pushes its own metrics like every host job (scripts/metrics_push.py); without the host address it pushes nothing.
        var.host_private_ip == null ? [] : [{ name = "PUSHGATEWAY_URL", value = "http://${var.host_private_ip}:9091" }, { name = "HAI_JOB", value = "publisher" }]
      )
      secrets = []
      mountPoints = [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = coalesce(var.log_group_name, "unset")
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "publisher"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
      }, step.name == "init" ? { dockerLabels = { (local.host_capacity_label) = local.publisher_binding } } : {}
    )
  ]
  # Both folders are on the host scratch volume (60 GiB default, infra/platform). An earlier 200 GiB estimate had no measurement behind it; the run 3
  # queue's recorded sizes add up to about 14.3 GiB, plus the inputs archive and the saved state.
  publisher_volumes = {
    workspace = "${var.host_scratch_root}/publisher/work"
    tmp       = "${var.host_scratch_root}/publisher/tmp"
  }
}

resource "aws_ecs_task_definition" "publisher_host" {
  count = length(local.publisher_selected)

  family                   = "${var.project_name}_${var.environment}_job_publisher_host"
  requires_compatibilities = ["EC2"]
  network_mode             = "bridge"
  cpu                      = tostring(local.publisher_cpu)
  memory                   = tostring(local.publisher_memory)
  execution_role_arn       = lookup(var.job_role_arns, "execution", "")
  task_role_arn            = lookup(var.job_role_arns, local.publisher_role, "")
  container_definitions    = jsonencode(local.publisher_containers)
  skip_destroy             = true
  enable_fault_injection   = false
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  lifecycle {
    # A replacement registers the new revision before it retires the old one, so a registration that ECS refuses keeps the old revision in the state. skip_destroy keeps it in ECS.
    create_before_destroy = true

    precondition {
      condition     = try(regex("^job_memory_mib=([0-9]+) ", var.host_capacity_binding)[0], "") == tostring(var.host_job_memory_mib)
      error_message = "The capacity binding gives another job memory than host_job_memory_mib. Render both from the same services output with render_jobs_config."
    }

    precondition {
      condition     = local.publisher_memory <= local.host_memory_cap && local.publisher_cpu <= local.host_cpu_cap
      error_message = "The publisher may not reserve more than the job room: its row in config/orchestration/host_jobs.json asks for more memory than host_job_memory_mib or more than 3072 CPU units. The launch preflight would refuse every launch of such a revision."
    }
  }

  dynamic "volume" {
    for_each = local.publisher_volumes
    content {
      name                = volume.key
      host_path           = volume.value
      configure_at_launch = false
    }
  }
}

output "publisher_host_container_definitions" {
  description = "Complete container JSON of the publisher host task for review and mocked plan assertions. Null while publisher_job_enabled is false."
  value       = try(aws_ecs_task_definition.publisher_host[0].container_definitions, null)
}

output "publisher_host_run_task" {
  description = "What the direct launch's run-task call needs for the publisher host task: the capacity provider strategy and no network configuration. Bridge networking gives the task no public IP; it reaches the internet through the host, on TCP 443 only. Null while publisher_job_enabled is false."
  value = var.publisher_job_enabled ? {
    task_definition       = aws_ecs_task_definition.publisher_host[0].family
    task_role_key         = local.publisher_role
    capacity_provider     = local.host_provider
    network_mode          = "bridge"
    network               = "publisher"
    capture_root          = "raw/"
    propagate_tags        = "TASK_DEFINITION"
    container_environment = local.publisher_run_variables
    scratch               = local.publisher_volumes
  } : null
}

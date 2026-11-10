variable "publisher_job_enabled" {
  description = "Register the Run 3 publisher host task definition <project>_<environment>_job_publisher_host, which the direct ECS launch starts on the host (orchestration/publisher_launch.py). It needs the analytics image digest, the execution role, the publisher task role and the jobs log group. The publisher has no Fargate definition."
  type        = bool
  default     = false
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
  default     = false
  nullable    = false
}

variable "publisher_filtered_enabled" {
  description = "Let the publisher job take the units whose originals stay in its workspace and whose filtered copies go under raw/."
  type        = bool
  default     = false
  nullable    = false
}

variable "publisher_resources" {
  description = "CPU units and memory MiB the publisher host task asks for. They are cut to the host job caps (local.host_cpu_cap and local.host_memory_cap) that every host job shares. The 8 GiB default is the Fargate-era shape; no publisher run has been measured yet, and its first run logs its peaks (scripts/resource_peaks.py)."
  type = object({
    cpu    = number
    memory = number
  })
  default  = { cpu = 2048, memory = 8192 }
  nullable = false

  validation {
    condition = (
      var.publisher_resources.cpu >= 256 && var.publisher_resources.memory >= 512 &&
      var.publisher_resources.cpu == floor(var.publisher_resources.cpu) && var.publisher_resources.memory == floor(var.publisher_resources.memory)
    )
    error_message = "Use whole CPU units from 256 and whole MiB from 512."
  }
}

locals {
  publisher_selected = var.publisher_job_enabled ? toset(["publisher"]) : toset([])
  publisher_role     = "publisher_task"
  # The containers' run variables, which the launch request sets (orchestration/publisher_launch.py); each exists empty in its container.
  publisher_run_variables = {
    fetch = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "HAI_DEADLINE_UTC"]
    run   = ["RUN_ID", "PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256", "PUBLISHER_RESUME_SOURCES", "PUBLISHER_PRIOR_STATE", "PUBLISHER_REQUEST_SHA256", "HAI_DEADLINE_UTC"]
  }
  # The host firewall lets a job reach DNS and TCP 443 only, and has no rule per host. The approved hosts file is the control on where the task
  # connects; the downloader enforces it.
  publisher_settings = concat(
    [
      { name = "HAI_CAPTURE_ROOT", value = "raw/" },
      { name = "HAI_APPROVED_HOSTS", value = "/workspace/inputs/publisher_hosts.json" },
      { name = "HAI_APPROVED_HOSTS_APPROVAL", value = "/workspace/inputs/hosts_approval.json" },
      { name = "HAI_STORED_MIRROR", value = "/workspace/inputs/mirror" },
      { name = "HAI_STORED_INDEX", value = "/workspace/inputs/stored_index.json" },
      { name = "EXPECTED_ACCOUNT_ID", value = var.expected_account_id },
      { name = "TASK_ROLE_NAME", value = "${var.project_name}_job_publisher_task_${var.environment}" },
    ],
    var.publisher_keys_enabled ? [{ name = "HAI_TASK_KEYS", value = "1" }] : [],
    var.publisher_filtered_enabled ? [{ name = "HAI_TASK_FILTERED", value = "1" }] : []
  )
  # Like every host job, init deletes what the last task left in the family's two folders, and the last phase deletes them again only after it succeeds.
  publisher_steps = [
    { name = "init", script = "${local.host_init_wipe}chown 10001:10001 /workspace /tmp" },
    { name = "fetch", script = "timeout 1800 python -I -c '${local.layer_fetch_python}'" },
    { name = "run", script = "(cd /workspace/code && python -m scripts.acquisition.publisher_run) && ${local.host_scratch_wipe}" },
  ]
  publisher_containers = [
    for index, step in local.publisher_steps : {
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
        [for name in lookup(local.publisher_run_variables, step.name, []) : { name = name, value = "" }]
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
    }
  ]
  # Both folders are on the host scratch volume (60 GiB default, infra/platform). The Fargate-era 200 GiB had no measurement behind it; the run 3
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
  cpu                      = tostring(min(var.publisher_resources.cpu, local.host_cpu_cap))
  memory                   = tostring(min(var.publisher_resources.memory, local.host_memory_cap))
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

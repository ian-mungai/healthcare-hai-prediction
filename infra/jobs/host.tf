variable "host_jobs_enabled" {
  description = "Register the host twin of each selected job family: an EC2 bridge task definition named <project>_<environment>_job_<family>_host for the host capacity provider. Off by default. The Fargate definitions stay as they are, so a family moves between Fargate and the host with no change to either."
  type        = bool
  default     = false
  nullable    = false
}

variable "host_jobs" {
  description = "The job families whose host definition is registered while host_jobs_enabled is true. Each needs the inputs of its Fargate twin: the images, the roles, the parameter names and its own enable flag."
  type        = set(string)
  default     = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]
  nullable    = false

  validation {
    condition     = length(setsubtract(var.host_jobs, toset(local.host_families))) == 0
    error_message = "Select only the nine job families: bronze, drift, features, gold, metadata, promote, serve, silver and train."
  }

  validation {
    condition     = !var.host_jobs_enabled || alltrue([for family in var.host_jobs : lookup(local.host_source_ready, family, false)])
    error_message = "A host definition is built from the containers of its Fargate twin, so every selected family needs its own enable flag and inputs (layer_jobs_enabled with the layer in layer_jobs, bronze_job_enabled, train_enabled, drift_enabled, promote_enabled, serve_enabled or openmetadata_enabled)."
  }

  validation {
    condition     = !var.host_jobs_enabled || !contains(var.host_jobs, "metadata") || var.openmetadata_cpu_architecture == "ARM64"
    error_message = "The host is an ARM64 instance, so the catalog task runs on it only when openmetadata_cpu_architecture is ARM64 and all four images list linux/arm64."
  }
}

variable "host_scratch_root" {
  description = "Folder on the host's scratch volume: /data/job_scratch, where boot mounts that volume, or a folder inside it. Each family gets <root>/<family>/<volume> for its task volumes. The job wipes it at start and after success."
  type        = string
  default     = "/data/job_scratch"
  nullable    = false

  validation {
    condition     = can(regex("^/data/job_scratch(/[A-Za-z0-9_]([A-Za-z0-9_./-]*[A-Za-z0-9_])?)?$", var.host_scratch_root)) && !strcontains(var.host_scratch_root, "..") && !strcontains(var.host_scratch_root, "//")
    error_message = "Use /data/job_scratch, where boot mounts the scratch volume, or a folder inside it, without a trailing slash, a parent step or a double slash. Any other folder is on the snapshotted data volume."
  }
}

variable "host_job_memory_mib" {
  description = "Memory MiB of every host job: the services stack output host_job_memory_mib (host memory less the operating system reserve and the always-on services' reservations), rendered by scripts/infrastructure/render_jobs_config.py. One job runs at a time, so each job gets the whole room and its engines derive their limits from it."
  type        = number
  default     = null
  nullable    = true

  validation {
    condition     = var.host_job_memory_mib == null ? !var.host_jobs_enabled : (var.host_job_memory_mib >= 4096 && var.host_job_memory_mib < 32768 && var.host_job_memory_mib == floor(var.host_job_memory_mib))
    error_message = "Host jobs need host_job_memory_mib from the services stack output: a whole number of MiB from 4096 up to below the 32768 MiB host."
  }
}

variable "host_capacity_binding" {
  description = "The services stack output host_capacity_binding, rendered by scripts/infrastructure/render_jobs_config.py: job_memory_mib=<host_job_memory_mib> usable_memory_mib=<n> system_reserve_mib=<n> service_reservations_mib=<n>. The init container of every host revision carries it as the label hai.host_capacity, and the launch preflight runs a host job only from a revision bound to the current usable-memory inputs (owner decision A, Nov 10 2026)."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.host_capacity_binding == null ? !var.host_jobs_enabled : try(
      [for parts in [regex("^job_memory_mib=([1-9][0-9]*) usable_memory_mib=([1-9][0-9]*) system_reserve_mib=([0-9]+) service_reservations_mib=([0-9]+)$", var.host_capacity_binding)] :
      tonumber(parts[0]) == tonumber(parts[1]) - tonumber(parts[2]) - tonumber(parts[3])][0],
      false
    )
    error_message = "Host jobs need host_capacity_binding from the services stack output: job_memory_mib=<n> usable_memory_mib=<n> system_reserve_mib=<n> service_reservations_mib=<n>, where the job memory is the usable memory less the reserve and the reservations."
  }
}

variable "host_openmetadata_api" {
  description = "Address of the persistent OpenMetadata API for the metadata host task, for example http://10.0.1.20:8585/api, always port 8585 and path /api. Use the private address of the host, because the bridge task reaches the host's services at that address and the host firewall opens the catalog port for it. Loopback is the task's own address in bridge mode, and the bridge gateway is not one of the addresses the firewall rules name. Required while the metadata host task is selected."
  type        = string
  default     = null

  validation {
    condition = var.host_openmetadata_api == null ? !(var.host_jobs_enabled && contains(var.host_jobs, "metadata")) : try(
      [
        for captures in [regex("^http://(([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})):8585/api$", var.host_openmetadata_api)] : [
          for octets in [[for part in slice(captures, 1, 5) : tonumber(part)]] : (
            alltrue([for octet in octets : octet <= 255]) &&
            captures[0] == format("%d.%d.%d.%d", octets...) &&
            (octets[0] == 10 || (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) || (octets[0] == 192 && octets[1] == 168))
          )
        ][0]
      ][0],
      false
    )
    error_message = "Supply the catalog API as http://<private IPv4 address>:8585/api while the metadata host task is selected. The address must be in 10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16 with octets from 0 to 255 and no leading zero, the port must be 8585 and the path /api, because the task sends its bot token there."
  }
}

variable "host_repo_path" {
  description = "Absolute host path of the repository checkout that the metadata host task mounts read-only to run the ingestion script and read its configuration. Use the checkout that Airflow mounts. Required while the metadata host task is selected."
  type        = string
  default     = null

  validation {
    condition     = var.host_repo_path == null ? !(var.host_jobs_enabled && contains(var.host_jobs, "metadata")) : can(regex("^/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$", var.host_repo_path)) && !strcontains(var.host_repo_path, "..")
    error_message = "Supply an absolute path without a trailing slash or a parent step while the metadata host task is selected."
  }
}

locals {
  host_families = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]
  # The capacity provider that infra/platform registers: format("<project>_%s_<environment>", "host").
  host_provider = "${var.project_name}_host_${var.environment}"
  # The host has 4096 CPU units. One job runs at a time and gets 3072 units and the memory the always-on services leave (host_job_memory_mib, from the services stack).
  # A job derives its threads, memory and spill cap from these task-level limits when it starts, so the host rewrites no engine value.
  host_cpu_cap    = 3072
  host_memory_cap = var.host_job_memory_mib
  # The init container of every host revision carries the capacity binding it was sized from; orchestration/ecs_preflight.py reads this label.
  host_capacity_label = "hai.host_capacity"
  # The two volumes of every Fargate job keep their names and become host folders; a volume that is not named here keeps its own name as the folder.
  host_volume_directories = { workspace = "work" }
  host_source_ready = {
    silver   = var.layer_jobs_enabled && contains(var.layer_jobs, "silver")
    gold     = var.layer_jobs_enabled && contains(var.layer_jobs, "gold")
    features = var.layer_jobs_enabled && contains(var.layer_jobs, "features")
    bronze   = var.bronze_job_enabled
    train    = var.train_enabled
    drift    = var.drift_enabled
    promote  = var.promote_enabled
    serve    = var.serve_enabled
    metadata = contains(keys(var.openmetadata_images), "ingestion") && contains(keys(var.openmetadata_roles), "execution") && contains(keys(var.openmetadata_roles), "openmetadata_task") && var.log_group_name != null
  }
  # Roles and network class of each host definition (the class of config/orchestration/ecs_broker_jobs.json, which a Terraform test compares).
  host_specs = {
    silver   = { execution = lookup(var.job_role_arns, "execution", ""), task_key = local.layer_roles.silver, task = lookup(var.job_role_arns, local.layer_roles.silver, ""), network = "lakehouse" }
    gold     = { execution = lookup(var.job_role_arns, "execution", ""), task_key = local.layer_roles.gold, task = lookup(var.job_role_arns, local.layer_roles.gold, ""), network = "lakehouse" }
    features = { execution = lookup(var.job_role_arns, "execution", ""), task_key = local.layer_roles.features, task = lookup(var.job_role_arns, local.layer_roles.features, ""), network = "lakehouse" }
    bronze   = { execution = lookup(var.job_role_arns, "execution", ""), task_key = local.bronze_role, task = lookup(var.job_role_arns, local.bronze_role, ""), network = "lakehouse" }
    train    = { execution = lookup(var.train_roles, "execution", ""), task_key = "train_task", task = lookup(var.train_roles, "train_task", ""), network = "training" }
    drift    = { execution = lookup(var.drift_roles, "execution", ""), task_key = "drift_task", task = lookup(var.drift_roles, "drift_task", ""), network = "training" }
    promote  = { execution = lookup(var.train_roles, "execution", ""), task_key = "train_task", task = lookup(var.train_roles, "train_task", ""), network = "training" }
    serve    = { execution = lookup(var.train_roles, "execution", ""), task_key = "serve_task", task = lookup(var.serve_roles, "serve_task", ""), network = "training" }
    metadata = { execution = lookup(var.openmetadata_roles, "execution", ""), task_key = "openmetadata_task", task = lookup(var.openmetadata_roles, "openmetadata_task", ""), network = "lakehouse" }
  }
  host_selected = {
    for family, spec in local.host_specs : family => spec
    if var.host_jobs_enabled && contains(var.host_jobs, family)
  }
  host_layer_containers = { for layer in local.layer_jobs : layer => concat(local.layer_containers[layer], [local.layer_guards[layer]]) }
  host_source_containers = merge(local.host_layer_containers, {
    bronze   = local.bronze_containers
    train    = local.train_containers
    drift    = local.drift_containers
    promote  = local.promote_containers
    serve    = local.serve_containers
    metadata = local.host_metadata_containers
  })
  # The persistent catalog runs as a service on the host. The metadata job is only its client: after the init step, one container sends the Polaris and dbt metadata to the catalog API.
  host_repo_mount = "/opt/hai/repo"
  host_metadata_mounts = [
    { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
    { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false },
  ]
  host_metadata_log = {
    logDriver = "awslogs"
    options = {
      awslogs-group         = var.log_group_name
      awslogs-region        = var.aws_region
      awslogs-stream-prefix = "metadata"
      mode                  = "non-blocking"
      max-buffer-size       = "16m"
    }
  }
  # The init step of the metadata job is the one every other job has: it runs as root, so the common host wipe below deletes what the last attempt left and hands the two folders to the job user.
  host_metadata_containers = [
    {
      name                   = "init"
      image                  = lookup(var.openmetadata_images, "ingestion", "invalid")
      essential              = false
      privileged             = false
      readonlyRootFilesystem = true
      user                   = "0:0"
      workingDirectory       = "/workspace"
      entryPoint             = ["sh", "-c"]
      command                = ["chown 10001:10001 /workspace /tmp"]
      stopTimeout            = 60
      linuxParameters        = { capabilities = { add = [], drop = local.train_init_drop }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = []
      environment            = []
      secrets                = []
      mountPoints            = local.host_metadata_mounts
      logConfiguration       = local.host_metadata_log
    },
    {
      name                   = "ingest"
      image                  = lookup(var.openmetadata_images, "ingestion", "invalid")
      essential              = true
      privileged             = false
      readonlyRootFilesystem = true
      user                   = "10001:10001"
      workingDirectory       = local.host_repo_mount
      entryPoint             = ["sh", "-c"]
      command                = ["python -m scripts.infrastructure.openmetadata_host_ingest --config-dir ${local.host_repo_mount}/config/metadata/ingestion --work-dir /workspace polaris dbt"]
      stopTimeout            = 60
      linuxParameters        = { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = [{ containerName = "init", condition = "SUCCESS" }]
      environment = concat(local.om_common_environment, [
        { name = "OM_API", value = coalesce(var.host_openmetadata_api, "unset") },
        { name = "RUN_ID", value = "" },
        { name = "POLARIS_URI", value = "" },
      ])
      secrets = concat(local.reader_parameters, [
        { name = "OM_INGESTION_TOKEN", valueFrom = "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.openmetadata_ingestion_token_name}" },
      ])
      mountPoints      = concat(local.host_metadata_mounts, [{ sourceVolume = "repo", containerPath = local.host_repo_mount, readOnly = true }])
      logConfiguration = local.host_metadata_log
    },
  ]
  # A host folder keeps what the last job left, so the init step of each job first deletes the contents of its two folders. Root can
  # delete the job user's files only with DAC_OVERRIDE and FOWNER, which the Fargate init step drops, so the host init step keeps
  # those two and drops the rest of the Fargate list.
  #
  # The same deletion ends the last phase of the job, after the whole phase has succeeded. A task stops as soon as its essential container
  # ends, so a later container would never start, and that is why the phase runs the deletion itself: the group keeps the phase command
  # unchanged, && makes the deletion follow only an exit code of zero, and a failed phase leaves its files for diagnosis until the next job
  # of the family wipes them at its start. Both deletions name only /workspace and /tmp, the mounts of the family's own two folders.
  host_scratch_wipe = "find /workspace /tmp -mindepth 1 -delete"
  host_init_wipe    = "${local.host_scratch_wipe} && "
  host_init_drop    = ["FSETID", "MKNOD", "NET_RAW", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE"]
  host_phase_names = {
    for family in keys(local.host_selected) : family => [for container in lookup(local.host_source_containers, family, []) : container.name if container.name != "guard"]
  }
  host_last_phase = { for family, names in local.host_phase_names : family => length(names) > 0 ? names[length(names) - 1] : "" }
  host_containers = {
    for family in keys(local.host_selected) : family => [
      for container in lookup(local.host_source_containers, family, []) : merge(
        container,
        { for key, value in {
          command         = [format("%s%s", local.host_init_wipe, container.command[0])]
          linuxParameters = merge(container.linuxParameters, { capabilities = { add = [], drop = local.host_init_drop } })
          dockerLabels    = { (local.host_capacity_label) = coalesce(var.host_capacity_binding, "unset") }
        } : key => value if container.name == "init" },
        { for key, value in {
          command = [format("(%s) && %s", container.command[0], local.host_scratch_wipe)]
        } : key => value if container.name == local.host_last_phase[family] }
      )
    ]
  }
  host_volumes = {
    for family in keys(local.host_selected) : family => {
      for name in distinct(flatten([for container in local.host_containers[family] : [for mount in container.mountPoints : mount.sourceVolume]])) :
      name => name == "repo" ? coalesce(var.host_repo_path, "/unset") : "${var.host_scratch_root}/${family}/${lookup(local.host_volume_directories, name, name)}"
    }
  }
  host_resources = {
    for family, spec in local.host_selected : family => {
      cpu    = local.host_cpu_cap
      memory = local.host_memory_cap
    }
  }
  host_run_variables = {
    silver   = local.layer_run_variables.silver
    gold     = local.layer_run_variables.gold
    features = local.layer_run_variables.features
    bronze   = local.bronze_run_variables
    train    = local.train_run_variables
    drift    = local.drift_run_variables
    promote  = { for step in local.promote_steps : step.name => step.variables if length(step.variables) > 0 }
    serve    = { for step in local.serve_steps : step.name => step.variables if length(step.variables) > 0 }
    metadata = { ingest = ["RUN_ID", "POLARIS_URI"] }
  }
  host_dry_run = {
    silver   = { variable = "LAYER_DRY_RUN", value = "1" }
    gold     = { variable = "LAYER_DRY_RUN", value = "1" }
    features = { variable = "LAYER_DRY_RUN", value = "1" }
    bronze   = { variable = "BRONZE_DRY_RUN", value = "1" }
  }
}

resource "aws_ecs_task_definition" "host" {
  for_each = local.host_selected

  family                   = "${var.project_name}_${var.environment}_job_${each.key}_host"
  requires_compatibilities = ["EC2"]
  network_mode             = "bridge"
  cpu                      = tostring(local.host_resources[each.key].cpu)
  memory                   = tostring(local.host_resources[each.key].memory)
  execution_role_arn       = each.value.execution
  task_role_arn            = each.value.task
  container_definitions    = jsonencode(local.host_containers[each.key])
  skip_destroy             = true
  enable_fault_injection   = false
  tags                     = local.tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  lifecycle {
    precondition {
      condition     = try(regex("^job_memory_mib=([0-9]+) ", var.host_capacity_binding)[0], "") == tostring(var.host_job_memory_mib)
      error_message = "The capacity binding gives another job memory than host_job_memory_mib. Render both from the same services output with render_jobs_config."
    }
  }

  dynamic "volume" {
    for_each = local.host_volumes[each.key]
    content {
      name                = volume.key
      host_path           = volume.value
      configure_at_launch = false
    }
  }
}

module "host_flow" {
  source      = "../services/tests/flow"
  definitions = { for family, task in aws_ecs_task_definition.host : "host_${family}" => task.container_definitions }
  tasks = {
    for family, task in aws_ecs_task_definition.host : "host_${family}" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "host_task_definition_arns" {
  description = "Registered host task revisions, keyed by family. Null while host_jobs_enabled is false."
  value       = var.host_jobs_enabled ? { for family, task in aws_ecs_task_definition.host : family => task.arn } : null
}

output "host_container_definitions" {
  description = "Complete container JSON of each host task, keyed by family, for review and mocked plan assertions."
  value       = var.host_jobs_enabled ? { for family, task in aws_ecs_task_definition.host : family => task.container_definitions } : null
}

output "host_secret_flow" {
  description = "Secret-flow and pin results of the shared flow module for the host tasks."
  value = var.host_jobs_enabled ? {
    violations             = module.host_flow.violations
    alias_chains_converged = module.host_flow.alias_chains_converged
    containers_checked     = module.host_flow.containers_checked
    mutation_mismatches    = module.host_flow.mutation_mismatches
    mutations_checked      = module.host_flow.mutations_checked
  } : null
}

output "host_run_tasks" {
  description = "What the launcher's run-task call needs for each registered host task, the host equivalent of layer_run_tasks and the other run-task outputs. The call sends the capacity provider strategy and no network configuration. Registration starts nothing."
  value = var.host_jobs_enabled ? {
    for family, task in aws_ecs_task_definition.host : family => {
      task_definition       = task.family
      task_role_key         = local.host_selected[family].task_key
      capacity_provider     = local.host_provider
      network_mode          = "bridge"
      network               = local.host_selected[family].network
      propagate_tags        = "TASK_DEFINITION"
      container_environment = local.host_run_variables[family]
      dry_run_variable      = try(local.host_dry_run[family].variable, null)
      dry_run_value         = try(local.host_dry_run[family].value, null)
      scratch               = local.host_volumes[family]
    }
  } : null
}

output "host_dispatch_contract" {
  description = "Host equivalent of dispatch_contract and writer_dispatch_contracts: where a host job runs and what bounds it. The host firewall bounds the network, so the contract carries each family's class and no security group. Registration dispatches nothing."
  value = var.host_jobs_enabled ? {
    capacity_provider = local.host_provider
    network_mode      = "bridge"
    propagate_tags    = "TASK_DEFINITION"
    jobs_at_a_time    = 1
    cpu_units_cap     = local.host_cpu_cap
    memory_mib_cap    = local.host_memory_cap
    families          = sort(keys(local.host_selected))
    family_networks   = { for family, spec in local.host_selected : family => spec.network }
    writers = {
      for family, spec in local.host_selected : family => {
        task_role_key   = spec.task_key
        parameter_names = lookup(var.writer_parameter_names, spec.task_key, [])
      }
      if startswith(spec.task_key, "write_")
    }
  } : null
}

variable "layer_jobs_enabled" {
  description = "Register the permanent silver, gold and features task definitions that the lean job broker starts. They need the spark, analytics and quality image digests, the execution role, each enabled layer's writer role and pair, the jobs log group and, for features, the feature store image and the host Redis address."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition = !var.layer_jobs_enabled || (
      alltrue([for name in ["analytics", "quality", "spark"] : contains(keys(var.images), name)]) &&
      contains(keys(var.job_role_arns), "execution") &&
      alltrue([for layer in var.layer_jobs : contains(keys(var.job_role_arns), "write_${layer}_task") && contains(keys(var.writer_parameter_names), "write_${layer}_task")]) &&
      (!contains(var.layer_jobs, "features") || contains(keys(var.train_images), "feature_store")) &&
      var.log_group_name != null
    )
    error_message = "Layer jobs need the three image digests, the execution role, the writer role and pair of every enabled layer, the feature store image for features and the jobs log group."
  }
}

variable "layer_jobs" {
  description = "The layers whose task definition is registered while layer_jobs_enabled is true. Gold runs only after the silver publish passes, so the owner can register one layer at a time."
  type        = set(string)
  default     = ["silver", "gold", "features"]
  nullable    = false

  validation {
    condition     = length(setsubtract(var.layer_jobs, toset(["silver", "gold", "features"]))) == 0
    error_message = "Select only silver, gold or features."
  }
}

variable "layer_resources" {
  description = "Fargate CPU units, memory MiB and ephemeral storage GiB of every layer task."
  type = object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  })
  default = { cpu = 4096, memory = 16384, ephemeral_storage_gib = 100 }

  validation {
    condition = try(
      var.layer_resources.memory >= local.memory_shapes[tostring(var.layer_resources.cpu)].min &&
      var.layer_resources.memory <= local.memory_shapes[tostring(var.layer_resources.cpu)].max &&
      var.layer_resources.memory % local.memory_shapes[tostring(var.layer_resources.cpu)].step == 0 &&
      var.layer_resources.ephemeral_storage_gib >= 20 && var.layer_resources.ephemeral_storage_gib <= 200 &&
      var.layer_resources.ephemeral_storage_gib == floor(var.layer_resources.ephemeral_storage_gib), false
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch."
  }
}

variable "layer_feast_redis_host" {
  description = "Private address of the host Redis that the features job applies Feast to. Required while the features layer job is registered."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = var.layer_feast_redis_host == "" || can(regex("^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$", var.layer_feast_redis_host))
    error_message = "Supply a host name or address, or leave it empty."
  }

  validation {
    condition     = !(var.layer_jobs_enabled && contains(var.layer_jobs, "features")) || var.layer_feast_redis_host != ""
    error_message = "The features job needs the private address of the host Redis."
  }
}

locals {
  layer_jobs = var.layer_jobs_enabled ? var.layer_jobs : toset([])
  layer_roles = {
    silver   = "write_silver_task"
    gold     = "write_gold_task"
    features = "write_features_task"
  }
  # The container names and their empty variables equal config/orchestration/ecs_broker_jobs.json; a Terraform test compares them with the file.
  layer_run_variables = {
    silver = {
      fetch    = ["RUN_ID", "CODE_SHA256"]
      pin      = ["RUN_ID", "CODE_SHA256", "POLARIS_URI"]
      build    = ["RUN_ID", "POLARIS_URI"]
      validate = ["RUN_ID"]
      verify   = ["RUN_ID", "POLARIS_URI"]
      export   = ["RUN_ID"]
      publish  = ["RUN_ID", "POLARIS_URI"]
      upload   = ["RUN_ID"]
    }
    gold = {
      fetch    = ["RUN_ID", "CODE_SHA256"]
      pin      = ["RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID"]
      build    = ["RUN_ID", "POLARIS_URI"]
      validate = ["RUN_ID"]
      export   = ["RUN_ID"]
      publish  = ["RUN_ID", "POLARIS_URI"]
      upload   = ["RUN_ID"]
    }
    features = {
      fetch    = ["RUN_ID", "CODE_SHA256"]
      pin      = ["RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID"]
      build    = ["RUN_ID", "POLARIS_URI"]
      validate = ["RUN_ID"]
      export   = ["RUN_ID"]
      publish  = ["RUN_ID", "POLARIS_URI"]
      feast    = ["RUN_ID", "POLARIS_URI"]
    }
  }
  layer_driver = "cd /workspace/code && python -m scripts.lakehouse.layer_run %s --layer %s --run \"$RUN_ID\""
  # The task has no code before the first step. It downloads the staged archive, checks it against CODE_SHA256 and unpacks it; the pin phase checks it again.
  layer_fetch_python = trimspace(<<-EOT
    import boto3, hashlib, os, tarfile; d = os.environ["DATA_BUCKET_NAME"]; r = os.environ["RUN_ID"]; p = "/workspace/code.tar.gz"; boto3.client("s3").download_file(d, "lakehouse/run_stage/" + r + "/inputs/code.tar.gz", p); assert hashlib.sha256(open(p, "rb").read()).hexdigest() == os.environ["CODE_SHA256"], "code.tar.gz differs from CODE_SHA256"; tarfile.open(p).extractall("/workspace/code", filter="data")
  EOT
  )
  layer_phase_images = { fetch = "spark", pin = "spark", build = "analytics", validate = "quality", verify = "spark", export = "quality", publish = "spark", upload = "spark", feast = "feature_store" }
  layer_phase_secrets = {
    pin     = "reader"
    verify  = "reader"
    feast   = "reader"
    publish = "writer"
  }
  layer_phases = {
    silver   = ["fetch", "pin", "build", "validate", "verify", "export", "publish", "upload"]
    gold     = ["fetch", "pin", "build", "validate", "export", "publish", "upload"]
    features = ["fetch", "pin", "build", "validate", "export", "publish", "feast"]
  }
  layer_steps = {
    for layer in local.layer_jobs : layer => concat(
      [{ name = "init", script = "chown 10001:10001 /workspace /tmp" }],
      [for phase in local.layer_phases[layer] : {
        name   = phase
        script = phase == "fetch" ? "python -I -c '${local.layer_fetch_python}'" : format(local.layer_driver, phase, layer)
      }]
    )
  }
  layer_containers = {
    for layer, steps in local.layer_steps : layer => [
      for index, step in steps : {
        name                   = step.name
        image                  = step.name == "init" ? lookup(var.images, "spark", "invalid") : (local.layer_phase_images[step.name] == "feature_store" ? lookup(var.train_images, "feature_store", "invalid") : lookup(var.images, local.layer_phase_images[step.name], "invalid"))
        essential              = index == length(steps) - 1
        privileged             = false
        readonlyRootFilesystem = true
        user                   = step.name == "init" ? "0:0" : "10001:10001"
        workingDirectory       = "/workspace"
        entryPoint             = ["sh", "-c"]
        command                = [step.script]
        stopTimeout            = 90
        linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.train_init_drop : ["ALL"] }, initProcessEnabled = true }
        ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
        dependsOn              = index == 0 ? [] : [{ containerName = steps[index - 1].name, condition = "SUCCESS" }]
        environment = concat(
          [
            { name = "AWS_REGION", value = var.aws_region },
            { name = "AWS_DEFAULT_REGION", value = var.aws_region },
            { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
            { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
            { name = "HOME", value = "/tmp" },
            { name = "PYTHONHASHSEED", value = "0" },
            { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
            { name = "SPARK_JOB_MEMORY", value = "10g" },
            { name = "DUCKDB_MEMORY_LIMIT", value = "12GB" },
            { name = "JOB_THREADS", value = "4" },
            { name = "HAI_SPARK_LOCAL_DIRECTORY", value = "/workspace/spill" },
          ],
          [for name in lookup(local.layer_run_variables[layer], step.name, []) : { name = name, value = "" }],
          step.name == "feast" ? [
            { name = "FEAST_PROFILE", value = "aws" },
            { name = "FEAST_REDIS_HOST", value = var.layer_feast_redis_host },
            { name = "FEAST_REGISTRY_BUCKET", value = var.data_bucket_name },
          ] : []
        )
        secrets = lookup(local.layer_phase_secrets, step.name, "") == "reader" ? local.reader_parameters : (lookup(local.layer_phase_secrets, step.name, "") == "writer" ? lookup(local.writer_parameters, local.layer_roles[layer], []) : [])
        mountPoints = [
          { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
          { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
        ]
        logConfiguration = {
          logDriver = "awslogs"
          options = {
            awslogs-group         = var.log_group_name
            awslogs-region        = var.aws_region
            awslogs-stream-prefix = layer
            mode                  = "non-blocking"
            max-buffer-size       = "16m"
          }
        }
      }
    ]
  }
}

resource "aws_ecs_task_definition" "layer" {
  for_each = local.layer_jobs

  family                   = "${var.project_name}_${var.environment}_job_${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.layer_resources.cpu)
  memory                   = tostring(var.layer_resources.memory)
  execution_role_arn       = lookup(var.job_role_arns, "execution", "")
  task_role_arn            = lookup(var.job_role_arns, local.layer_roles[each.key], "")
  container_definitions    = jsonencode(local.layer_containers[each.key])
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
    for_each = var.layer_resources.ephemeral_storage_gib > 20 ? [var.layer_resources.ephemeral_storage_gib] : []
    content {
      size_in_gib = ephemeral_storage.value
    }
  }
}

module "layer_flow" {
  source      = "../services/tests/flow"
  definitions = { for layer, task in aws_ecs_task_definition.layer : "layer_${layer}" => task.container_definitions }
  tasks = {
    for layer, task in aws_ecs_task_definition.layer : "layer_${layer}" => {
      for attribute, value in task : attribute => try(tolist(value), value)
      if attribute != "container_definitions" && !contains(local.task_attributes_assigned_at_apply, attribute)
    }
  }
}

output "layer_task_definition_arns" {
  description = "Registered layer task revisions, keyed by layer. Empty while layer_jobs_enabled is false."
  value       = { for layer, task in aws_ecs_task_definition.layer : layer => task.arn }
}

output "layer_container_definitions" {
  description = "Complete container JSON of each layer task, keyed by layer, for review and mocked plan assertions."
  value       = { for layer, task in aws_ecs_task_definition.layer : layer => task.container_definitions }
}

output "layer_secret_flow" {
  description = "Secret-flow and pin results of the shared flow module for the layer tasks."
  value = {
    violations          = module.layer_flow.violations
    containers_checked  = module.layer_flow.containers_checked
    mutation_mismatches = module.layer_flow.mutation_mismatches
  }
}

output "layer_run_tasks" {
  description = "What the broker's run-task call needs for each registered layer task. Registration starts nothing. The dry run variable is the one override that makes a run a dry run (decision 70)."
  value = {
    for layer, task in aws_ecs_task_definition.layer : layer => {
      task_definition       = task.family
      task_role_key         = local.layer_roles[layer]
      assign_public_ip      = "DISABLED"
      capacity_providers    = ["FARGATE"]
      network               = "lakehouse"
      propagate_tags        = "TASK_DEFINITION"
      container_environment = local.layer_run_variables[layer]
      dry_run_variable      = "LAYER_DRY_RUN"
      dry_run_value         = "1"
    }
  }
}

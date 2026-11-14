variable "layer_jobs_enabled" {
  description = "Supply the inputs of the silver, gold and features host definitions. They need the spark, analytics and quality image digests, the execution role, each enabled layer's writer role and pair, the jobs log group and, for features, the feature store image and the host Redis address."
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
  description = "The layers whose host definition can be registered while layer_jobs_enabled is true. Gold runs only after the silver publish passes, so the owner can register one layer at a time."
  type        = set(string)
  default     = ["silver", "gold", "features"]
  nullable    = false

  validation {
    condition     = length(setsubtract(var.layer_jobs, toset(["silver", "gold", "features"]))) == 0
    error_message = "Select only silver, gold or features."
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
      fetch    = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]
      pin      = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "POLARIS_URI"]
      build    = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
      validate = ["RUN_ID", "CODE_RUN_ID"]
      verify   = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
      export   = ["RUN_ID", "CODE_RUN_ID"]
      publish  = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
      upload   = ["RUN_ID", "CODE_RUN_ID"]
    }
    gold = {
      fetch    = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]
      pin      = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID"]
      build    = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
      validate = ["RUN_ID", "CODE_RUN_ID"]
      export   = ["RUN_ID", "CODE_RUN_ID"]
      publish  = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
      upload   = ["RUN_ID", "CODE_RUN_ID"]
      maintain = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
    }
    features = {
      fetch    = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]
      pin      = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID", "FEATURE_SET"]
      build    = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "FEATURE_SET"]
      validate = ["RUN_ID", "CODE_RUN_ID", "FEATURE_SET"]
      export   = ["RUN_ID", "CODE_RUN_ID", "FEATURE_SET"]
      publish  = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "FEATURE_SET"]
      upload   = ["RUN_ID", "CODE_RUN_ID", "FEATURE_SET"]
      feast    = ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "FEATURE_SET"]
    }
  }
  layer_driver = "cd /workspace/code && python -m scripts.lakehouse.layer_run %s --layer %s --run \"$RUN_ID\""
  # The task has no code before the first step. It downloads the staged archive, checks it against CODE_SHA256 and unpacks it; the pin phase checks it again.
  layer_fetch_python = trimspace(<<-EOT
    import boto3, hashlib, os, re, tarfile; d = os.environ["DATA_BUCKET_NAME"]; r = os.environ["CODE_RUN_ID"]; assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", r), "CODE_RUN_ID is not a run ID"; p = "/workspace/code.tar.gz"; boto3.client("s3").download_file(d, "lakehouse/run_stage/" + r + "/inputs/code.tar.gz", p); assert hashlib.sha256(open(p, "rb").read()).hexdigest() == os.environ["CODE_SHA256"], "code.tar.gz differs from CODE_SHA256"; tarfile.open(p).extractall("/workspace/code", filter="data")
  EOT
  )
  layer_phase_images = { fetch = "spark", pin = "spark", build = "analytics", validate = "quality", verify = "spark", export = "quality", publish = "spark", upload = "spark", maintain = "spark", feast = "feature_store" }
  # The silver build reads the catalog through dbt, whose profile names the pair DBT_ENV_SECRET_POLARIS_CLIENT_ID and _SECRET so that dbt scrubs them from its logs.
  layer_build_secrets = [for item in local.reader_parameters : { name = "DBT_ENV_SECRET_${item.name}", valueFrom = item.valueFrom }]
  # Gold maintenance commits snapshot removal to the catalog, so it holds the gold writer pair; it is a gold phase only.
  layer_phase_secrets = {
    pin      = "reader"
    verify   = "reader"
    feast    = "reader"
    publish  = "writer"
    maintain = "writer"
  }
  layer_phases = {
    silver   = ["fetch", "pin", "build", "validate", "verify", "export", "publish", "upload"]
    gold     = ["fetch", "pin", "build", "validate", "export", "publish", "upload", "maintain"]
    features = ["fetch", "pin", "build", "validate", "export", "publish", "upload", "feast"]
  }
  layer_steps = {
    for layer in local.layer_jobs : layer => concat(
      [{ name = "init", script = "date +%s > /workspace/task_started && chown 10001:10001 /workspace /tmp /workspace/task_started" }],
      [for phase in local.layer_phases[layer] : {
        name   = phase
        script = phase == "fetch" ? "timeout 1800 python -I -c '${local.layer_fetch_python}'" : format(local.layer_driver, phase, layer)
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
        linuxParameters        = { capabilities = { add = [], drop = step.name == "init" ? local.host_init_drop : ["ALL"] }, initProcessEnabled = true }
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
            # The spill folders are paths. The Spark heap, the DuckDB memory, the threads and the DuckDB spill cap are derived from the capacity of the task when a
            # phase starts (scripts/lakehouse/task_capacity.py), so no value here fixes them.
            { name = "HAI_SPARK_LOCAL_DIRECTORY", value = "/workspace/spill" },
            { name = "HAI_DUCKDB_TEMP_DIRECTORY", value = "/workspace/duckdb_spill" },
          ],
          [for name in lookup(local.layer_run_variables[layer], step.name, []) : { name = name, value = "" }],
          step.name == "feast" ? [
            { name = "FEAST_PROFILE", value = "aws" },
            { name = "FEAST_REGISTRY_BUCKET", value = var.data_bucket_name },
          ] : []
        )
        secrets = layer == "silver" && step.name == "build" ? local.layer_build_secrets : (lookup(local.layer_phase_secrets, step.name, "") == "reader" ? local.reader_parameters : (lookup(local.layer_phase_secrets, step.name, "") == "writer" ? lookup(local.writer_parameters, local.layer_roles[layer], []) : []))
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

# A phase that exits with an error does not stop the task: the phases after it wait for its success and never start, and the one essential container (the last
# phase) never runs, so the task stays PENDING until somebody stops it. The guard is a second essential container with no dependency. It reads the task metadata
# endpoint and exits with an error as soon as a phase has failed, which stops the whole task (scripts/orchestration/task_guard.py). It also ends the task 900 seconds
# after the job deadline of the job catalog (config/orchestration/ecs_broker_jobs.json), so a hand-started task has a limit too. The deadline counts from the init
# container and covers the fetch phase and every other phase, so the order is: the driver stops a phase at the deadline, the guard stops the task 900 seconds later
# and the owner's last resort in the guide is 900 seconds after that. The driver also derives the time of the features build from the deadline that is left.
locals {
  layer_guard_seconds = { for layer in local.layer_jobs : layer => jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[layer].deadline_seconds + 900 }
  layer_guards = {
    for layer in local.layer_jobs : layer => {
      name                   = "guard"
      image                  = lookup(var.images, "spark", "invalid")
      essential              = true
      privileged             = false
      readonlyRootFilesystem = true
      user                   = "10001:10001"
      entryPoint             = ["python", "-I", "-c"]
      command                = [file("${path.module}/../../scripts/orchestration/task_guard.py")]
      stopTimeout            = 5
      linuxParameters        = { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true }
      ulimits                = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn              = []
      environment = [
        { name = "GUARD_SECONDS", value = tostring(local.layer_guard_seconds[layer]) },
        { name = "GUARD_INTERVAL_SECONDS", value = "10" },
        { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
      ]
      secrets     = []
      mountPoints = []
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
  }
}

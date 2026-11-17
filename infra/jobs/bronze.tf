variable "bronze_job_enabled" {
  description = "Supply the inputs of the bronze host definition. It needs the spark image digest, the execution role, the bronze writer role and its pair, and the jobs log group. The tables, the target and the dry run flag arrive at dispatch."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition = !var.bronze_job_enabled || (
      contains(keys(var.images), "spark") &&
      contains(keys(var.job_role_arns), "execution") &&
      contains(keys(var.job_role_arns), "write_bronze_task") &&
      contains(keys(var.writer_parameter_names), "write_bronze_task") &&
      var.log_group_name != null
    )
    error_message = "The bronze job needs the spark image digest, the execution role, the bronze writer role and pair, and the jobs log group."
  }
}

variable "bronze_read_environment" {
  description = "The read environment the IAM bronze_writer_contract states when the raw read grant is on. The bronze job runs under that writer role, so every bronze phase receives it. Empty keeps the containers as they were."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition     = alltrue([for name, value in var.bronze_read_environment : name == "HAI_READ_RAW_ROOT" && value == "true"])
    error_message = "Only HAI_READ_RAW_ROOT with the value true is accepted."
  }
}

locals {
  bronze_job_enabled = var.bronze_job_enabled ? toset(["bronze"]) : toset([])
  bronze_role        = "write_bronze_task"
  # The container names and their empty variables equal config/orchestration/ecs_broker_jobs.json; a Terraform test compares them with the file.
  bronze_run_variables = {
    fetch = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "HAI_DEADLINE_UTC"]
    pin   = ["RUN_ID", "POLARIS_URI", "CODE_SHA256", "BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "BRONZE_INVENTORY_SHA256", "HAI_DEADLINE_UTC"]
    load  = ["RUN_ID", "POLARIS_URI", "BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "HAI_DEADLINE_UTC"]
    check = ["RUN_ID", "BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "HAI_DEADLINE_UTC"]
  }
  bronze_driver = "cd /workspace/code && python -m scripts.lakehouse.bronze_run %s --run \"$RUN_ID\""
  bronze_steps = length(local.bronze_job_enabled) == 0 ? [] : concat(
    [{ name = "init", script = "chown 10001:10001 /workspace /tmp" }],
    [for phase in ["fetch", "pin", "load", "check"] : {
      name   = phase
      script = phase == "fetch" ? "python -I -c '${local.layer_fetch_python}'" : format(local.bronze_driver, phase)
    }]
  )
  bronze_containers = [
    for index, step in local.bronze_steps : {
      name                   = step.name
      image                  = lookup(var.images, "spark", "invalid")
      essential              = index == length(local.bronze_steps) - 1
      privileged             = false
      readonlyRootFilesystem = true
      user                   = step.name == "init" ? "0:0" : "10001:10001"
      workingDirectory       = "/workspace"
      # Every phase starts through the in-image deadline runner, which stops the process group at the one absolute deadline the launch sets in HAI_DEADLINE_UTC.
      entryPoint      = step.name == "init" ? ["sh", "-c"] : ["/opt/hai/deadline_run", "sh", "-c"]
      command         = [step.script]
      stopTimeout     = 90
      linuxParameters = { capabilities = { add = [], drop = step.name == "init" ? local.host_init_drop : ["ALL"] }, initProcessEnabled = true }
      ulimits         = [{ name = "core", softLimit = 0, hardLimit = 0 }]
      dependsOn       = index == 0 ? [] : [{ containerName = local.bronze_steps[index - 1].name, condition = "SUCCESS" }]
      environment = concat(
        [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
          { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONHASHSEED", value = "0" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "HAI_SPARK_LOCAL_DIRECTORY", value = "/workspace/spill" },
        ],
        [for name in lookup(local.bronze_run_variables, step.name, []) : { name = name, value = "" }],
        step.name == "init" ? [] : [for name, value in var.bronze_read_environment : { name = name, value = value }],
        # The check phase names the files that no run of the ingestion retry period captured: it reads the run records under the capture root (raw/, only while the
        # raw read is on) and publishes the owner notice to the alerts topic, once the stack is told it (the way the promote task gets it).
        step.name == "check" && contains(keys(var.bronze_read_environment), "HAI_READ_RAW_ROOT") ? [{ name = "HAI_CAPTURE_ROOT", value = "raw/" }] : [],
        step.name == "check" && var.alerts_topic_arn != null ? [{ name = "ALERTS_TOPIC_ARN", value = var.alerts_topic_arn }] : []
      )
      secrets = step.name == "load" ? lookup(local.writer_parameters, local.bronze_role, []) : []
      mountPoints = [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "bronze"
          mode                  = "non-blocking"
          max-buffer-size       = "16m"
        }
      }
    }
  ]
}

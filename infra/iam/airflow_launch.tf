variable "airflow_launch_prepared" {
  description = "Prepare the role of the Airflow scheduler that launches the batch jobs on the host. It may run, stop and describe the host definitions of the named job families only, pass their roles, check the cluster's container instances, read the code digest and the job logs, read and create once the code run binding of a run, read the run record of each bronze run when bronze is named and the drift records when drift is named, and write each launch record once under its own prefix."
  type        = bool
  default     = false
  nullable    = false
}

variable "airflow_launch_families" {
  description = "The job families the launch role may start. Each family is one host task definition <project>_<environment>_job_<family>_host and one task role. The Airflow DAG launches need train, promote, serve and drift."
  type        = list(string)
  default     = ["train"]
  nullable    = false

  validation {
    condition     = alltrue([for family in var.airflow_launch_families : contains(["train", "bronze", "silver", "gold", "features", "drift", "promote", "serve", "metadata", "publisher"], family)]) && length(var.airflow_launch_families) == length(distinct(var.airflow_launch_families))
    error_message = "Name distinct job families from train, bronze, silver, gold, features, drift, promote, serve, metadata and publisher."
  }

  validation {
    condition     = !var.airflow_launch_prepared || length(var.airflow_launch_families) > 0
    error_message = "Name at least one job family, or set airflow_launch_prepared to false."
  }
}

variable "airflow_launch_record_prefix" {
  description = "Key prefix in the data bucket under which the launch role creates, lists and reads its launch records. A record is created once, by a conditional write that fails when the object exists, and is never deleted. Only the decision.json of a run and job may be replaced, and only by a write that carries a version check (If-Match). The list grant lets a missing record read as absent instead of as a refusal."
  type        = string
  default     = "lakehouse/launch_records/"
  nullable    = false

  validation {
    condition     = length(var.airflow_launch_record_prefix) <= 256 && can(regex("^[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*/$", var.airflow_launch_record_prefix)) && !strcontains(var.airflow_launch_record_prefix, "..")
    error_message = "Use a key prefix of at most 256 characters that does not start with a slash, ends with a slash and has parts of letters, digits, underscores, dots and dashes. Wildcards, policy variables, spaces and parent steps are not allowed."
  }
}

variable "host_job_cap_parameter_name" {
  description = "Parameter Store name of the current host job cap that the services stack writes (its variable of the same name). The Airflow launch role may read this one name, and the project user may manage it so the services apply can create, update and delete it."
  type        = string
  default     = "/healthcare_hai_prediction/host_job_cap"
  nullable    = false

  validation {
    condition     = length(var.host_job_cap_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.host_job_cap_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.host_job_cap_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "host_launch_hold_parameter_name" {
  description = "Parameter Store name of the launch hold that the services apply command sets and clears (the services variable of the same name). The Airflow launch role may read this one name; the project user may put and read it."
  type        = string
  default     = "/healthcare_hai_prediction/host_launch_hold"
  nullable    = false

  validation {
    condition     = length(var.host_launch_hold_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.host_launch_hold_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.host_launch_hold_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "services_apply_lock_parameter_name" {
  description = "Parameter Store name of the services apply lock. scripts/infrastructure/services_apply.py creates it without overwrite before each services apply (so a second apply anywhere is refused) and deletes it after the apply settles. The project user may create, read and delete this one name."
  type        = string
  default     = "/healthcare_hai_prediction/services_apply_lock"
  nullable    = false

  validation {
    condition     = length(var.services_apply_lock_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.services_apply_lock_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.services_apply_lock_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

locals {
  # The task role each family runs with, as the host definitions of infra/jobs take them. Promote reuses the training role.
  airflow_launch_family_roles = {
    train     = "job_train_task"
    bronze    = "job_write_bronze_task"
    silver    = "job_write_silver_task"
    gold      = "job_write_gold_task"
    features  = "job_write_features_task"
    drift     = "job_drift_task"
    promote   = "job_train_task"
    serve     = "job_serve_task"
    metadata  = "job_openmetadata_task"
    publisher = "job_publisher_task"
  }
  # The staged request a family's run reads before it starts. Only these three families stage one; the launch reads the publisher's
  # (orchestration/publisher_launch.py) and binds every attempt to its digest.
  airflow_launch_request_files = {
    bronze    = "bronze_request.json"
    serve     = "serving_request.json"
    publisher = "publisher_request.json"
  }
  airflow_launch_families_sorted = sort(var.airflow_launch_families)
  airflow_launch_request_arns = [
    for family in local.airflow_launch_families_sorted : "arn:aws:s3:::${var.data_bucket_name}/lakehouse/run_stage/*/inputs/${local.airflow_launch_request_files[family]}"
    if contains(keys(local.airflow_launch_request_files), family)
  ]
  airflow_launch_policy_values = merge(local.owner_policy_values, {
    PROJECT_TAG                     = local.platform_project_tag
    CLUSTER_ARN                     = "${local.ecs_arn}:cluster/${local.cluster_name}"
    CONTAINER_INSTANCE_ARN_PATTERN  = "${local.ecs_arn}:container-instance/${local.cluster_name}/*"
    TASK_ARN_PATTERN                = "${local.ecs_arn}:task/${local.cluster_name}/*"
    LOG_GROUP_NAME                  = format(local.platform_name, "jobs")
    DATA_BUCKET_NAME                = var.data_bucket_name
    HOST_JOB_CAP_PARAMETER_NAME     = var.host_job_cap_parameter_name
    HOST_LAUNCH_HOLD_PARAMETER_NAME = var.host_launch_hold_parameter_name
    # The launch role reads the services apply lock (ReadHostJobCap); the project user's ssmcap policy grants its create, read and delete.
    SERVICES_APPLY_LOCK_PARAMETER_NAME = var.services_apply_lock_parameter_name
    RECORD_PREFIX                      = var.airflow_launch_record_prefix
    REQUEST_OBJECT_ARNS                = jsonencode(local.airflow_launch_request_arns)
    FAMILIES                           = jsonencode(local.airflow_launch_families_sorted)
    TASK_DEFINITION_ARNS               = jsonencode([for family in local.airflow_launch_families_sorted : "${local.ecs_arn}:task-definition/${var.project_name}_${var.environment}_job_${family}_host:*"])
    ROLE_ARNS = jsonencode(sort(distinct(concat(
      ["arn:aws:iam::${var.expected_account_id}:role/${format(local.platform_name, "job_execution")}"],
      [for family in local.airflow_launch_families_sorted : "arn:aws:iam::${var.expected_account_id}:role/${format(local.platform_name, local.airflow_launch_family_roles[family])}"]
    ))))
  })
}

resource "aws_iam_role" "airflow_launch_task" {
  count = var.airflow_launch_prepared ? 1 : 0

  name                 = format(local.platform_name, "airflow_launch_task")
  description          = "Airflow scheduler: runs, stops and describes the host definitions of the named job families only, passes their roles, writes each launch record once and reads the code digest and the job logs."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "airflow_launch_jobs" {
  count = var.airflow_launch_prepared ? 1 : 0

  name   = "airflow_launch_jobs"
  role   = aws_iam_role.airflow_launch_task[0].name
  policy = templatefile("${path.module}/owner_policies/airflow_launch_jobs.json", local.airflow_launch_policy_values)
}

resource "aws_iam_role_policy" "airflow_launch_records" {
  count = var.airflow_launch_prepared ? 1 : 0

  name   = "airflow_launch_records"
  role   = aws_iam_role.airflow_launch_task[0].name
  policy = templatefile("${path.module}/owner_policies/airflow_launch_records.json", local.airflow_launch_policy_values)
}

# The staged request of a bronze run holds its tables, target and dry run flag, that of a serving deployment names the HAI type, the mode and the candidate, and that of a Run 3 publisher run names its inputs, queue, resumed sources and prior state. Only a launch role that may start the bronze, serve or publisher family can read them.
resource "aws_iam_role_policy" "airflow_launch_requests" {
  count = var.airflow_launch_prepared && length(local.airflow_launch_request_arns) > 0 ? 1 : 0

  name   = "airflow_launch_requests"
  role   = aws_iam_role.airflow_launch_task[0].name
  policy = templatefile("${path.module}/owner_policies/airflow_launch_requests.json", local.airflow_launch_policy_values)
}

# The bronze check container writes lakehouse/bronze_runs/<run>/check.json once. The launch mode reads it as the bronze job's evidence: the table snapshots
# and the objects the run loaded. Only a launch role that may start the bronze family can read it, and only that record name.
resource "aws_iam_role_policy" "airflow_launch_bronze_record" {
  count = var.airflow_launch_prepared && contains(var.airflow_launch_families, "bronze") ? 1 : 0

  name   = "airflow_launch_bronze_record"
  role   = aws_iam_role.airflow_launch_task[0].name
  policy = templatefile("${path.module}/owner_policies/airflow_launch_bronze_record.json", local.airflow_launch_policy_values)
}

# Every job's launch reads lakehouse/run_stage/<run>/inputs/code_run.json, the run whose staged code the run uses (orchestration/ecs_preflight.py,
# S3CodeRuns). The chain gate and the drift step create it once. The list grant on that key alone makes a missing binding answer 404, which reads as the
# run's own code, instead of 403, which the step never reads as missing.
resource "aws_iam_role_policy" "airflow_launch_code_runs" {
  count = var.airflow_launch_prepared ? 1 : 0

  name   = "airflow_launch_code_runs"
  role   = aws_iam_role.airflow_launch_task[0].name
  policy = templatefile("${path.module}/owner_policies/airflow_launch_code_runs.json", local.airflow_launch_policy_values)
}

# The drift DAG's decision step runs in the Airflow scheduler and reads the records the drift task published under drift/. The scheduler runs as the launch
# role, so a launch role that may start the drift family reads them with the same document as the drift stack's read. The control-plane role gains nothing.
resource "aws_iam_role_policy" "airflow_launch_drift_records" {
  count = var.airflow_launch_prepared && contains(var.airflow_launch_families, "drift") ? 1 : 0

  name   = "airflow_launch_drift_records"
  role   = aws_iam_role.airflow_launch_task[0].name
  policy = local.drift_policy_documents.drift_records_read
}

output "airflow_launch_roles" {
  description = "The prepared launch role name, ARN and unique ID, the job families it may start and its record prefix. Empty until airflow_launch_prepared is true. The services stack takes the ARN as airflow_launch_task_role_arn, the task role of the Airflow scheduler."
  value = {
    for role in aws_iam_role.airflow_launch_task : "airflow_launch_task" => {
      name          = role.name
      arn           = role.arn
      unique_id     = role.unique_id
      families      = local.airflow_launch_families_sorted
      record_prefix = var.airflow_launch_record_prefix
    }
  }
}

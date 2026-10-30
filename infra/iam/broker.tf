variable "broker_prepared" {
  description = "Prepare the role of the lean job broker. It may run, stop and describe the named job families only, pass their roles and read the code digest, the job logs and the record of the signed first publishes."
  type        = bool
  default     = false
  nullable    = false
}

variable "broker_job_families" {
  description = "The job families the broker role may start. Each family is one task definition <project>_<environment>_job_<family> and one task role."
  type        = list(string)
  default     = ["train"]
  nullable    = false

  validation {
    condition     = alltrue([for family in var.broker_job_families : contains(["train", "silver", "gold", "features", "drift", "promote", "metadata"], family)]) && length(var.broker_job_families) == length(distinct(var.broker_job_families))
    error_message = "Name distinct job families from train, silver, gold, features, drift, promote and metadata."
  }

  validation {
    condition     = !var.broker_prepared || length(var.broker_job_families) > 0
    error_message = "Name at least one job family, or set broker_prepared to false."
  }
}

variable "broker_token_name" {
  description = "Parameter Store SecureString name of the broker session token. The host execution role may inject it. The value is set outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/broker_token"
  nullable    = false

  validation {
    condition     = length(var.broker_token_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.broker_token_name)) && !can(regex("^/(?i:aws|ssm)", var.broker_token_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "broker_signed_parameter_name" {
  description = "Parameter Store name of the record of the first signed silver and gold publishes. The broker role may read it. The owner sets the value after signing both reports."
  type        = string
  default     = "/healthcare_hai_prediction/first_publishes_signed"
  nullable    = false

  validation {
    condition     = length(var.broker_signed_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.broker_signed_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.broker_signed_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

locals {
  # The task role each family runs with. The drift role does not exist yet: it follows the drift task, so naming drift before then fails at apply.
  broker_family_roles = {
    train    = "job_train_task"
    silver   = "job_write_silver_task"
    gold     = "job_write_gold_task"
    features = "job_write_features_task"
    drift    = "job_drift_task"
    promote  = "job_train_task"
    metadata = "job_openmetadata_task"
  }
  broker_families = sort(var.broker_job_families)
  broker_policy_values = merge(local.owner_policy_values, {
    PROJECT_TAG           = local.platform_project_tag
    CLUSTER_ARN           = "${local.ecs_arn}:cluster/${local.cluster_name}"
    TASK_ARN_PATTERN      = "${local.ecs_arn}:task/${local.cluster_name}/*"
    LOG_GROUP_NAME        = format(local.platform_name, "jobs")
    DATA_BUCKET_NAME      = var.data_bucket_name
    SIGNED_PARAMETER_NAME = var.broker_signed_parameter_name
    BROKER_TOKEN_NAME     = var.broker_token_name
    FAMILIES              = jsonencode(local.broker_families)
    TASK_DEFINITION_ARNS  = jsonencode([for family in local.broker_families : "${local.ecs_arn}:task-definition/${var.project_name}_${var.environment}_job_${family}:*"])
    ROLE_ARNS = jsonencode(sort(distinct(concat(
      ["arn:aws:iam::${var.expected_account_id}:role/${format(local.platform_name, "job_execution")}"],
      [for family in local.broker_families : "arn:aws:iam::${var.expected_account_id}:role/${format(local.platform_name, local.broker_family_roles[family])}"]
    ))))
  })
}

resource "aws_iam_role" "broker_task" {
  count = var.broker_prepared ? 1 : 0

  name                 = format(local.platform_name, "broker_task")
  description          = "Lean job broker: runs, stops and describes the named job families only, passes their roles and reads the code digest, the job logs and the signed publish record."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "broker_jobs" {
  count = var.broker_prepared ? 1 : 0

  name   = "broker_jobs"
  role   = aws_iam_role.broker_task[0].name
  policy = templatefile("${path.module}/owner_policies/broker_jobs.json", local.broker_policy_values)
}

# The host execution role injects the token into the broker and the scheduler. It holds this one parameter grant beside its other eight and the MLflow pair.
resource "aws_iam_role_policy" "broker_parameter" {
  count = var.broker_prepared ? 1 : 0

  name   = "broker_parameter"
  role   = aws_iam_role.task_execution.name
  policy = templatefile("${path.module}/owner_policies/broker_parameter.json", local.broker_policy_values)
}

output "broker_roles" {
  description = "The prepared broker role name, ARN and unique ID, and the job families it may start. Empty until broker_prepared is true. The services stack takes the ARN as broker_task_role_arn."
  value       = { for role in aws_iam_role.broker_task : "broker_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id, families = local.broker_families } }
}

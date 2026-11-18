variable "openmetadata_persistent_prepared" {
  description = "Prepare the persistent OpenMetadata catalog on the container host: the bootstrap task role, the execution grants for its two passwords, and the Session Manager document for the UI. Off by default. It does not need the finite catalog task."
  type        = bool
  default     = false
  nullable    = false
}

variable "airflow_lineage_prepared" {
  description = "Prepare the lineage bot token grant for the Airflow scheduler: the shared task execution role may inject exactly the lineage bot token parameter (openmetadata_bot_token_names[\"lineage\"]) as OPENLINEAGE__TRANSPORT__AUTH__API_KEY. Off by default. Needs openmetadata_persistent_prepared. The services input airflow_lineage_http_enabled needs this grant first."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.airflow_lineage_prepared || var.openmetadata_persistent_prepared
    error_message = "airflow_lineage_prepared needs openmetadata_persistent_prepared, because the lineage bot token parameter belongs to the persistent catalog."
  }
}

variable "job_lineage_prepared" {
  description = "Prepare the lineage bot token grant for the layer jobs: the shared job execution role may inject exactly the lineage bot token parameter (openmetadata_bot_token_names[\"lineage\"]) into the pin and upload containers of the silver, gold and features host jobs, which send their own lineage events (orchestration/job_lineage.py). Off by default. Needs openmetadata_persistent_prepared. The jobs input job_lineage_enabled needs this grant first: a task whose definition names a secret its execution role may not read does not start."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.job_lineage_prepared || var.openmetadata_persistent_prepared
    error_message = "job_lineage_prepared needs openmetadata_persistent_prepared, because the lineage bot token parameter belongs to the persistent catalog."
  }
}

variable "openmetadata_db_password_name" {
  description = "Parameter Store SecureString name of the password of the OpenMetadata database role. The host execution role may inject exactly this parameter. The owner sets the value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_db_password"
  nullable    = false

  validation {
    condition     = length(var.openmetadata_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.openmetadata_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.openmetadata_db_password_name))
    error_message = "Use a parameter path of letters, digits and underscores that starts with a slash; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "mlflow_metadata_reader_password_name" {
  description = "Parameter Store SecureString name of the password of the read-only MLflow user metadata_reader, which the MLflow source of the metadata job reads the model registry with. The job execution role may inject exactly this parameter. The owner step scripts/infrastructure/mlflow_reader_setup.py writes it; Terraform never reads a value."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_metadata_reader_password"
  nullable    = false

  validation {
    condition = (
      length(var.mlflow_metadata_reader_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.mlflow_metadata_reader_password_name)) && !can(regex("^/(?i:aws|ssm)", var.mlflow_metadata_reader_password_name)) &&
      !contains([var.mlflow_admin_password_name, var.openmetadata_admin_password_name, var.openmetadata_db_password_name], var.mlflow_metadata_reader_password_name) &&
      !contains(values(var.openmetadata_bot_token_names), var.mlflow_metadata_reader_password_name)
    )
    error_message = "Use its own parameter path of letters, digits and underscores that starts with a slash, never the MLflow administrator password, a catalog password or a bot token."
  }
}

variable "openmetadata_bot_token_names" {
  description = "Parameter Store SecureString name of the bot token of each caller: lineage and ingestion. The bootstrap task role may read and write exactly these two parameters. Terraform never reads a value."
  type        = map(string)
  default = {
    lineage   = "/healthcare_hai_prediction/openmetadata_bot_lineage"
    ingestion = "/healthcare_hai_prediction/openmetadata_bot_ingestion"
  }
  nullable = false

  validation {
    condition = (
      toset(keys(var.openmetadata_bot_token_names)) == toset(["lineage", "ingestion"]) &&
      alltrue([for name in values(var.openmetadata_bot_token_names) : length(name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", name)) && !can(regex("^/(?i:aws|ssm)", name))]) &&
      length(distinct(values(var.openmetadata_bot_token_names))) == 2 &&
      !contains(values(var.openmetadata_bot_token_names), var.openmetadata_admin_password_name) &&
      !contains(values(var.openmetadata_bot_token_names), var.openmetadata_db_password_name)
    )
    error_message = "Name exactly the callers lineage and ingestion, each with its own parameter path of letters, digits and underscores, none equal to another or to the administrator or database password."
  }
}

locals {
  openmetadata_parameter_arn = { for caller, name in var.openmetadata_bot_token_names : caller => "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${name}" }
}

# The host bootstrap task writes the bot tokens it issues. Nothing else of the control plane may write them.
resource "aws_iam_role" "openmetadata_host_task" {
  count = var.openmetadata_persistent_prepared ? 1 : 0

  name                 = format(local.platform_name, "openmetadata_host_task")
  description          = "Persistent catalog bootstrap: reads and writes the three bot token parameters and nothing else."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "openmetadata_host_task" {
  count = var.openmetadata_persistent_prepared ? 1 : 0

  name = "bot_tokens"
  role = aws_iam_role.openmetadata_host_task[0].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadAndWriteTheBotTokenParameters"
      Effect   = "Allow"
      Action   = ["ssm:GetParameter", "ssm:PutParameter"]
      Resource = [for caller in sort(keys(local.openmetadata_parameter_arn)) : local.openmetadata_parameter_arn[caller]]
    }]
  })
}

# The administrator and database passwords are injected through the host execution role. The base policy stays as it was.
resource "aws_iam_role_policy" "task_execution_openmetadata" {
  count = var.openmetadata_persistent_prepared ? 1 : 0

  name = "task_execution_openmetadata"
  role = aws_iam_role.task_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "InjectCatalogPasswords"
      Effect = "Allow"
      Action = "ssm:GetParameters"
      Resource = [
        "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.openmetadata_admin_password_name}",
        "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.openmetadata_db_password_name}",
      ]
    }]
  })
}

# The Airflow scheduler gets the lineage bot token through the shared task execution role. The password grant above stays byte equal. The parameter is a
# SecureString under the AWS-managed key aws/ssm, so the role holds no KMS permission (roles.tf).
resource "aws_iam_role_policy" "task_execution_airflow_lineage" {
  count = var.airflow_lineage_prepared ? 1 : 0

  name = "task_execution_airflow_lineage"
  role = aws_iam_role.task_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InjectLineageBotToken"
      Effect   = "Allow"
      Action   = "ssm:GetParameters"
      Resource = [local.openmetadata_parameter_arn["lineage"]]
    }]
  })
}

# The ingestion steps get the ingestion bot token through the shared job execution role. The base job policies stay as they were.
resource "aws_iam_role_policy" "job_execution_openmetadata_ingestion_token" {
  count = var.openmetadata_persistent_prepared ? 1 : 0

  name = "openmetadata_ingestion_token"
  role = aws_iam_role.job["execution"].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InjectIngestionBotToken"
      Effect   = "Allow"
      Action   = "ssm:GetParameters"
      Resource = [local.openmetadata_parameter_arn["ingestion"]]
    }]
  })
}

# The registry reader password reaches the metadata job's ingest container through the shared job execution role, beside the ingestion token. The MLflow
# administrator password stays out of it: the source reads with the read-only user.
resource "aws_iam_role_policy" "job_execution_mlflow_metadata_reader" {
  count = var.openmetadata_persistent_prepared ? 1 : 0

  name = "mlflow_metadata_reader_password"
  role = aws_iam_role.job["execution"].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InjectRegistryReaderPassword"
      Effect   = "Allow"
      Action   = "ssm:GetParameters"
      Resource = ["arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.mlflow_metadata_reader_password_name}"]
    }]
  })
}

# The layer jobs get the lineage bot token through the shared job execution role. It is the parameter the scheduler already sends with (task_execution_airflow_lineage),
# so a job's events and the scheduler's have one sender identity. The ingestion token grant above stays byte equal.
resource "aws_iam_role_policy" "job_execution_lineage_token" {
  count = var.job_lineage_prepared ? 1 : 0

  name = "openmetadata_lineage_token"
  role = aws_iam_role.job["execution"].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InjectLineageBotToken"
      Effect   = "Allow"
      Action   = "ssm:GetParameters"
      Resource = [local.openmetadata_parameter_arn["lineage"]]
    }]
  })
}

# The owner reaches the catalog UI on the host through this Session Manager document and no other. The destination port is fixed.
resource "aws_ssm_document" "openmetadata_ui_forward" {
  count = var.openmetadata_persistent_prepared ? 1 : 0

  name          = format(local.platform_name, "openmetadata_forward")
  document_type = "Session"
  tags          = local.project_tags
  content = jsonencode({
    schemaVersion = "1.0"
    description   = "Forward a local port to the persistent catalog UI on the container host. The destination port is fixed at 8585."
    sessionType   = "Port"
    parameters = {
      localPortNumber = {
        type           = "String"
        description    = "Local port to listen on."
        allowedPattern = "^[0-9]{1,5}$"
        default        = "8585"
      }
    }
    properties = {
      portNumber      = "8585"
      type            = "LocalPortForwarding"
      localPortNumber = "{{ localPortNumber }}"
    }
  })
}

output "openmetadata_persistent_roles" {
  description = "The prepared persistent catalog bootstrap role name, ARN and unique ID. Empty until openmetadata_persistent_prepared is true. The services root takes openmetadata_host_task as openmetadata_task_role_arn."
  value       = { for role in aws_iam_role.openmetadata_host_task : "openmetadata_host_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
}

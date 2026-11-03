variable "aws_profile" {
  description = "Only the project deployment profile may run this stack; IAM administration is a separate root."
  type        = string
  nullable    = false

  validation {
    condition     = var.aws_profile == "${var.project_name}_${var.environment}" && length(var.aws_profile) <= 64
    error_message = "AWS_PROFILE must be PROJECT_NAME, an underscore and ENVIRONMENT, and a valid IAM username; use iam/ for permission updates."
  }
}

variable "environment" {
  description = "Deployment environment; also the suffix of the project profile and its IAM user."
  type        = string
  nullable    = false

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "ENVIRONMENT must be dev or prod."
  }
}

variable "data_bucket_name" {
  description = "Exact approved acquisition bucket name, supplied through ignored local configuration."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.data_bucket_name))
    error_message = "Use a 3-63 character bucket name containing lowercase letters, digits and hyphens."
  }
}

variable "project_name" {
  description = "Underscore project identifier supplied from .env."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_name))
    error_message = "Use a lowercase project name containing letters, digits and underscores."
  }
}

variable "aws_region" {
  description = "User-selected AWS region; verify any existing bucket is in this region before planning."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Supply an AWS region code such as eu-west-1, without surrounding whitespace."
  }
}

variable "expected_account_id" {
  description = "Account verified through the new project profile before planning or applying."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply the verified 12-digit AWS account ID."
  }
}

variable "terraform_state_enabled" {
  description = "Allow Terraform state under terraform_state/ in this bucket: a lifecycle rule for replaced state versions and bucket policy statements for the project user. Off by default. The owner turns it on in a reviewed commit before the state move."
  type        = bool
  default     = false
  nullable    = false
}

variable "athena_retention_enabled" {
  description = "Add the rule that expires Athena query results under athena/results/ after 7 days and their replaced versions after 1 day. Off by default, because the rule changes the live lifecycle configuration of the data bucket; the owner sets it in the apply step that prepares the Athena mirror."
  type        = bool
  default     = false
  nullable    = false
}

variable "terraform_state_admin_principals" {
  description = "The administrator that applies the IAM stack, which keeps its state under terraform_state/iam/, as role/NAME or user/NAME. The account comes from expected_account_id, so no account ID is written here. Required when terraform_state_enabled is true."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = alltrue([for name in var.terraform_state_admin_principals : can(regex("^(user|role)/[A-Za-z0-9_+=,.@/-]+$", name)) && !can(regex("[0-9]{12}", name))])
    error_message = "Use role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID."
  }
}

variable "access_logging_enabled" {
  description = "Deliver server access logs of the project bucket to a logging bucket of this stack, with a 30-day expiry. Off by default. Needs the project profile to hold the logging permissions first (project guide: access logging)."
  type        = bool
  default     = false
  nullable    = false
}

variable "mlflow_retention_enabled" {
  description = "Add the rule that expires replaced MLflow artifact versions under mlflow/ after 30 days. Off by default, because the rule changes the live lifecycle configuration of the data bucket; the owner sets it in the apply step that prepares MLflow."
  type        = bool
  default     = false
  nullable    = false
}

variable "access_logging_reader_principals" {
  description = "The owner's administrator path, the only principals that may read the logs, as role/NAME or user/NAME. The account comes from expected_account_id. Required when access_logging_enabled is true."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = alltrue([for name in var.access_logging_reader_principals : can(regex("^(user|role)/[A-Za-z0-9_+=,.@/-]+$", name)) && !can(regex("[0-9]{12}", name))])
    error_message = "Use role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID."
  }
}

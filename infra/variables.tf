variable "aws_profile" {
  description = "Only the project deployment profile may run this stack; IAM administration is a separate root."
  type        = string
  nullable    = false

  validation {
    condition     = var.aws_profile == "${var.project_name}_user" && length(var.aws_profile) <= 64
    error_message = "AWS_PROFILE must match PROJECT_NAME followed by _user and be a valid IAM username; use iam/ for permission updates."
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

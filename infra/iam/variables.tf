variable "aws_profile" {
  description = "Explicitly approved IAM administrator profile, not the deployment profile."
  type        = string
  nullable    = false

  validation {
    condition     = length(trimspace(var.aws_profile)) > 0 && var.aws_profile != "healthcare_hai_prediction_user"
    error_message = "Supply the approved administrator profile explicitly; the project user must not manage its own permissions."
  }
}

variable "aws_region" {
  description = "Approved region for project storage."
  type        = string
  default     = "us-west-2"
}

variable "expected_account_id" {
  description = "Verified account in which the existing project IAM user resides."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply the verified 12-digit AWS account ID."
  }
}

variable "data_bucket_name" {
  description = "The exact bucket the policies may manage."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.data_bucket_name))
    error_message = "Use a 3-63 character bucket name containing lowercase letters, digits and hyphens."
  }
}

variable "project_name" {
  description = "Prefix rendered into service policy names from .env."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_name))
    error_message = "Use a lowercase project name containing letters, digits and underscores."
  }
}

variable "deployment_user_name" {
  description = "Existing project IAM user derived from AWS_PROFILE; this configuration does not create identities or access keys."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_+=,.@-]{1,64}$", var.deployment_user_name))
    error_message = "Supply an existing IAM user name, not an ARN."
  }
}

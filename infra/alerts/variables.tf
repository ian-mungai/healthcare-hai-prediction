variable "aws_profile" {
  description = "Only the project deployment profile may run this root; IAM administration is a separate root."
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

variable "project_name" {
  description = "Underscore project identifier supplied from .env."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_name))
    error_message = "Use a lowercase project name containing letters, digits and underscores."
  }
}

variable "aws_region" {
  description = "User-selected AWS region; the topics are in the same region as the platform."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Supply an AWS region code such as eu-west-1, without surrounding whitespace."
  }
}

variable "expected_account_id" {
  description = "Account verified through the project profile before planning or applying."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply the verified 12-digit AWS account ID."
  }
}

variable "alerts_topic_arn" {
  description = "The platform alerts topic, read from the platform output alerts_topic_arn when the inputs are rendered. Only that topic of this project, environment, region and account is accepted."
  type        = string
  nullable    = false

  validation {
    condition     = var.alerts_topic_arn == "arn:aws:sns:${var.aws_region}:${var.expected_account_id}:${var.project_name}_alerts_${var.environment}"
    error_message = "Supply the alerts_topic_arn output of the applied platform stack: the project's alerts topic in this account, region and environment."
  }
}

variable "alert_email" {
  description = "The one address bootstrap subscribes to every alert topic, supplied from .env. Empty creates no subscription. The address is not a secret value but it names a person, so the input is sensitive and stays out of plan output."
  type        = string
  default     = ""
  nullable    = false
  sensitive   = true

  validation {
    condition     = var.alert_email == "" || (length(var.alert_email) <= 254 && can(regex("^[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)+$", var.alert_email)))
    error_message = "Set ALERT_EMAIL to empty or one plain address such as name@example.org, without a display name, spaces, a list or a scheme."
  }
}

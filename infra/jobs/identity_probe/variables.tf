variable "aws_profile" {
  description = "Project deployment profile from the environment renderer."
  type        = string
  nullable    = false

  validation {
    condition     = var.aws_profile == "${var.project_name}_${var.environment}" && length(var.aws_profile) <= 64
    error_message = "Use the project profile named <project>_<environment>."
  }
}

variable "aws_region" {
  description = "Region of the approved seed image and regional STS call."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Supply an AWS region code."
  }
}

variable "expected_account_id" {
  description = "Verified account that owns the seed image and merged roles."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply a twelve-digit account ID."
  }
}

variable "project_name" {
  description = "Underscore project identifier and foundation Project tag."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,47}$", var.project_name))
    error_message = "Use 3 to 48 lowercase letters, digits and underscores, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment of the foundation and job roles."
  type        = string
  nullable    = false

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Use dev or prod."
  }
}

variable "probe_enabled" {
  description = "Register the separate seed definition only after review of its exact image digest and plan."
  type        = bool
  default     = false
  nullable    = false
}

variable "seed_image" {
  description = "Approved existing Spark image with Python, the AWS SDK and timeout, pinned by ECR digest. No rebuilt worker is needed."
  type        = string
  default     = null

  validation {
    condition = !var.probe_enabled || can(regex(
      "^${var.expected_account_id}\\.dkr\\.ecr\\.${var.aws_region}\\.amazonaws\\.com/${var.project_name}_spark_${var.environment}@sha256:[a-f0-9]{64}$", var.seed_image
    ))
    error_message = "Supply the approved existing project Spark image with an immutable SHA-256 digest."
  }
}

variable "execution_role_arn" {
  description = "Merged job execution role for the seed image pull and existing jobs log group."
  type        = string
  default     = null

  validation {
    condition     = !var.probe_enabled || var.execution_role_arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_execution_${var.environment}"
    error_message = "Bind the merged job execution role."
  }
}

variable "task_role_arn" {
  description = "Merged offline task role used only for the bounded identity comparison."
  type        = string
  default     = null

  validation {
    condition     = !var.probe_enabled || var.task_role_arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_offline_task_${var.environment}"
    error_message = "Bind only the merged offline job task role, never a writer or foundation role."
  }
}

variable "task_role_unique_id" {
  description = "Merged IAM job_roles offline_task unique_id for comparison with the authenticated STS UserId."
  type        = string
  default     = null

  validation {
    condition     = !var.probe_enabled || can(regex("^AROA[A-Z0-9]{17}$", var.task_role_unique_id))
    error_message = "Supply the merged offline task role unique ID."
  }
}

variable "log_group_name" {
  description = "Existing jobs log group exported by the platform."
  type        = string
  default     = null

  validation {
    condition     = !var.probe_enabled || var.log_group_name == "${var.project_name}_jobs_${var.environment}"
    error_message = "Supply the existing platform jobs log group."
  }
}

variable "foundation_network" {
  description = "Foundation public subnet IDs and broad task group IDs, for this separately approved one-task probe only."
  type = object({
    subnet_ids         = list(string)
    security_group_ids = list(string)
  })
  default = null

  validation {
    condition = !var.probe_enabled || try(
      length(var.foundation_network.subnet_ids) > 0 && length(var.foundation_network.subnet_ids) <= 16 &&
      length(var.foundation_network.security_group_ids) > 0 && length(var.foundation_network.security_group_ids) <= 5 &&
      alltrue([for id in var.foundation_network.subnet_ids : can(regex("^subnet-([a-f0-9]{8}|[a-f0-9]{17})$", id))]) &&
      alltrue([for id in var.foundation_network.security_group_ids : can(regex("^sg-([a-f0-9]{8}|[a-f0-9]{17})$", id))]), false
    )
    error_message = "Supply the reviewed foundation subnets and task security group for the probe dispatch contract."
  }
}

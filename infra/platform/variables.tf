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

variable "project_name" {
  description = "Underscore project identifier supplied from .env."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_name))
    error_message = "Use a lowercase project name containing letters, digits and underscores."
  }
}

variable "aws_region" {
  description = "User-selected AWS region; the platform runs in the same region as the data bucket."
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

variable "data_volume_gb" {
  description = "Size of the host's gp3 data volume, mounted at the data mount path."
  type        = number
  default     = 60

  validation {
    condition     = var.data_volume_gb >= 1 && var.data_volume_gb <= 1024
    error_message = "Use a data volume size from 1 to 1024 GB."
  }
}

variable "snapshot_retain_count" {
  description = "Number of daily data-volume snapshots kept."
  type        = number
  default     = 7

  validation {
    condition     = var.snapshot_retain_count >= 1 && var.snapshot_retain_count <= 30
    error_message = "Keep from 1 to 30 daily snapshots."
  }
}

variable "snapshot_time_utc" {
  description = "Daily snapshot start time in UTC, as HH:MM."
  type        = string
  default     = "09:00"

  validation {
    condition     = can(regex("^([01][0-9]|2[0-3]):[0-5][0-9]$", var.snapshot_time_utc))
    error_message = "Write the snapshot time as HH:MM in 24-hour UTC."
  }
}

variable "ecr_repositories" {
  description = "Image names that get an ECR repository; the repository name is the project, the image and the environment."
  type        = set(string)
  default = [
    "airflow", "analytics", "egress_proxy", "marquez", "marquez_web", "polaris", "polaris_admin", "postgres", "quality", "redis", "spark"
  ]

  validation {
    condition     = length(var.ecr_repositories) > 0 && alltrue([for name in var.ecr_repositories : can(regex("^[a-z][a-z0-9_]{1,40}$", name))])
    error_message = "Image names use lowercase letters, digits and underscores."
  }
}

variable "monitoring_enabled" {
  description = "Add the ECR repositories and the log group of the monitoring stack. False leaves them out."
  type        = bool
  default     = false
}

variable "ecr_keep_images" {
  description = "Number of most recent images each repository keeps."
  type        = number
  default     = 5

  validation {
    condition     = var.ecr_keep_images >= 1 && var.ecr_keep_images <= 50
    error_message = "Keep from 1 to 50 images."
  }
}

variable "log_groups" {
  description = "Log group names for the host's services and the run tasks."
  type        = set(string)
  default     = ["airflow", "jobs", "marquez", "polaris", "postgres", "redis"]

  validation {
    condition     = length(var.log_groups) > 0 && alltrue([for name in var.log_groups : can(regex("^[a-z][a-z0-9_]{1,40}$", name))])
    error_message = "Log group names use lowercase letters, digits and underscores."
  }
}

variable "log_retention_days" {
  description = "Days CloudWatch keeps platform logs."
  type        = number
  default     = 7

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30], var.log_retention_days)
    error_message = "Use a CloudWatch retention of 1, 3, 5, 7, 14 or 30 days."
  }
}

variable "allow_destructive_teardown" {
  description = "Allow terraform destroy to delete ECR repositories that still hold images. Set true only for an approved teardown."
  type        = bool
  default     = false
  nullable    = false
}

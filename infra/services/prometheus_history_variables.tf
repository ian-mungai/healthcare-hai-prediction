variable "prometheus_history_task_role_arn" {
  description = "IAM output prometheus_history_roles.shipper: the task role of the Prometheus history shipper. It creates each finished metric block once under prometheus/blocks/ of the data bucket and lists that prefix, and holds no read, delete or overwrite. Null plans no shipper, no restore one-shot and the default Prometheus block durations (history is then not kept)."
  type        = string
  default     = null

  validation {
    condition     = var.prometheus_history_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.prometheus_history_task_role_arn, "none")))
    error_message = "Supply an IAM role ARN for the Prometheus history shipper, or leave prometheus_history_task_role_arn null."
  }

  validation {
    condition     = var.prometheus_history_task_role_arn == null || var.prometheus_history_bucket_name != null
    error_message = "Prometheus history needs prometheus_history_bucket_name, the data bucket that holds prometheus/blocks/."
  }

  validation {
    condition     = var.prometheus_history_task_role_arn == null || var.monitoring_enabled
    error_message = "Prometheus history needs monitoring_enabled: there is no Prometheus data to ship without it."
  }

  validation {
    condition = var.prometheus_history_task_role_arn == null || !contains(
      [var.control_plane_task_role_arn, var.polaris_task_role_arn, var.mlflow_task_role_arn, var.openmetadata_task_role_arn], var.prometheus_history_task_role_arn
    )
    error_message = "The shipper must have its own task role, so no service holds its write and it holds none of theirs."
  }
}

variable "prometheus_restore_task_role_arn" {
  description = "IAM output prometheus_history_roles.restore: the task role of the restore one-shot. It reads and lists prometheus/blocks/ of the data bucket and nothing else. Null with the shipper role null."
  type        = string
  default     = null

  validation {
    condition     = var.prometheus_restore_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.prometheus_restore_task_role_arn, "none")))
    error_message = "Supply an IAM role ARN for the Prometheus restore one-shot, or leave prometheus_restore_task_role_arn null."
  }

  validation {
    condition     = (var.prometheus_history_task_role_arn == null) == (var.prometheus_restore_task_role_arn == null)
    error_message = "Supply both the shipper role (prometheus_history_task_role_arn) and the restore role, or neither."
  }

  validation {
    condition     = var.prometheus_restore_task_role_arn == null || var.prometheus_restore_task_role_arn != var.prometheus_history_task_role_arn
    error_message = "The restore role and the shipper role must be two roles: the restore never writes and the shipper never reads."
  }

  validation {
    condition = var.prometheus_restore_task_role_arn == null || !contains(
      [var.control_plane_task_role_arn, var.polaris_task_role_arn, var.mlflow_task_role_arn, var.openmetadata_task_role_arn], var.prometheus_restore_task_role_arn
    )
    error_message = "The restore one-shot must have its own task role."
  }
}

variable "prometheus_history_bucket_name" {
  description = "Name of the data bucket whose prometheus/blocks/ holds the shipped Prometheus blocks. Required while prometheus_history_task_role_arn is set."
  type        = string
  default     = null

  validation {
    condition     = var.prometheus_history_bucket_name == null || can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", coalesce(var.prometheus_history_bucket_name, "x")))
    error_message = "Use an S3 bucket name of lowercase letters, digits and dashes."
  }
}

variable "prometheus_history_memory_mib" {
  description = "Hard limit and reservation in MiB of the Prometheus history shipper (Python and boto3 in the training image, 128 and 56). The reservation counts in the service reservations only while the shipper is planned, and it lowers the host job cap by the same amount (with every service on, 8080 + 48 = 8128 MiB, so the cap is at most 32768 - 2048 - 8128 = 22592 MiB). An estimate, not a measurement: replace it after the chain run shows the real peak."
  type        = object({ limit = number, reservation = number })
  default     = { limit = 128, reservation = 48 }
  nullable    = false

  validation {
    condition     = var.prometheus_history_memory_mib.limit >= 64 && var.prometheus_history_memory_mib.reservation >= 1 && var.prometheus_history_memory_mib.reservation <= var.prometheus_history_memory_mib.limit
    error_message = "The shipper limit needs at least 64 MiB, and its reservation at least 1 MiB and at most its limit."
  }
}

variable "prometheus_history_interval_seconds" {
  description = "Seconds between two shipper passes. The newest block is cut about 1 to 3 hours after its start, so the shipper adds at most this much to the unshipped newest data (about 3 hours at a teardown)."
  type        = number
  default     = 300
  nullable    = false

  validation {
    condition     = var.prometheus_history_interval_seconds >= 60 && var.prometheus_history_interval_seconds <= 3600 && floor(var.prometheus_history_interval_seconds) == var.prometheus_history_interval_seconds
    error_message = "Use a whole number of seconds from 60 to 3600."
  }
}

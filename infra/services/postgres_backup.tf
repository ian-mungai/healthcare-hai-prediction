variable "postgres_backup_bucket_name" {
  description = "Name of the data bucket whose pg_backups/ prefix holds the Postgres dumps. Required while postgres_backup_enabled or postgres_restore_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.postgres_backup_bucket_name == null || can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", coalesce(var.postgres_backup_bucket_name, "x")))
    error_message = "Use an S3 bucket name of lowercase letters, digits and dashes."
  }
}

variable "postgres_backup_enabled" {
  description = "Run the Postgres backup service: every postgres_backup_interval_seconds it dumps each database with pg_dump -Fc and uploads the set to pg_backups/ in the data bucket, write-once. Needs postgres_backup_task_role_arn and postgres_backup_bucket_name. Off by default."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.postgres_backup_enabled || (var.postgres_backup_task_role_arn != null && var.postgres_backup_bucket_name != null)
    error_message = "postgres_backup_enabled needs postgres_backup_task_role_arn and postgres_backup_bucket_name."
  }
}

variable "postgres_backup_task_role_arn" {
  description = "IAM output restore_after_teardown_roles.postgres_backup: the task role of the backup service. It may only create objects under pg_backups/ and only once per key. Null plans no backup service."
  type        = string
  default     = null

  validation {
    condition     = var.postgres_backup_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/[a-z][a-z0-9_]+_postgres_backup_task_(dev|prod)$", coalesce(var.postgres_backup_task_role_arn, "none")))
    error_message = "Supply the postgres_backup_task IAM role ARN, or leave postgres_backup_task_role_arn null."
  }
}

variable "postgres_restore_enabled" {
  description = "Restore each empty database from the newest completed dump in pg_backups/ when the postgres task starts. Needs postgres_restore_task_role_arn and postgres_backup_bucket_name. Off by default, so the default postgres task is unchanged."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.postgres_restore_enabled || (var.postgres_restore_task_role_arn != null && var.postgres_backup_bucket_name != null)
    error_message = "postgres_restore_enabled needs postgres_restore_task_role_arn and postgres_backup_bucket_name."
  }
}

variable "postgres_restore_task_role_arn" {
  description = "IAM output restore_after_teardown_roles.postgres_restore: the task role of the postgres task while restore is on. It may only list and read pg_backups/."
  type        = string
  default     = null

  validation {
    condition     = var.postgres_restore_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/[a-z][a-z0-9_]+_postgres_restore_task_(dev|prod)$", coalesce(var.postgres_restore_task_role_arn, "none")))
    error_message = "Supply the postgres_restore_task IAM role ARN, or leave postgres_restore_task_role_arn null."
  }
}

variable "postgres_backup_interval_seconds" {
  description = "Seconds between two dump runs of the backup service. A failed run retries after 900 seconds."
  type        = number
  default     = 21600

  validation {
    condition     = var.postgres_backup_interval_seconds >= 900 && var.postgres_backup_interval_seconds <= 86400
    error_message = "Use 900 to 86400 seconds."
  }
}

locals {
  pg_backup_on  = var.postgres_backup_enabled
  pg_restore_on = var.postgres_restore_enabled
  pg_bucket     = coalesce(var.postgres_backup_bucket_name, "unset")

  # The database and owner of each database the task manages, in the order the setup handles them. The list names exactly what the task
  # creates, so a dump and a restore cover the same databases.
  postgres_database_pairs = concat(
    ["polaris:polaris", "airflow:airflow"],
    var.mlflow_enabled ? ["mlflow:mlflow", "mlflow_auth:mlflow"] : [],
    local.om_on ? ["openmetadata_db:openmetadata_user"] : [],
  )
  postgres_database_names = [for pair in local.postgres_database_pairs : split(":", pair)[0]]
  postgres_role_pairs = concat(
    ["airflow:AIRFLOW_DB_PASSWORD"],
    var.mlflow_enabled ? ["mlflow:MLFLOW_DB_PASSWORD"] : [],
    local.om_on ? ["openmetadata_user:OPENMETADATA_DB_PASSWORD"] : [],
  )

  pg_backup_path  = "${var.data_mount_path}/postgres_backup"
  pg_restore_path = "${var.data_mount_path}/postgres_restore"

  pg_client_environment = [
    { name = "PGCONNECT_TIMEOUT", value = "30" },
    { name = "PGDATABASE", value = "polaris" },
    { name = "PGHOST", value = "127.0.0.1" },
    { name = "PGUSER", value = "polaris" },
  ]
  pg_aws_environment = [
    { name = "AWS_DEFAULT_REGION", value = var.aws_region },
    { name = "AWS_REGION", value = var.aws_region },
    { name = "DATA_BUCKET_NAME", value = local.pg_bucket },
    { name = "HOME", value = "/tmp" },
    { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
    { name = "PYTHONUNBUFFERED", value = "1" },
  ]

  # The fetch container runs first and reads the newest completed backup set. The databases container then creates the roles and restores each empty database.
  postgres_restore_fetch_container = merge(local.container_defaults, {
    name                   = "postgres_restore_fetch"
    image                  = lookup(local.image, "training", "")
    user                   = "999:999"
    essential              = false
    readonlyRootFilesystem = true
    memory                 = 512
    memoryReservation      = 64
    stopTimeout            = 30
    entryPoint             = ["python", "-I", "-c"]
    command                = [file("${path.module}/scripts/pg_restore_fetch.py")]
    environment = concat(local.pg_aws_environment, [
      { name = "PG_DATABASES", value = join(" ", local.postgres_database_pairs) },
      { name = "RESTORE_DIR", value = "/restore" },
    ])
    mountPoints = [{ sourceVolume = "postgres_restore", containerPath = "/restore", readOnly = false }]
    linuxParameters = {
      capabilities = { add = [], drop = ["ALL"] }
      tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
    }
    logConfiguration = lookup(local.log_configuration, "postgres_restore_fetch", null)
  })

  postgres_backup_containers = [
    merge(local.container_defaults, {
      name              = "postgres_backup_dump"
      image             = local.image["postgres"]
      user              = "999:999"
      memory            = 512
      memoryReservation = 64
      stopTimeout       = 120
      entryPoint        = ["sh", "-ec"]
      command           = [file("${path.module}/scripts/pg_backup_dump.sh")]
      environment = concat(local.pg_client_environment, [
        { name = "BACKUP_DIR", value = "/backup" },
        { name = "BACKUP_FIRST_DELAY_SECONDS", value = "900" },
        { name = "BACKUP_INTERVAL_SECONDS", value = tostring(var.postgres_backup_interval_seconds) },
        { name = "BACKUP_RETRY_SECONDS", value = "900" },
        { name = "DUMP_SECONDS", value = "900" },
        { name = "PG_DATABASES", value = join(" ", local.postgres_database_names) },
      ])
      secrets          = [{ name = "PGPASSWORD", valueFrom = local.secret_arn["postgres_password"] }]
      mountPoints      = [{ sourceVolume = "postgres_backup", containerPath = "/backup", readOnly = false }]
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "postgres_backup_dump", null)
    }),
    merge(local.container_defaults, {
      name                   = "postgres_backup_upload"
      image                  = lookup(local.image, "training", "")
      user                   = "999:999"
      readonlyRootFilesystem = true
      memory                 = 512
      memoryReservation      = 64
      stopTimeout            = 30
      entryPoint             = ["python", "-I", "-c"]
      command                = [file("${path.module}/scripts/pg_backup_upload.py")]
      environment            = concat(local.pg_aws_environment, [{ name = "BACKUP_DIR", value = "/backup" }])
      mountPoints            = [{ sourceVolume = "postgres_backup", containerPath = "/backup", readOnly = false }]
      linuxParameters = {
        capabilities = { add = [], drop = ["ALL"] }
        tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
      }
      restartPolicy    = local.restart_policy
      logConfiguration = lookup(local.log_configuration, "postgres_backup_upload", null)
    }),
  ]
}

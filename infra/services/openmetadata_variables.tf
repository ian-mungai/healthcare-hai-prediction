variable "openmetadata_persistent_enabled" {
  description = "Run persistent OpenMetadata and its single-node OpenSearch as one host task, plus the one-shot bootstrap task that sets the administrator password and issues one bot token per caller. It replaces Marquez once the retirement comparison passes. Their images are pushed first (config/metadata/images.json) and their digests recorded; this stays false until then. False plans no OpenMetadata resource and leaves every existing limit as it was."
  type        = bool
  default     = false
  nullable    = false
}

variable "openmetadata_task_role_arn" {
  description = "Foundation output: the task role of the bootstrap task. It may write only the bot token parameters named by openmetadata_bot_token_names. Required when openmetadata_persistent_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = !var.openmetadata_persistent_enabled || (var.openmetadata_task_role_arn != null && can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.openmetadata_task_role_arn, "none"))))
    error_message = "Supply an IAM role ARN for the OpenMetadata bootstrap, or set openmetadata_persistent_enabled to false."
  }

  validation {
    condition = !var.openmetadata_persistent_enabled || (
      var.openmetadata_task_role_arn != var.control_plane_task_role_arn && var.openmetadata_task_role_arn != var.polaris_task_role_arn
      && var.openmetadata_task_role_arn != var.mlflow_task_role_arn && var.openmetadata_task_role_arn != var.broker_task_role_arn
    )
    error_message = "The OpenMetadata bootstrap must have its own task role, so the control plane, Polaris, MLflow and the broker hold no permission to write the bot tokens."
  }
}

variable "openmetadata_admin_password_name" {
  description = "Parameter Store SecureString name of the OpenMetadata administrator password. The bootstrap task reads it through the execution role and replaces the factory password with it. The owner sets the value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_admin_password"

  validation {
    condition     = length(var.openmetadata_admin_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.openmetadata_admin_password_name)) && !can(regex("^/(?i:aws|ssm)", var.openmetadata_admin_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "openmetadata_db_password_name" {
  description = "Parameter Store SecureString name of the password of the openmetadata_user database role. The database container sets it on the role, and the migration and the server read it. The owner sets the value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_db_password"

  validation {
    condition     = length(var.openmetadata_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.openmetadata_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.openmetadata_db_password_name)) && var.openmetadata_db_password_name != var.openmetadata_admin_password_name
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores, other than the administrator password. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "openmetadata_bot_token_names" {
  description = "Parameter Store SecureString name of the bot token of each caller: lineage (the host delivery), ingestion (the Fargate Polaris and dbt ingestion jobs) and comparison (the Marquez retirement comparison). The bootstrap task issues each token and writes it straight to its parameter; Terraform never sees a value. A caller reads only its own parameter."
  type        = map(string)
  default = {
    lineage    = "/healthcare_hai_prediction/openmetadata_bot_lineage"
    ingestion  = "/healthcare_hai_prediction/openmetadata_bot_ingestion"
    comparison = "/healthcare_hai_prediction/openmetadata_bot_comparison"
  }
  nullable = false

  validation {
    condition = (
      toset(keys(var.openmetadata_bot_token_names)) == toset(["lineage", "ingestion", "comparison"])
      && alltrue([for name in values(var.openmetadata_bot_token_names) : length(name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", name)) && !can(regex("^/(?i:aws|ssm)", name))])
      && length(distinct(values(var.openmetadata_bot_token_names))) == 3
      && !contains(values(var.openmetadata_bot_token_names), var.openmetadata_admin_password_name)
      && !contains(values(var.openmetadata_bot_token_names), var.openmetadata_db_password_name)
    )
    error_message = "Name exactly the callers lineage, ingestion and comparison, each with its own parameter path of letters, digits and underscores (no dashes, wildcards, spaces or the prefixes aws and ssm), none equal to another or to the administrator or database password."
  }
}

variable "openmetadata_memory_mib" {
  description = "Hard limit and reservation in MiB of each OpenMetadata container, and the fixed JVM heap in MiB of the server and of OpenSearch. These are estimates for a catalog of a few hundred tables, not measurements: replace them after the peak measurement under load (ECS reports task memory, docker stats shows each container). A limit is at least its reservation, the OpenSearch heap is at most half its limit and the server heap at most 60 percent of its limit."
  type = object({
    server    = object({ limit = number, reservation = number, heap_mib = number })
    search    = object({ limit = number, reservation = number, heap_mib = number })
    migrate   = object({ limit = number, reservation = number })
    bootstrap = object({ limit = number, reservation = number })
  })
  default = {
    server    = { limit = 1792, reservation = 1280, heap_mib = 1024 }
    search    = { limit = 1792, reservation = 1280, heap_mib = 896 }
    migrate   = { limit = 768, reservation = 256 }
    bootstrap = { limit = 512, reservation = 256 }
  }
  nullable = false

  validation {
    condition = (
      var.openmetadata_memory_mib.server.reservation >= 1 && var.openmetadata_memory_mib.server.reservation <= var.openmetadata_memory_mib.server.limit
      && var.openmetadata_memory_mib.search.reservation >= 1 && var.openmetadata_memory_mib.search.reservation <= var.openmetadata_memory_mib.search.limit
      && var.openmetadata_memory_mib.migrate.reservation >= 1 && var.openmetadata_memory_mib.migrate.reservation <= var.openmetadata_memory_mib.migrate.limit
      && var.openmetadata_memory_mib.bootstrap.reservation >= 1 && var.openmetadata_memory_mib.bootstrap.reservation <= var.openmetadata_memory_mib.bootstrap.limit
      && var.openmetadata_memory_mib.bootstrap.limit >= 128 && var.openmetadata_memory_mib.migrate.limit >= 512
      && var.openmetadata_memory_mib.server.heap_mib >= 512 && var.openmetadata_memory_mib.server.heap_mib * 100 <= var.openmetadata_memory_mib.server.limit * 60
      && var.openmetadata_memory_mib.search.heap_mib >= 512 && var.openmetadata_memory_mib.search.heap_mib * 2 <= var.openmetadata_memory_mib.search.limit
    )
    error_message = "Each reservation needs 1 MiB or more and at most its limit. The server and OpenSearch heaps need 512 MiB or more. The OpenSearch heap is at most half its limit and the server heap at most 60 percent of its limit. The migration needs 512 MiB and the bootstrap 128 MiB or more."
  }
}

variable "openmetadata_commands" {
  description = "Start commands of the upstream images, unproven until the owner reads each image's entry point. The server runs its start script, the migration runs once, OpenSearch runs its entry point."
  type = object({
    server  = list(string)
    migrate = list(string)
    search  = list(string)
  })
  default = {
    server  = ["./openmetadata-start.sh"]
    migrate = ["./bootstrap/openmetadata-ops.sh", "migrate"]
    search  = ["./opensearch-docker-entrypoint.sh", "opensearch"]
  }
  nullable = false
}

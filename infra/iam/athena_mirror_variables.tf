variable "athena_mirror_prepared" {
  description = "Create the silver, gold and features Glue databases, the limited Athena workgroup and the mirror-only grants (with openmetadata_prepared, also the catalog task's read of them). Whether publishes register the tables is set by enabled in config/lakehouse/athena_mirror.json (on since Nov 13 2026)."
  type        = bool
  default     = false
  nullable    = false
}

variable "athena_owner_user" {
  description = "Name of the one IAM user that the Athena reader policy attaches to. Empty attaches it to nobody. Set it in ignored local input, not in the repository."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = var.athena_owner_user == "" || can(regex("^[A-Za-z0-9_+=,.@-]{1,64}$", var.athena_owner_user))
    error_message = "Use one IAM user name of at most 64 characters, without a path or a wildcard."
  }
}

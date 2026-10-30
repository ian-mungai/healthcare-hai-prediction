variable "athena_mirror_prepared" {
  description = "Create the Glue databases, the limited Athena workgroup and the mirror-only grants for the tables that config/lakehouse/athena_mirror.json lists. The mirror itself stays off in that file until the owner turns it on."
  type        = bool
  default     = false
  nullable    = false
}

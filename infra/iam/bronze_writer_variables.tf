variable "bronze_writer_prepared" {
  description = "Prepare the separate bronze role after the owner approves the reviewed IAM batch. This never activates the layer."
  type        = bool
  default     = false
  nullable    = false
}

variable "bronze_live_locations" {
  description = "Live reader evidence for the bronze and dictionary namespace locations. Leave empty until both complete location inventories are verified."
  type        = map(string)
  default     = {}
  nullable    = false
}

variable "bronze_qualification_collection" {
  description = "The one existing source collection selected for qualification after the coordinating agent verifies which mapped collection is smallest."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = var.bronze_qualification_collection == "" || can(regex("^[a-z0-9_]+/[a-z0-9_]+$", var.bronze_qualification_collection))
    error_message = "Use a mapped publisher/collection name or leave the unprepared candidate empty."
  }
}

variable "source_raw_root_enabled" {
  description = "Let the bronze writer and qualification roles read each mapped source collection under raw/ as well as at the bucket root. Existing root objects stay readable. Off until the owner approves the raw root route."
  type        = bool
  default     = false
  nullable    = false
}

variable "pseudonym_key_prefix" {
  description = "Parameter Store name prefix of the SecureString pseudonym keys. The key version of config/lakehouse/person_policies.json is appended. Must equal PARAMETER_PREFIX in scripts/pseudonym_core.py."
  type        = string
  default     = "/healthcare_hai_prediction/pseudonym_key_"
  nullable    = false
}

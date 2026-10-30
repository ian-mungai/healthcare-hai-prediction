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

variable "job_endpoints_enabled" {
  description = "Create the optional private job endpoints after their reviewed enable plan is approved."
  type        = bool
  default     = false
  nullable    = false
}

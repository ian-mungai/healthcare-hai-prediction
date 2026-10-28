variable "job_writer_e2e_prepared" {
  description = "Prepare the qualification role and reader and execution extensions after the reviewed batch is approved. This never activates catalog writes."
  type        = bool
  default     = false
  nullable    = false
}

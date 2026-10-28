variable "job_writer_e2e_prepared" {
  description = "Prepare the qualification role and reader and execution extensions after the reviewed batch is approved. This never activates catalog writes."
  type        = bool
  default     = false
  nullable    = false
}

variable "aws_polaris_e2e_client_id_name" {
  description = "Name of the dedicated AWS qualification client ID parameter. The owner issues its value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_e2e_client_id"
  nullable    = false

  validation {
    condition = (
      length(var.aws_polaris_e2e_client_id_name) <= 512 &&
      can(regex("^/(?:[A-Za-z0-9_]+/){0,14}aws_polaris_e2e_client_id$", var.aws_polaris_e2e_client_id_name)) &&
      !can(regex("^/(?i:aws|ssm)", var.aws_polaris_e2e_client_id_name))
    )
    error_message = "Use the dedicated aws_polaris_e2e_client_id parameter path with at most 15 underscore-compatible parts and 512 characters."
  }
}

variable "aws_polaris_e2e_client_secret_name" {
  description = "Name of the dedicated AWS qualification client secret parameter. No secret value enters Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_e2e_client_secret"
  nullable    = false

  validation {
    condition = (
      length(var.aws_polaris_e2e_client_secret_name) <= 512 &&
      can(regex("^/(?:[A-Za-z0-9_]+/){0,14}aws_polaris_e2e_client_secret$", var.aws_polaris_e2e_client_secret_name)) &&
      !can(regex("^/(?i:aws|ssm)", var.aws_polaris_e2e_client_secret_name))
    )
    error_message = "Use the dedicated aws_polaris_e2e_client_secret parameter path with at most 15 underscore-compatible parts and 512 characters."
  }
}

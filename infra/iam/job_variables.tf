variable "job_endpoints_enabled" {
  description = "Create the optional private job endpoints after their reviewed enable plan is approved."
  type        = bool
  default     = false
  nullable    = false
}

variable "aws_polaris_reader_client_id_name" {
  description = "Name of the AWS catalog reader client ID parameter. No parameter value enters Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_reader_client_id"
  nullable    = false

  validation {
    condition     = can(regex("^/[A-Za-z0-9_]+(/[A-Za-z0-9_]+)+$", var.aws_polaris_reader_client_id_name))
    error_message = "Use a slash-delimited parameter name containing only letters, digits and underscores."
  }
}

variable "aws_polaris_reader_client_secret_name" {
  description = "Name of the AWS catalog reader client secret parameter. No parameter value enters Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_reader_client_secret"
  nullable    = false

  validation {
    condition     = can(regex("^/[A-Za-z0-9_]+(/[A-Za-z0-9_]+)+$", var.aws_polaris_reader_client_secret_name))
    error_message = "Use a slash-delimited parameter name containing only letters, digits and underscores."
  }
}

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
  description = "The one existing source collection selected for qualification after the lead verifies which mapped collection is smallest."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = var.bronze_qualification_collection == "" || can(regex("^[a-z0-9_]+/[a-z0-9_]+$", var.bronze_qualification_collection))
    error_message = "Use a mapped publisher/collection name or leave the unprepared candidate empty."
  }
}

variable "aws_polaris_bronze_client_id_name" {
  description = "Name of the bronze writer client ID parameter. The owner issues its value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_bronze_client_id"
  nullable    = false

  validation {
    condition     = can(regex("^/[A-Za-z0-9_]+(/[A-Za-z0-9_]+)*/aws_polaris_bronze_client_id$", var.aws_polaris_bronze_client_id_name))
    error_message = "Use the dedicated aws_polaris_bronze_client_id parameter name."
  }
}

variable "aws_polaris_bronze_client_secret_name" {
  description = "Name of the bronze writer client secret parameter. No secret value enters Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_bronze_client_secret"
  nullable    = false

  validation {
    condition     = can(regex("^/[A-Za-z0-9_]+(/[A-Za-z0-9_]+)*/aws_polaris_bronze_client_secret$", var.aws_polaris_bronze_client_secret_name))
    error_message = "Use the dedicated aws_polaris_bronze_client_secret parameter name."
  }
}

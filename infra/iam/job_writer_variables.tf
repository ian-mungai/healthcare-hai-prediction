variable "job_writer_e2e_prepared" {
  description = "Prepare the qualification role and reader and execution extensions after the reviewed batch is approved. This never activates catalog writes."
  type        = bool
  default     = false
  nullable    = false
}

variable "job_writer_layers_prepared" {
  description = "Prepare the silver, gold and features roles and their execution grants after the reviewed IAM preparation is approved. Catalog mutations and activation remain separate."
  type        = bool
  default     = false
  nullable    = false
}

variable "catalog_cutover_fenced" {
  description = "Strict boolean request for the separately reviewed source fence integration. This preparation batch attaches no fence statement."
  type        = any
  default     = false

  validation {
    condition     = var.catalog_cutover_fenced == true || var.catalog_cutover_fenced == false
    error_message = "catalog_cutover_fenced must be a boolean true or false, without quotes."
  }
}

variable "aws_writers_fenced" {
  description = "Strict boolean request for the separately reviewed reverse fence integration. This preparation batch attaches no fence statement."
  type        = any
  default     = false

  validation {
    condition     = var.aws_writers_fenced == true || var.aws_writers_fenced == false
    error_message = "aws_writers_fenced must be a boolean true or false, without quotes."
  }
}

variable "aws_polaris_features_client_id_name" {
  description = "Name of the dedicated AWS features writer client id parameter. The owner issues its value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_features_client_id"
  nullable    = false

  validation {
    condition = (
      length(var.aws_polaris_features_client_id_name) <= 512 &&
      can(regex("^/(?:[A-Za-z0-9_]+/){0,14}aws_polaris_features_client_id$", var.aws_polaris_features_client_id_name)) &&
      !can(regex("^/(?i:aws|ssm)", var.aws_polaris_features_client_id_name))
    )
    error_message = "Use the dedicated aws_polaris_features_client_id parameter path with at most 15 underscore-compatible parts and 512 characters."
  }
}

variable "aws_polaris_features_client_secret_name" {
  description = "Name of the dedicated AWS features writer client secret parameter. The owner issues its value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_features_client_secret"
  nullable    = false

  validation {
    condition = (
      length(var.aws_polaris_features_client_secret_name) <= 512 &&
      can(regex("^/(?:[A-Za-z0-9_]+/){0,14}aws_polaris_features_client_secret$", var.aws_polaris_features_client_secret_name)) &&
      !can(regex("^/(?i:aws|ssm)", var.aws_polaris_features_client_secret_name))
    )
    error_message = "Use the dedicated aws_polaris_features_client_secret parameter path with at most 15 underscore-compatible parts and 512 characters."
  }
}

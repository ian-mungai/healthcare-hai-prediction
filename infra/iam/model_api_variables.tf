variable "model_api_prepared" {
  description = "Prepare the task role of the private model API. It reads the Feast registry and the deployment records and nothing else. This never starts the service."
  type        = bool
  default     = false
  nullable    = false
}

variable "model_api_prediction_writes" {
  description = "Let the model API task role write the Iceberg objects of the prediction table, under the serving prefix of the lakehouse and nowhere else. The role cannot append predictions without it. Requires model_api_prepared."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.model_api_prediction_writes || var.model_api_prepared
    error_message = "Prediction writes belong to the model API role, so set model_api_prepared to true first."
  }
}

variable "model_api_registry_key" {
  description = "Object key of the Feast registry file in the data bucket, for example lakehouse/feast/registry.pb. Required when model_api_prepared is true."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = !var.model_api_prepared || (can(regex("^[A-Za-z0-9_][A-Za-z0-9_./-]{0,500}$", var.model_api_registry_key)) && !startswith(var.model_api_registry_key, "lakehouse/serving"))
    error_message = "Set model_api_registry_key to one object key without wildcards or a leading slash, outside the prediction table prefix."
  }
}

variable "model_api_manifest_prefix" {
  description = "Key prefix of the deployment records in the data bucket, without a trailing slash, for example serving/deployments. Required when model_api_prepared is true."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = !var.model_api_prepared || (can(regex("^[A-Za-z0-9_][A-Za-z0-9_./-]{0,200}[A-Za-z0-9_]$", var.model_api_manifest_prefix)) && !startswith(var.model_api_manifest_prefix, "lakehouse/serving"))
    error_message = "Set model_api_manifest_prefix to one key prefix without wildcards, a leading slash or a trailing slash, outside the prediction table prefix."
  }
}

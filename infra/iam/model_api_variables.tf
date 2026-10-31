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

variable "model_api_key_hashes_name" {
  description = "Parameter Store SecureString name of the JSON object that maps each model API key identifier to the SHA-256 hash of its key. The project may read, create and put it, and the task execution role injects it into the model API while model_api_prepared is true."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_key_hashes"

  validation {
    condition     = length(var.model_api_key_hashes_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_key_hashes_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_key_hashes_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "model_api_mlflow_user_name" {
  description = "Parameter Store SecureString name of the MLflow user the model API loads models with. The project may read, create and put it, and the task execution role injects it into the model API while model_api_prepared is true."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_mlflow_user"

  validation {
    condition     = length(var.model_api_mlflow_user_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_mlflow_user_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_mlflow_user_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "model_api_mlflow_credential_name" {
  description = "Parameter Store SecureString name of the credential of the MLflow user the model API loads models with. The project may read, create and put it, and the task execution role injects it into the model API while model_api_prepared is true."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_mlflow_credential"

  validation {
    condition     = length(var.model_api_mlflow_credential_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_mlflow_credential_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_mlflow_credential_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "streamlit_model_api_key_name" {
  description = "Parameter Store SecureString name of the API key the page presents to the model API. The project may read, create and put it, and the task execution role injects it into the page while model_api_prepared is true."
  type        = string
  default     = "/healthcare_hai_prediction/streamlit_model_api_key"

  validation {
    condition     = length(var.streamlit_model_api_key_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.streamlit_model_api_key_name)) && !can(regex("^/(?i:aws|ssm)", var.streamlit_model_api_key_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

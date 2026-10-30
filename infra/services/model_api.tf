variable "model_api_enabled" {
  description = "Run the private model API service on the host. It binds the host loopback only, so Session Manager port forwarding is the one way in. A public load balancer is a later, separate decision."
  type        = bool
  default     = false
}

variable "model_api_task_role_arn" {
  description = "Task role of the model API. It may read the model artifacts, the Feast registry and the deployment records, and nothing else. Required when model_api_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.model_api_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.model_api_task_role_arn, "x")))
    error_message = "Supply an IAM role ARN."
  }

  validation {
    condition     = !var.model_api_enabled || var.model_api_task_role_arn != null
    error_message = "Set model_api_task_role_arn when model_api_enabled is true."
  }

  validation {
    condition     = var.model_api_task_role_arn == null || (var.model_api_task_role_arn != var.polaris_task_role_arn && var.model_api_task_role_arn != var.control_plane_task_role_arn)
    error_message = "The model API needs its own task role, so it holds neither the lakehouse permission of Polaris nor the control-plane role."
  }
}

variable "model_api_hai_types" {
  description = "Comma separated HAI types the API serves. Each type keeps its stable model and at most one canary model in memory."
  type        = string
  default     = "HAI_1"

  validation {
    condition     = can(regex("^HAI_[1-6](,HAI_[1-6]){0,5}$", var.model_api_hai_types))
    error_message = "List distinct HAI types from HAI_1 to HAI_6 separated by commas."
  }
}

variable "model_api_manifest_uri" {
  description = "S3 URI prefix of the deployment records that serve_deploy writes and the API reads (active and history folders)."
  type        = string
  default     = ""

  validation {
    condition     = !var.model_api_enabled || can(regex("^s3://[a-z0-9][a-z0-9.-]{2,62}/[A-Za-z0-9_./-]+$", var.model_api_manifest_uri))
    error_message = "Set model_api_manifest_uri to an s3:// URI with a prefix when model_api_enabled is true."
  }
}

variable "model_api_registry_uri" {
  description = "S3 URI of the Feast registry file the API reads."
  type        = string
  default     = ""

  validation {
    condition     = !var.model_api_enabled || can(regex("^s3://[a-z0-9][a-z0-9.-]{2,62}/[A-Za-z0-9_./-]+$", var.model_api_registry_uri))
    error_message = "Set model_api_registry_uri to an s3:// URI when model_api_enabled is true."
  }
}

variable "model_api_mlflow_uri" {
  description = "Address of the MLflow tracking server on the host, for example http://127.0.0.1:5100."
  type        = string
  default     = ""

  validation {
    condition     = !var.model_api_enabled || can(regex("^http://127\\.0\\.0\\.1:[0-9]{4,5}$", var.model_api_mlflow_uri))
    error_message = "Set model_api_mlflow_uri to the loopback address of the MLflow server when model_api_enabled is true."
  }
}

variable "model_api_rate_per_minute" {
  description = "Requests a minute that each API key may make, after its burst."
  type        = number
  default     = 60

  validation {
    condition     = var.model_api_rate_per_minute >= 1 && var.model_api_rate_per_minute <= 6000
    error_message = "Use 1 to 6000."
  }
}

variable "model_api_burst" {
  description = "Requests that each API key may make at once."
  type        = number
  default     = 20

  validation {
    condition     = var.model_api_burst >= 1 && var.model_api_burst <= 1000
    error_message = "Use 1 to 1000."
  }
}

variable "model_api_key_hashes_name" {
  description = "Parameter Store SecureString name of the JSON object that maps each API key identifier to the SHA-256 hash of its key. The keys themselves are never stored."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_key_hashes"

  validation {
    condition     = length(var.model_api_key_hashes_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_key_hashes_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_key_hashes_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "model_api_writer_client_id_name" {
  description = "Parameter Store SecureString name of the client id of the Polaris principal that may append to the serving namespace."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_writer_client_id"

  validation {
    condition     = length(var.model_api_writer_client_id_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_writer_client_id_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_writer_client_id_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "model_api_writer_client_credential_name" {
  description = "Parameter Store SecureString name of the client credential of the Polaris principal that may append to the serving namespace."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_writer_client_credential"

  validation {
    condition     = length(var.model_api_writer_client_credential_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_writer_client_credential_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_writer_client_credential_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "model_api_mlflow_user_name" {
  description = "Parameter Store SecureString name of the MLflow login the API uses to read model versions."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_mlflow_user"

  validation {
    condition     = length(var.model_api_mlflow_user_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_mlflow_user_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_mlflow_user_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "model_api_mlflow_credential_name" {
  description = "Parameter Store SecureString name of the MLflow login credential the API uses to read model versions."
  type        = string
  default     = "/healthcare_hai_prediction/model_api_mlflow_credential"

  validation {
    condition     = length(var.model_api_mlflow_credential_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.model_api_mlflow_credential_name)) && !can(regex("^/(?i:aws|ssm)", var.model_api_mlflow_credential_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

locals {
  model_api_port = 8000

  model_api_secret_names = var.model_api_enabled ? {
    model_api_key_hashes        = var.model_api_key_hashes_name
    model_api_writer_id         = var.model_api_writer_client_id_name
    model_api_writer_credential = var.model_api_writer_client_credential_name
    model_api_mlflow_user       = var.model_api_mlflow_user_name
    model_api_mlflow_credential = var.model_api_mlflow_credential_name
  } : {}

  model_api_environment = {
    AWS_REGION                      = var.aws_region
    DO_NOT_TRACK                    = "true"
    FEAST_REDIS_HOST                = "127.0.0.1"
    FEAST_REGISTRY_PATH             = var.model_api_registry_uri
    HOME                            = "/tmp"
    MLFLOW_DISABLE_TELEMETRY        = "true"
    MLFLOW_HTTP_REQUEST_MAX_RETRIES = "0"
    MLFLOW_HTTP_REQUEST_TIMEOUT     = "20"
    MLFLOW_TRACKING_URI             = var.model_api_mlflow_uri
    MODEL_API_BIND                  = "127.0.0.1"
    MODEL_API_BURST                 = tostring(var.model_api_burst)
    MODEL_API_HAI_TYPES             = var.model_api_hai_types
    MODEL_API_MANIFEST_URI          = var.model_api_manifest_uri
    MODEL_API_PORT                  = tostring(local.model_api_port)
    MODEL_API_RATE_PER_MINUTE       = tostring(var.model_api_rate_per_minute)
    POLARIS_URI                     = "http://127.0.0.1:8181/api/catalog"
    PYTHONDONTWRITEBYTECODE         = "1"
    TMPDIR                          = "/tmp"
  }

  model_api_containers = var.model_api_enabled ? [
    merge(local.container_defaults, {
      name                   = "model_api"
      image                  = local.image["model_api"]
      user                   = "10010:10010"
      readonlyRootFilesystem = true
      memory                 = 1280
      memoryReservation      = 640
      stopTimeout            = 30
      environment            = [for name, value in local.model_api_environment : { name = name, value = value }]
      secrets = [
        { name = "MODEL_API_KEY_HASHES", valueFrom = local.secret_arn["model_api_key_hashes"] },
        { name = "POLARIS_CLIENT_ID", valueFrom = local.secret_arn["model_api_writer_id"] },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = local.secret_arn["model_api_writer_credential"] },
        { name = "MLFLOW_TRACKING_USERNAME", valueFrom = local.secret_arn["model_api_mlflow_user"] },
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = local.secret_arn["model_api_mlflow_credential"] },
        { name = "REDIS_PASSWORD", valueFrom = local.secret_arn["redis_password"] },
      ]
      portMappings = [{ containerPort = local.model_api_port, hostPort = local.model_api_port, protocol = "tcp" }]
      linuxParameters = {
        initProcessEnabled = true
        tmpfs              = [{ containerPath = "/tmp", size = 256, mountOptions = ["noexec", "nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request as u; u.urlopen('http://127.0.0.1:${local.model_api_port}/v1/health', timeout=3)"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 180
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["model_api"]
    }),
  ] : []
}

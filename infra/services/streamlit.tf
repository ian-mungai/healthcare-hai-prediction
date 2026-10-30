variable "streamlit_enabled" {
  description = "Run the Streamlit page on the host. It binds the host loopback only, so Session Manager port forwarding is the one way in, and it reaches the model API over the host loopback. Public access is a later, separate decision."
  type        = bool
  default     = false

  validation {
    condition     = !var.streamlit_enabled || var.model_api_enabled
    error_message = "The Streamlit page is a client of the model API. Set model_api_enabled to true, or streamlit_enabled to false."
  }
}

variable "streamlit_task_role_arn" {
  description = "Task role of the Streamlit page. The page calls no AWS service, so the role grants nothing. Required when streamlit_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.streamlit_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.streamlit_task_role_arn, "x")))
    error_message = "Supply an IAM role ARN."
  }

  validation {
    condition     = !var.streamlit_enabled || var.streamlit_task_role_arn != null
    error_message = "Set streamlit_task_role_arn when streamlit_enabled is true."
  }

  validation {
    condition     = var.streamlit_task_role_arn == null || (var.streamlit_task_role_arn != var.polaris_task_role_arn && var.streamlit_task_role_arn != var.control_plane_task_role_arn && var.streamlit_task_role_arn != var.model_api_task_role_arn)
    error_message = "The Streamlit page needs its own task role, so it holds none of the permissions of Polaris, the control plane or the model API."
  }
}

variable "streamlit_hai_types" {
  description = "Comma separated HAI types the page offers. Each must be one the model API serves."
  type        = string
  default     = "HAI_1"

  validation {
    condition     = can(regex("^HAI_[1-6](,HAI_[1-6]){0,5}$", var.streamlit_hai_types))
    error_message = "List distinct HAI types from HAI_1 to HAI_6 separated by commas."
  }

  validation {
    condition     = alltrue([for type in split(",", var.streamlit_hai_types) : contains(split(",", var.model_api_hai_types), type)])
    error_message = "The page may offer only HAI types that model_api_hai_types serves."
  }
}

variable "streamlit_api_key_name" {
  description = "Parameter Store SecureString name of the API key the page presents to the model API. It is a key of its own: its SHA-256 hash is an entry in the model_api_key_hashes parameter under its own identifier, so the API rate limit and the prediction rows name it apart from other callers."
  type        = string
  default     = "/healthcare_hai_prediction/streamlit_model_api_key"

  validation {
    condition     = length(var.streamlit_api_key_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.streamlit_api_key_name)) && !can(regex("^/(?i:aws|ssm)", var.streamlit_api_key_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

locals {
  streamlit_port = 8501

  streamlit_secret_names = var.streamlit_enabled ? { streamlit_api_key = var.streamlit_api_key_name } : {}

  streamlit_environment = {
    HOME                                 = "/tmp"
    MODEL_API_URL                        = "http://127.0.0.1:${local.model_api_port}"
    PYTHONDONTWRITEBYTECODE              = "1"
    STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"
    STREAMLIT_HAI_TYPES                  = var.streamlit_hai_types
    STREAMLIT_SERVER_ADDRESS             = "127.0.0.1"
    STREAMLIT_SERVER_PORT                = tostring(local.streamlit_port)
    STREAMLIT_SERVER_FILE_WATCHER_TYPE   = "none"
    TMPDIR                               = "/tmp"
  }

  # The memory limits are assumptions, not measurements: an idle Streamlit server with its data libraries is expected well under the reservation. Measure the
  # resident memory with a page open, then adjust.
  streamlit_containers = var.streamlit_enabled ? [
    merge(local.container_defaults, {
      name                   = "streamlit"
      image                  = local.image["streamlit"]
      user                   = "10020:10020"
      readonlyRootFilesystem = true
      memory                 = 512
      memoryReservation      = 256
      stopTimeout            = 30
      environment            = [for name, value in local.streamlit_environment : { name = name, value = value }]
      secrets                = [{ name = "MODEL_API_KEY", valueFrom = local.secret_arn["streamlit_api_key"] }]
      portMappings           = [{ containerPort = local.streamlit_port, hostPort = local.streamlit_port, protocol = "tcp" }]
      linuxParameters = {
        initProcessEnabled = true
        tmpfs              = [{ containerPath = "/tmp", size = 64, mountOptions = ["noexec", "nosuid"] }]
      }
      healthCheck = {
        command     = ["CMD", "python", "-c", "import urllib.request as u; u.urlopen('http://127.0.0.1:${local.streamlit_port}/_stcore/health', timeout=3)"]
        interval    = 30
        timeout     = 5
        retries     = 5
        startPeriod = 60
      }
      restartPolicy    = local.restart_policy
      logConfiguration = local.log_configuration["streamlit"]
    }),
  ] : []
}

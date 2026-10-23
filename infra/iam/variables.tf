variable "admin_profile" {
  description = "Explicit IAM permission-update operator; never stored as the project profile."
  type        = string
  nullable    = false

  validation {
    condition     = length(trimspace(var.admin_profile)) > 0 && var.admin_profile != var.aws_profile
    error_message = "Supply the approved administrator profile explicitly; the project user must not manage its own permissions."
  }
}

variable "aws_region" {
  description = "Approved region for project storage."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Supply an AWS region code such as eu-west-1, without surrounding whitespace."
  }
}

variable "expected_account_id" {
  description = "Verified account in which the existing project IAM user resides."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply the verified 12-digit AWS account ID."
  }
}

variable "data_bucket_name" {
  description = "The exact bucket the policies may manage."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.data_bucket_name))
    error_message = "Use a 3-63 character bucket name containing lowercase letters, digits and hyphens."
  }
}

variable "project_name" {
  description = "Underscore project identifier from .env; it prefixes every resource name and is the value of the Project tag."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_name))
    error_message = "Use a lowercase project name containing letters, digits and underscores."
  }
}

variable "aws_profile" {
  description = "Project profile and same-named existing IAM user from AWS_PROFILE; no separate deployment identity."
  type        = string
  nullable    = false

  validation {
    condition     = var.aws_profile == "${var.project_name}_${var.environment}" && length(var.aws_profile) <= 64
    error_message = "AWS_PROFILE must be PROJECT_NAME, an underscore and ENVIRONMENT, and a valid IAM username."
  }
}

variable "environment" {
  description = "Deployment environment; also the suffix of the project profile and its IAM user."
  type        = string
  nullable    = false

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "ENVIRONMENT must be dev or prod."
  }
}

variable "census_secret_name" {
  description = "Secrets Manager name of the Census API key; the project may read it."
  type        = string
  default     = "census_api_key"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.census_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "bls_secret_name" {
  description = "Secrets Manager name of the BLS API key; the project may read it."
  type        = string
  default     = "bls_api_key"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.bls_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "hud_secret_name" {
  description = "Secrets Manager name of the HUD USPS crosswalk token; the project may read it."
  type        = string
  default     = "hud_api_key"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.hud_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "jev_secret_name" {
  description = "Secrets Manager name of the TypeSafe Jev API key; the project may read it."
  type        = string
  default     = "jev_api_key"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.jev_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "airflow_admin_secret_name" {
  description = "Parameter Store SecureString name of the Airflow administrator password; the project may read it."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_admin"

  validation {
    condition     = length(var.airflow_admin_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_admin_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_admin_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "postgres_password_name" {
  description = "Parameter Store SecureString name of the local catalog database password; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/postgres_password"

  validation {
    condition     = length(var.postgres_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.postgres_password_name)) && !can(regex("^/(?i:aws|ssm)", var.postgres_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_root_secret_name" {
  description = "Parameter Store SecureString name of the catalog root client secret; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_root_secret"

  validation {
    condition     = length(var.polaris_root_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_root_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_root_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_reader_client_id_name" {
  description = "Parameter Store SecureString name of the read-only catalog principal's client ID; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_reader_client_id"

  validation {
    condition     = length(var.polaris_reader_client_id_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_reader_client_id_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_reader_client_id_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_reader_client_secret_name" {
  description = "Parameter Store SecureString name of the read-only catalog principal's client secret; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_reader_client_secret"

  validation {
    condition     = length(var.polaris_reader_client_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_reader_client_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_reader_client_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_gold_client_id_name" {
  description = "Parameter Store SecureString name of the gold writer principal's client ID; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_gold_client_id"

  validation {
    condition     = length(var.polaris_gold_client_id_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_gold_client_id_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_gold_client_id_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_gold_client_secret_name" {
  description = "Parameter Store SecureString name of the gold writer principal's client secret; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_gold_client_secret"

  validation {
    condition     = length(var.polaris_gold_client_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_gold_client_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_gold_client_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_silver_client_id_name" {
  description = "Parameter Store SecureString name of the silver writer principal's client ID; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_silver_client_id"

  validation {
    condition     = length(var.polaris_silver_client_id_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_silver_client_id_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_silver_client_id_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_silver_client_secret_name" {
  description = "Parameter Store SecureString name of the silver writer principal's client secret; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_silver_client_secret"

  validation {
    condition     = length(var.polaris_silver_client_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_silver_client_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_silver_client_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_db_password_name" {
  description = "Parameter Store SecureString name of the Airflow metadata database password; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_db_password"

  validation {
    condition     = length(var.airflow_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_db_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_fernet_key_name" {
  description = "Parameter Store SecureString name of the Airflow Fernet key; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_fernet_key"

  validation {
    condition     = length(var.airflow_fernet_key_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_fernet_key_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_fernet_key_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_jwt_secret_name" {
  description = "Parameter Store SecureString name of the Airflow JSON Web Token secret; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_jwt_secret"

  validation {
    condition     = length(var.airflow_jwt_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_jwt_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_jwt_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_api_secret_key_name" {
  description = "Parameter Store SecureString name of the Airflow API server secret key; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_api_secret_key"

  validation {
    condition     = length(var.airflow_api_secret_key_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_api_secret_key_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_api_secret_key_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "marquez_db_password_name" {
  description = "Parameter Store SecureString name of the lineage database password; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/marquez_db_password"

  validation {
    condition     = length(var.marquez_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.marquez_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.marquez_db_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "redis_password_name" {
  description = "Parameter Store SecureString name of the Redis password; the project may read, create and put it."
  type        = string
  default     = "/healthcare_hai_prediction/redis_password"

  validation {
    condition     = length(var.redis_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.redis_password_name)) && !can(regex("^/(?i:aws|ssm)", var.redis_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "lakehouse_reader_role_name" {
  description = "IAM role name of the lakehouse reader; only the project user may assume it, and the launcher asks for a short session of it for a read job."
  type        = string
  default     = "healthcare_hai_prediction_lakehouse_reader"

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.lakehouse_reader_role_name))
    error_message = "Use 1-64 characters from letters, digits and +=,.@_-."
  }
}

variable "lakehouse_writer_role_name" {
  description = "IAM role name of the lakehouse writer; only the project user may assume it, and the launcher asks for a short session of it for a write job."
  type        = string
  default     = "healthcare_hai_prediction_lakehouse_writer"

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.lakehouse_writer_role_name))
    error_message = "Use 1-64 characters from letters, digits and +=,.@_-."
  }
}

variable "lakehouse_session_max_seconds" {
  description = "Longest session either lakehouse role can give, in seconds. It must exceed the longest pipeline job deadline (3600 seconds) plus the launcher's margin."
  type        = number
  default     = 7200

  validation {
    condition     = var.lakehouse_session_max_seconds >= 3600 && var.lakehouse_session_max_seconds <= 43200 && var.lakehouse_session_max_seconds == floor(var.lakehouse_session_max_seconds)
    error_message = "Use a whole number of seconds from 3600 to 43200, the range IAM accepts."
  }
}

variable "owner_ip_cidrs" {
  description = "The owner's public IPv4 addresses, each as a /32; the only sources the host accepts. Empty keeps the host closed."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.owner_ip_cidrs : can(cidrhost(cidr, 0)) && endswith(cidr, "/32") && !strcontains(cidr, ":")])
    error_message = "Each owner address must be a single IPv4 address written as a /32."
  }
}

variable "host_ingress_ports" {
  description = "TCP ports the host opens to the owner addresses."
  type        = list(number)
  default     = []

  validation {
    condition     = alltrue([for port in var.host_ingress_ports : port >= 1 && port <= 65535 && floor(port) == port])
    error_message = "Each port must be a whole number from 1 to 65535."
  }
}

variable "host_task_ports" {
  description = "TCP ports on the host that the run tasks reach: the Polaris catalog and Redis."
  type        = list(number)
  default     = [6379, 8181]

  validation {
    condition     = alltrue([for port in var.host_task_ports : port >= 1 && port <= 65535 && floor(port) == port])
    error_message = "Each port must be a whole number from 1 to 65535."
  }
}

variable "budget_limit_usd" {
  description = "Quarterly amount for the resources that carry the Project tag; the owner tracks it and the alerts only inform."
  type        = number
  default     = 150

  validation {
    condition     = var.budget_limit_usd > 0 && var.budget_limit_usd <= 1000
    error_message = "Use a budget from 1 to 1000 US dollars."
  }
}

variable "budget_alert_percentages" {
  description = "Actual-spend percentages of the budget that send an alert to the budget topic."
  type        = set(number)
  default     = [50, 80]

  validation {
    condition     = length(var.budget_alert_percentages) > 0 && alltrue([for value in var.budget_alert_percentages : value > 0 && value <= 100])
    error_message = "Alert percentages must be above 0 and at most 100."
  }
}

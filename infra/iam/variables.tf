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
  description = "Prefix rendered into service policy names from .env."
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
  description = "Secrets Manager name of the Airflow administrator password; the project may read it."
  type        = string
  default     = "healthcare-hai-prediction/airflow-admin"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.airflow_admin_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "postgres_password_name" {
  description = "Secrets Manager name of the local catalog database password; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/postgres-password"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.postgres_password_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_root_secret_name" {
  description = "Secrets Manager name of the catalog root client secret; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-root-secret"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_root_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_reader_client_id_name" {
  description = "Secrets Manager name of the read-only catalog principal's client ID; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-reader-client-id"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_reader_client_id_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_reader_client_secret_name" {
  description = "Secrets Manager name of the read-only catalog principal's client secret; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-reader-client-secret"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_reader_client_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_gold_client_id_name" {
  description = "Secrets Manager name of the gold writer principal's client ID; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-gold-client-id"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_gold_client_id_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_gold_client_secret_name" {
  description = "Secrets Manager name of the gold writer principal's client secret; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-gold-client-secret"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_gold_client_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_silver_client_id_name" {
  description = "Secrets Manager name of the silver writer principal's client ID; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-silver-client-id"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_silver_client_id_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "polaris_silver_client_secret_name" {
  description = "Secrets Manager name of the silver writer principal's client secret; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/polaris-silver-client-secret"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.polaris_silver_client_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "airflow_db_password_name" {
  description = "Secrets Manager name of the Airflow metadata database password; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/airflow-db-password"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.airflow_db_password_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "airflow_fernet_key_name" {
  description = "Secrets Manager name of the Airflow Fernet key; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/airflow-fernet-key"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.airflow_fernet_key_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "airflow_jwt_secret_name" {
  description = "Secrets Manager name of the Airflow JSON Web Token secret; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/airflow-jwt-secret"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.airflow_jwt_secret_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "airflow_api_secret_key_name" {
  description = "Secrets Manager name of the Airflow API server secret key; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/airflow-api-secret-key"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.airflow_api_secret_key_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

variable "marquez_db_password_name" {
  description = "Secrets Manager name of the lineage database password; the project may read, describe, create and put it."
  type        = string
  default     = "healthcare-hai-prediction/marquez-db-password"

  validation {
    condition     = can(regex("^[A-Za-z0-9/_+=.@-]{1,512}$", var.marquez_db_password_name))
    error_message = "Use 1-512 characters from letters, digits and /_+=.@-; wildcards are not allowed."
  }
}

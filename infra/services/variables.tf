variable "aws_profile" {
  description = "Only the project deployment profile may run this stack."
  type        = string
  nullable    = false

  validation {
    condition     = var.aws_profile == "${var.project_name}_${var.environment}" && length(var.aws_profile) <= 64
    error_message = "AWS_PROFILE must be PROJECT_NAME, an underscore and ENVIRONMENT, and a valid IAM username."
  }
}

variable "environment" {
  description = "Deployment environment; also the suffix of the project profile."
  type        = string
  nullable    = false

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "ENVIRONMENT must be dev or prod."
  }
}

variable "project_name" {
  description = "Underscore project identifier supplied from .env."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_name))
    error_message = "Use a lowercase project name containing letters, digits and underscores."
  }
}

variable "project_tag" {
  description = "Foundation output project_tag: the Project tag value (the project name), which the budget filters on together with the services the AWS-first design adds. Every resource of this stack carries it."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,79}$", var.project_tag)) && var.project_tag == var.project_name
    error_message = "Supply the foundation project_tag output: the project name, with underscores, the same value as the data bucket's Project tag."
  }
}

variable "aws_region" {
  description = "AWS region of the foundation, the secrets and the log groups."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Supply an AWS region code such as us-west-2, without surrounding whitespace."
  }
}

variable "expected_account_id" {
  description = "Account verified through the project profile before planning or applying."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply the verified 12-digit AWS account ID."
  }
}

variable "cluster_name" {
  description = "Foundation output: name of the ECS cluster that holds the container host."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_-]{1,255}$", var.cluster_name))
    error_message = "Use 1-255 letters, digits, hyphens and underscores."
  }
}

variable "ec2_capacity_provider_name" {
  description = "Foundation output: name of the capacity provider backed by the container host. Every control-plane service runs only on it."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_-]{1,255}$", var.ec2_capacity_provider_name))
    error_message = "Use 1-255 letters, digits, hyphens and underscores."
  }
}

variable "execution_role_arn" {
  description = "Foundation output: the ECS task execution role. It pulls images from ECR, writes the log streams and reads every parameter named by the secret name variables."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/", var.execution_role_arn))
    error_message = "Supply an IAM role ARN."
  }
}

variable "polaris_task_role_arn" {
  description = "Foundation output: the task role of Polaris. It alone may read and write the lakehouse prefix of the data bucket."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/", var.polaris_task_role_arn))
    error_message = "Supply an IAM role ARN."
  }
}

variable "control_plane_task_role_arn" {
  description = "Foundation output: the task role of Postgres, Redis, Airflow, Marquez and the host preparation task. It holds no data permission."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/", var.control_plane_task_role_arn))
    error_message = "Supply an IAM role ARN."
  }

  validation {
    condition     = var.control_plane_task_role_arn != var.polaris_task_role_arn
    error_message = "Polaris must have its own task role, so Airflow, Postgres, Redis and Marquez hold no lakehouse permission."
  }
}

variable "log_group_names" {
  description = "Foundation output: log group name for each component. The keys postgres, redis, polaris, airflow and marquez are required, and mlflow when mlflow_enabled is true; components may share a group."
  type        = map(string)

  validation {
    condition     = alltrue([for component in ["postgres", "redis", "polaris", "airflow", "marquez"] : contains(keys(var.log_group_names), component)])
    error_message = "Name a log group for postgres, redis, polaris, airflow and marquez."
  }

  validation {
    condition     = !var.mlflow_enabled || contains(keys(var.log_group_names), "mlflow")
    error_message = "Name a log group for mlflow, or set mlflow_enabled to false."
  }
}

variable "ecr_repository_urls" {
  description = "Foundation output: ECR repository URL for each image. The keys postgres, redis, polaris, polaris_admin and airflow are required, and marquez and marquez_web when marquez_enabled is true, and mlflow when mlflow_enabled is true."
  type        = map(string)

  validation {
    condition = alltrue([
      for image in concat(["postgres", "redis", "polaris", "polaris_admin", "airflow"], var.marquez_enabled ? ["marquez", "marquez_web"] : [], var.mlflow_enabled ? ["mlflow"] : []) :
      can(regex("^[0-9]{12}\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com/[a-z0-9._/-]+$", lookup(var.ecr_repository_urls, image, "")))
    ])
    error_message = "Name an ECR repository URL (without a tag or digest) for each image that runs."
  }
}

variable "image_digests" {
  description = "Image digest for each ECR image, written by scripts/infrastructure/push_images.py after the push. Images run by digest, never by tag. The repositories are immutable, so every push uses a unique tag."
  type        = map(string)

  validation {
    condition     = alltrue([for digest in values(var.image_digests) : can(regex("^sha256:[0-9a-f]{64}$", digest))])
    error_message = "Every image reference must be a sha256 digest."
  }

  validation {
    condition     = alltrue([for image in ["postgres", "redis", "polaris", "polaris_admin", "airflow"] : contains(keys(var.image_digests), image)])
    error_message = "Supply a digest for postgres, redis, polaris, polaris_admin and airflow."
  }

  validation {
    condition     = !var.marquez_enabled || alltrue([for image in ["marquez", "marquez_web"] : contains(keys(var.image_digests), image)])
    error_message = "Supply a digest for marquez and marquez_web, or set marquez_enabled to false."
  }

  validation {
    condition     = !var.mlflow_enabled || contains(keys(var.image_digests), "mlflow")
    error_message = "Supply a digest for mlflow, or set mlflow_enabled to false."
  }
}

variable "marquez_enabled" {
  description = "Run the Marquez services. Their arm64 images are built from source (push_images.py plan) and pushed first; this stays false until their digests are recorded."
  type        = bool
  default     = false
}

variable "mlflow_enabled" {
  description = "Run the MLflow tracking service on the host. Training jobs on AWS log runs to it. Its image is built from services/mlflow and pushed first. Set this to false to stop the service: the database and the artifacts stay."
  type        = bool
  default     = false
}

variable "mlflow_task_role_arn" {
  description = "Foundation output: the task role of MLflow. It may read and write only the mlflow/ prefix of the data bucket. Required when mlflow_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = !var.mlflow_enabled || (var.mlflow_task_role_arn != null && can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.mlflow_task_role_arn, "none"))))
    error_message = "Supply an IAM role ARN for MLflow, or set mlflow_enabled to false."
  }

  validation {
    condition     = !var.mlflow_enabled || (var.mlflow_task_role_arn != var.control_plane_task_role_arn && var.mlflow_task_role_arn != var.polaris_task_role_arn)
    error_message = "MLflow must have its own task role, so the control plane and Polaris hold no artifact permission and MLflow holds no lakehouse permission."
  }
}

variable "mlflow_host_ip" {
  description = "Private IPv4 address of the container host. The training jobs call MLflow at this address on port 5100, so MLflow accepts it as a Host header. Required when mlflow_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = !var.mlflow_enabled || can(regex("^(10\\.[0-9]{1,3}|172\\.(1[6-9]|2[0-9]|3[01])|192\\.168)\\.[0-9]{1,3}\\.[0-9]{1,3}$", coalesce(var.mlflow_host_ip, "none")))
    error_message = "Supply the private IPv4 address of the host, or set mlflow_enabled to false."
  }
}

variable "mlflow_artifact_bucket" {
  description = "Name of the data bucket. MLflow stores its artifacts under the mlflow/ prefix. Required when mlflow_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = !var.mlflow_enabled || (var.mlflow_artifact_bucket != null && can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", coalesce(var.mlflow_artifact_bucket, "!"))))
    error_message = "Name the data bucket for MLflow artifacts, or set mlflow_enabled to false."
  }
}

variable "mlflow_db_password_name" {
  description = "Parameter Store SecureString name of the password of the mlflow database role."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_db_password"

  validation {
    condition     = length(var.mlflow_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.mlflow_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.mlflow_db_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "data_mount_path" {
  description = "Foundation output: absolute path where the host mounts the data volume. Every stateful container stores its data below it."
  type        = string

  validation {
    condition     = can(regex("^/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$", var.data_mount_path)) && !strcontains(var.data_mount_path, "..")
    error_message = "Use an absolute path without a trailing slash or '..'."
  }
}

variable "dag_code_path" {
  description = "Absolute host path of the repository checkout that Airflow mounts read-only (dags, orchestration client and lineage configuration). Null means repo below the data mount."
  type        = string
  default     = null

  validation {
    condition     = var.dag_code_path == null || can(regex("^/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$", coalesce(var.dag_code_path, "/x")))
    error_message = "Use an absolute path without a trailing slash."
  }
}

variable "host_memory_mib" {
  description = "Memory of the container host in MiB (r7g.large: 16384). The resize to r7g.xlarge changes this value."
  type        = number
  default     = 16384

  validation {
    condition     = var.host_memory_mib >= 8192
    error_message = "The host needs at least 8192 MiB."
  }
}

variable "other_services_reserve_mib" {
  description = "Memory in MiB that stays free of control-plane reservations: the operating system, the ECS agent and the later services (MLflow, prediction API, Streamlit, Prometheus, Grafana)."
  type        = number
  default     = 5632

  validation {
    condition     = var.other_services_reserve_mib >= 2048
    error_message = "Keep at least 2048 MiB for the operating system and the ECS agent."
  }
}

variable "airflow_parallelism" {
  description = "Most tasks the Airflow scheduler runs at once; each task only calls the job broker."
  type        = number
  default     = 4

  validation {
    condition     = var.airflow_parallelism >= 1 && var.airflow_parallelism <= 16
    error_message = "Use 1 to 16."
  }
}

variable "postgres_password_name" {
  description = "Parameter Store SecureString name of the Postgres superuser and Polaris database password."
  type        = string
  default     = "/healthcare_hai_prediction/postgres_password"

  validation {
    condition     = length(var.postgres_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.postgres_password_name)) && !can(regex("^/(?i:aws|ssm)", var.postgres_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_db_password_name" {
  description = "Parameter Store SecureString name of the password of the airflow database role."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_db_password"

  validation {
    condition     = length(var.airflow_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_db_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_fernet_key_name" {
  description = "Parameter Store SecureString name of the Airflow Fernet key."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_fernet_key"

  validation {
    condition     = length(var.airflow_fernet_key_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_fernet_key_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_fernet_key_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_jwt_secret_name" {
  description = "Parameter Store SecureString name of the Airflow JWT signing secret."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_jwt_secret"

  validation {
    condition     = length(var.airflow_jwt_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_jwt_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_jwt_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "airflow_api_secret_key_name" {
  description = "Parameter Store SecureString name of the Airflow API server secret key."
  type        = string
  default     = "/healthcare_hai_prediction/airflow_api_secret_key"

  validation {
    condition     = length(var.airflow_api_secret_key_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.airflow_api_secret_key_name)) && !can(regex("^/(?i:aws|ssm)", var.airflow_api_secret_key_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "marquez_db_password_name" {
  description = "Parameter Store SecureString name of the password of the marquez database role."
  type        = string
  default     = "/healthcare_hai_prediction/marquez_db_password"

  validation {
    condition     = length(var.marquez_db_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.marquez_db_password_name)) && !can(regex("^/(?i:aws|ssm)", var.marquez_db_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "redis_password_name" {
  description = "Parameter Store SecureString name of the Redis password (new secret; the Feast online store clients read it too)."
  type        = string
  default     = "/healthcare_hai_prediction/redis_password"

  validation {
    condition     = length(var.redis_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.redis_password_name)) && !can(regex("^/(?i:aws|ssm)", var.redis_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "polaris_root_secret_name" {
  description = "Parameter Store SecureString name of the Polaris root principal secret (new secret; the root client id is root). Only the one-shot Polaris bootstrap task reads it."
  type        = string
  default     = "/healthcare_hai_prediction/polaris_root_secret"

  validation {
    condition     = length(var.polaris_root_secret_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.polaris_root_secret_name)) && !can(regex("^/(?i:aws|ssm)", var.polaris_root_secret_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

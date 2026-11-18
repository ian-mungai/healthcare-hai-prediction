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
  description = "Foundation output: the task role of Postgres, Redis, Airflow and the host preparation task. It holds no data permission."
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/", var.control_plane_task_role_arn))
    error_message = "Supply an IAM role ARN."
  }

  validation {
    condition     = var.control_plane_task_role_arn != var.polaris_task_role_arn
    error_message = "Polaris must have its own task role, so Airflow, Postgres and Redis hold no lakehouse permission."
  }
}

variable "serving_execution_role_arn" {
  description = "Foundation output: the dedicated execution role of the model API and the dashboard (owner decision D18). It pulls their two images, writes their log streams and injects their seven parameters, and reads no control-plane secret. Required when model_api_enabled or streamlit_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.serving_execution_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.serving_execution_role_arn, "x")))
    error_message = "Supply an IAM role ARN."
  }

  validation {
    condition     = !(var.model_api_enabled || var.streamlit_enabled) || var.serving_execution_role_arn != null
    error_message = "Set serving_execution_role_arn when the model API or the dashboard is on: they do not use the shared execution role."
  }

  validation {
    condition     = var.serving_execution_role_arn == null || var.serving_execution_role_arn != var.execution_role_arn
    error_message = "The serving tasks need an execution role of their own: the shared one reads the control-plane secrets."
  }
}

variable "log_group_names" {
  description = "Foundation output: log group name for each component. The keys postgres, redis, polaris and airflow are required, mlflow when mlflow_enabled is true, openmetadata when openmetadata_persistent_enabled is true, and monitoring when monitoring_enabled is true; components may share a group."
  type        = map(string)

  validation {
    condition     = alltrue([for component in ["postgres", "redis", "polaris", "airflow"] : contains(keys(var.log_group_names), component)])
    error_message = "Name a log group for postgres, redis, polaris and airflow."
  }

  validation {
    condition     = !var.openmetadata_persistent_enabled || contains(keys(var.log_group_names), "openmetadata")
    error_message = "Name a log group for openmetadata, or set openmetadata_persistent_enabled to false."
  }

  validation {
    condition     = !var.mlflow_enabled || contains(keys(var.log_group_names), "mlflow")
    error_message = "Name a log group for mlflow, or set mlflow_enabled to false."
  }

  validation {
    condition     = !var.monitoring_enabled || contains(keys(var.log_group_names), "monitoring")
    error_message = "Name a log group for monitoring, or set monitoring_enabled to false."
  }
}

variable "ecr_repository_urls" {
  description = "Foundation output: ECR repository URL for each image. The keys postgres, redis, polaris, polaris_admin and airflow are required, mlflow when mlflow_enabled is true, model_api when model_api_enabled is true, streamlit when streamlit_enabled is true, prometheus, grafana and statsd_exporter when monitoring_enabled is true, and openmetadata_server, openmetadata_ingestion and openmetadata_search when openmetadata_persistent_enabled is true."
  type        = map(string)

  validation {
    condition = alltrue([
      for image in concat(["postgres", "redis", "polaris", "polaris_admin", "airflow"], var.mlflow_enabled ? ["mlflow"] : [], var.model_api_enabled ? ["model_api"] : [], var.streamlit_enabled ? ["streamlit"] : [], var.monitoring_enabled ? ["prometheus", "grafana", "statsd_exporter", "pushgateway", "redis_exporter", "postgres_exporter", "node_exporter"] : [], var.openmetadata_persistent_enabled ? ["openmetadata_server", "openmetadata_ingestion", "openmetadata_search"] : []) :
      can(regex("^[0-9]{12}\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com/[a-z0-9._/-]+$", lookup(var.ecr_repository_urls, image, "")))
    ])
    error_message = "Name an ECR repository URL (without a tag or digest) for each image that runs, the monitoring images included when monitoring_enabled is true."
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
    condition     = !var.mlflow_enabled || contains(keys(var.image_digests), "mlflow")
    error_message = "Supply a digest for mlflow, or set mlflow_enabled to false."
  }

  validation {
    condition     = !var.model_api_enabled || contains(keys(var.image_digests), "model_api")
    error_message = "Supply a digest for model_api, or set model_api_enabled to false."
  }

  validation {
    condition     = !var.evidently_ui_enabled || alltrue([for image in ["drift", "training"] : contains(keys(var.image_digests), image)])
    error_message = "Supply the image digests of the drift and training images when evidently_ui_enabled is true: the UI runs the first and its restore step the second."
  }

  validation {
    condition     = !var.streamlit_enabled || contains(keys(var.image_digests), "streamlit")
    error_message = "Supply a digest for streamlit, or set streamlit_enabled to false."
  }

  validation {
    condition     = !var.monitoring_enabled || alltrue([for image in ["prometheus", "grafana", "statsd_exporter", "pushgateway", "redis_exporter", "postgres_exporter", "node_exporter"] : contains(keys(var.image_digests), image)])
    error_message = "Supply a digest for prometheus, grafana, statsd_exporter, pushgateway, redis_exporter, postgres_exporter and node_exporter, or set monitoring_enabled to false."
  }

  validation {
    condition     = !var.openmetadata_persistent_enabled || alltrue([for image in ["openmetadata_server", "openmetadata_ingestion", "openmetadata_search"] : contains(keys(var.image_digests), image)])
    error_message = "Supply a digest for openmetadata_server, openmetadata_ingestion and openmetadata_search, or set openmetadata_persistent_enabled to false."
  }
}

variable "mlflow_enabled" {
  description = "Run the MLflow tracking service on the host. Training jobs on AWS log runs to it. Its image is built from services/mlflow and pushed first. Set this to false to stop the service: the database and the artifacts stay."
  type        = bool
  default     = true
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

variable "host_private_ip" {
  description = "Private IPv4 address of the container host: the platform stack's host_private_ip output, which the services renderer reads, never typed by hand. The training jobs call MLflow at this address on port 5100, so MLflow accepts it as a Host header, and Airflow launches the host jobs with the catalog and MLflow addresses built from it. Required when mlflow_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = !var.mlflow_enabled || can(regex("^(10\\.[0-9]{1,3}|172\\.(1[6-9]|2[0-9]|3[01])|192\\.168)\\.[0-9]{1,3}\\.[0-9]{1,3}$", coalesce(var.host_private_ip, "none")))
    error_message = "Supply the private IPv4 address of the host (the platform output host_private_ip), or set mlflow_enabled to false."
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

variable "mlflow_admin_password_name" {
  description = "Parameter Store SecureString name of the password of the MLflow administrator account. MLflow asks for a login on every request and grants nothing by default. The training jobs and the owner hold this one credential."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_admin_password"

  validation {
    condition     = length(var.mlflow_admin_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.mlflow_admin_password_name)) && !can(regex("^/(?i:aws|ssm)", var.mlflow_admin_password_name)) && var.mlflow_admin_password_name != var.mlflow_db_password_name
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores, other than the MLflow database password. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
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
  description = "Memory of the container host in MiB (r7g.xlarge: 32768)."
  type        = number
  default     = 32768

  validation {
    condition     = var.host_memory_mib >= 8192
    error_message = "The host needs at least 8192 MiB."
  }
}

variable "host_usable_memory_mib" {
  description = "Memory in MiB that ECS can place on the host. It defaults to the current host size (r7g.xlarge: 32768 MiB, the same as host_memory_mib), so the stack always gives a job room; host_system_reserve_mib is the margin for the operating system, Docker and the ECS agent below it. After a host session the owner may set it to the MEMORY integerValue of registeredResources that describe-container-instances shows for the host, which then replaces the default."
  type        = number
  default     = 32768
  nullable    = false

  validation {
    condition     = var.host_usable_memory_mib > 0 && var.host_usable_memory_mib <= var.host_memory_mib && var.host_usable_memory_mib == floor(var.host_usable_memory_mib)
    error_message = "Use the ECS registered memory of the host: a whole number of MiB above 0 and at most the nominal host_memory_mib."
  }
}

variable "host_job_cap_mib" {
  description = "The host job cap in MiB: the memory one host job gets (one host job runs at a time). The stack renders it into every Airflow container as HAI_LAUNCH_HOST_JOB_CAP_MIB, which the launch preflight reads at every host launch (a missing or invalid value refuses the launch), and hands it to the jobs stack as the job memory (output host_job_memory_mib) and in the capacity binding. It must fit the room the services leave: the usable memory less host_system_reserve_mib and the always-on service reservations (output host_job_memory_mib refuses a larger cap). The default is the largest cap that fits with every service on, the optional ones included (the Evidently UI, the Prometheus history shipper and restore, the Postgres backup and restore and the MLflow user task): they reserve 8592 MiB on a 32768 MiB host, so 32768 - 2048 - 8592 = 22128 MiB. The services of the committed defaults alone reserve 8080 MiB and would leave 22640 MiB. A change of the cap is a host job cap change: it starts with the reviewed jobs inputs and their SHA-256 (--jobs-inputs and --jobs-sha256 of terraform.sh --keep-launch-hold) and ends with services_apply finish after the jobs apply. The authoritative procedure is the docstring of scripts/infrastructure/services_apply.py."
  type        = number
  default     = 22128
  nullable    = false

  validation {
    condition     = var.host_job_cap_mib >= 1024 && var.host_job_cap_mib < var.host_memory_mib && var.host_job_cap_mib == floor(var.host_job_cap_mib)
    error_message = "Use a whole number of MiB of at least 1024 and below the nominal host memory host_memory_mib."
  }
}

variable "host_launch_hold_parameter_name" {
  description = "Parameter Store name of the launch hold. The services apply command (scripts/infrastructure/services_apply.py, run by terraform.sh --services apply) sets it before the apply changes anything and clears it only after the ECS services reach steady state. The launch preflight and the fit probe refuse every host launch while it is set or missing. Not managed by Terraform, so an apply never clears it."
  type        = string
  default     = "/healthcare_hai_prediction/host_launch_hold"
  nullable    = false

  validation {
    condition     = length(var.host_launch_hold_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.host_launch_hold_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.host_launch_hold_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "services_apply_lock_parameter_name" {
  description = "Parameter Store name of the services apply lock that scripts/infrastructure/services_apply.py creates before each services apply and deletes after it settles (the IAM variable of the same name). The launch preflight and the send boundary refuse every host launch while it exists or cannot be read."
  type        = string
  default     = "/healthcare_hai_prediction/services_apply_lock"
  nullable    = false

  validation {
    condition     = length(var.services_apply_lock_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.services_apply_lock_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.services_apply_lock_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "host_system_reserve_mib" {
  description = "Safety margin in MiB that no task reservation gets: the operating system, Docker and the ECS agent below the nominal host memory, and memory the reservations do not count within the usable memory. The room for the host job cap (host_job_cap_mib) is the usable memory less this margin and the always-on service reservations."
  type        = number
  default     = 2048
  nullable    = false

  validation {
    condition     = var.host_system_reserve_mib >= 1024 && var.host_system_reserve_mib == floor(var.host_system_reserve_mib)
    error_message = "Keep at least 1024 MiB, a whole number, for the operating system, Docker and the ECS agent."
  }
}

variable "host_job_memory_floor_mib" {
  description = "Smallest job room in MiB the services may leave on the host; a plan whose cap host_job_cap_mib is below it is refused at the host_job_memory_mib output."
  type        = number
  default     = 4096
  nullable    = false

  validation {
    condition     = var.host_job_memory_floor_mib >= 1024 && var.host_job_memory_floor_mib == floor(var.host_job_memory_floor_mib)
    error_message = "Use a whole number of at least 1024 MiB."
  }
}

variable "other_services_reserve_mib" {
  description = "Memory in MiB that stays free of control-plane reservations: the operating system, the ECS agent and the later services (MLflow, prediction API, Streamlit). The monitoring containers count in the control-plane reservations when monitoring_enabled is true."
  type        = number
  default     = 5632

  validation {
    condition     = var.other_services_reserve_mib >= 2048
    error_message = "Keep at least 2048 MiB for the operating system and the ECS agent."
  }
}

variable "airflow_parallelism" {
  description = "Most tasks the Airflow scheduler runs at once; each task only launches a job on the host."
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

variable "monitoring_enabled" {
  description = "Run Prometheus, Grafana and the StatsD exporter as one task on the host, all on loopback, and send the Airflow metrics to the exporter. Their images are mirrored first (push_images.py plan) and their digests recorded, and the Grafana password parameter exists; the committed default is true. False plans no monitoring resource and no Airflow change."
  type        = bool
  default     = true
  nullable    = false
}

variable "grafana_admin_password_name" {
  description = "Parameter Store SecureString name of the Grafana administrator password (new secret). Only the Grafana container reads it, through the task execution role."
  type        = string
  default     = "/healthcare_hai_prediction/grafana_admin_password"

  validation {
    condition     = length(var.grafana_admin_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.grafana_admin_password_name)) && !can(regex("^/(?i:aws|ssm)", var.grafana_admin_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "redis_exporter_password_name" {
  description = "Parameter Store SecureString name of the password of the Redis user exporter, which the Redis exporter logs in with (created by an owner script, never by Terraform). Only the Redis task and the Redis exporter container read it, through the task execution role."
  type        = string
  default     = "/healthcare_hai_prediction/redis_exporter_password"

  validation {
    condition     = length(var.redis_exporter_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.redis_exporter_password_name)) && !can(regex("^/(?i:aws|ssm)", var.redis_exporter_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "postgres_exporter_password_name" {
  description = "Parameter Store SecureString name of the password of the Postgres role postgres_exporter, which the Postgres exporter logs in with (created by an owner script, never by Terraform). Only the Postgres database task and the Postgres exporter container read it, through the task execution role."
  type        = string
  default     = "/healthcare_hai_prediction/postgres_exporter_password"

  validation {
    condition     = length(var.postgres_exporter_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.postgres_exporter_password_name)) && !can(regex("^/(?i:aws|ssm)", var.postgres_exporter_password_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores; dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "alerts_topic_arn" {
  description = "ARN of the platform alerts topic (the alerts_topic_arn output of infra/platform). Grafana sends its alert notifications to this one topic through the Amazon SNS contact point in monitoring/grafana/provisioning/alerting, which reads the ARN from the container environment ALERTS_TOPIC_ARN. The task role (control_plane_task) needs sns:Publish and sns:GetTopicAttributes on it (infra/iam). Required while monitoring_enabled is true, because Grafana refuses to provision a contact point without a topic."
  type        = string
  default     = null

  validation {
    condition     = var.alerts_topic_arn == null || can(regex("^arn:aws:sns:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]{1,256}$", var.alerts_topic_arn))
    error_message = "Use the ARN of one SNS topic with no wildcard, for example the platform output alerts_topic_arn."
  }

  validation {
    condition     = !var.monitoring_enabled || var.alerts_topic_arn != null
    error_message = "Supply alerts_topic_arn while monitoring_enabled is true, or Grafana has no topic for its alert notifications."
  }
}

variable "monitoring_memory_mib" {
  description = "Hard limit and reservation in MiB of each monitoring container. The reservations sum to 464 MiB (Prometheus, Grafana, the StatsD exporter and the preparation container 304, the Pushgateway 32, the host state exporter 56 and the Redis, Postgres and node exporters 24 each, owner decisions of Nov 17 2026), which makes the job cap at most 22640 MiB with every service on. These are estimates, not measurements: replace them after the chain run shows the real peak (ECS reports task memory, and docker stats shows each container). Each limit must be at least 128 MiB for Prometheus and Grafana, 64 MiB for the host state exporter and the three exporters, and 32 MiB for the StatsD exporter and the Pushgateway, and at least its reservation."
  type = object({
    prometheus          = object({ limit = number, reservation = number })
    grafana             = object({ limit = number, reservation = number })
    statsd_exporter     = object({ limit = number, reservation = number })
    pushgateway         = object({ limit = number, reservation = number })
    host_state_exporter = object({ limit = number, reservation = number })
    redis_exporter      = object({ limit = number, reservation = number })
    postgres_exporter   = object({ limit = number, reservation = number })
    node_exporter       = object({ limit = number, reservation = number })
  })
  default = {
    prometheus          = { limit = 256, reservation = 128 }
    grafana             = { limit = 256, reservation = 128 }
    statsd_exporter     = { limit = 64, reservation = 32 }
    pushgateway         = { limit = 96, reservation = 32 }
    host_state_exporter = { limit = 128, reservation = 56 }
    redis_exporter      = { limit = 64, reservation = 24 }
    postgres_exporter   = { limit = 64, reservation = 24 }
    node_exporter       = { limit = 64, reservation = 24 }
  }
  nullable = false

  validation {
    condition = (
      var.monitoring_memory_mib.prometheus.limit >= 128 && var.monitoring_memory_mib.grafana.limit >= 128 && var.monitoring_memory_mib.statsd_exporter.limit >= 32
      && var.monitoring_memory_mib.pushgateway.limit >= 32 && var.monitoring_memory_mib.host_state_exporter.limit >= 64
      && var.monitoring_memory_mib.redis_exporter.limit >= 64 && var.monitoring_memory_mib.postgres_exporter.limit >= 64 && var.monitoring_memory_mib.node_exporter.limit >= 64
      && alltrue([for memory in values(var.monitoring_memory_mib) : memory.reservation >= 1 && memory.reservation <= memory.limit])
    )
    error_message = "Each limit needs at least 128 MiB (Prometheus, Grafana), 64 MiB (the host state exporter and the three exporters) or 32 MiB (the StatsD exporter, the Pushgateway), and each reservation at least 1 MiB and at most its limit."
  }
}

variable "prometheus_retention_days" {
  description = "Days of samples Prometheus keeps. Older blocks are deleted. The size limit below stops the store sooner when it is reached first."
  type        = number
  default     = 15
  nullable    = false

  validation {
    condition     = var.prometheus_retention_days >= 1 && var.prometheus_retention_days <= 90 && floor(var.prometheus_retention_days) == var.prometheus_retention_days
    error_message = "Use a whole number of days from 1 to 90."
  }
}

variable "prometheus_retention_size_mb" {
  description = "Most MB the Prometheus blocks and write-ahead log may use on the data volume. Prometheus deletes the oldest blocks once the total passes this limit."
  type        = number
  default     = 2048
  nullable    = false

  validation {
    condition     = var.prometheus_retention_size_mb >= 256 && var.prometheus_retention_size_mb <= 16384 && floor(var.prometheus_retention_size_mb) == var.prometheus_retention_size_mb
    error_message = "Use a whole number of MB from 256 to 16384."
  }
}

variable "airflow_launch_task_role_arn" {
  description = "IAM output airflow_launch_roles: the task role of the Airflow scheduler, which runs every DAG step under the LocalExecutor and launches the jobs through the direct ECS launch library. It may start, stop and describe the host definitions of the named job families, pass their roles and write the launch records. The API server, the DAG processor and the triggerer keep the control-plane role. Null keeps the scheduler on the control-plane role, which can launch nothing."
  type        = string
  default     = null

  validation {
    condition     = var.airflow_launch_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.airflow_launch_task_role_arn, "none")))
    error_message = "Supply an IAM role ARN for the Airflow scheduler, or leave airflow_launch_task_role_arn null."
  }

  validation {
    condition = var.airflow_launch_task_role_arn == null || (
      var.airflow_launch_task_role_arn != var.control_plane_task_role_arn && var.airflow_launch_task_role_arn != var.polaris_task_role_arn &&
      var.airflow_launch_task_role_arn != var.mlflow_task_role_arn
    )
    error_message = "The Airflow scheduler must have its own launch role, so the control plane, Polaris and MLflow hold no permission to start job tasks and the scheduler holds no lakehouse or artifact permission."
  }

  validation {
    condition     = var.airflow_launch_task_role_arn == null || can(regex("^(10\\.[0-9]{1,3}|172\\.(1[6-9]|2[0-9]|3[01])|192\\.168)\\.[0-9]{1,3}\\.[0-9]{1,3}$", coalesce(var.host_private_ip, "none")))
    error_message = "The launched jobs reach the catalog and MLflow at the host's private address: supply host_private_ip with airflow_launch_task_role_arn."
  }
}

variable "airflow_launch_bucket_name" {
  description = "Name of the data bucket that holds the launch records and the staged inputs of the jobs the Airflow scheduler launches (HAI_LAUNCH_BUCKET). Required when airflow_launch_task_role_arn is set."
  type        = string
  default     = null

  validation {
    condition     = var.airflow_launch_task_role_arn == null || (var.airflow_launch_bucket_name != null && can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", coalesce(var.airflow_launch_bucket_name, "!"))))
    error_message = "Name the data bucket of the launch records and staged inputs, or leave airflow_launch_task_role_arn null."
  }
}

variable "airflow_launch_scratch_path" {
  description = "Absolute host folder of the scratch volume that the Airflow containers see read-only, so that the launch library can check the free space before it starts a job."
  type        = string
  default     = "/data/job_scratch"

  validation {
    condition     = can(regex("^/[A-Za-z0-9_][A-Za-z0-9_./-]*[A-Za-z0-9_]$", var.airflow_launch_scratch_path)) && !strcontains(var.airflow_launch_scratch_path, "..") && !strcontains(var.airflow_launch_scratch_path, "//")
    error_message = "Use an absolute folder without a trailing slash, a parent step or a double slash, for example /data/job_scratch."
  }
}

variable "airflow_lineage_http_enabled" {
  description = "Send the OpenLineage events of the Airflow tasks over HTTP only to OpenMetadata's lineage route (/api/v1/openlineage/lineage), with the lineage bot credential of openmetadata_bot_token_names as an ECS secret of the scheduler and a 5 second limit, and switch the provider on for that container only. It is on by default and needs persistent OpenMetadata running and the execution role granted the lineage bot parameter (the parameter_names_used output names it). False leaves the provider disabled in every Airflow container (AIRFLOW__OPENLINEAGE__DISABLED true, no transport, no spool), so no lineage event is sent, and adds no lineage bot secret to the scheduler; with the flag on, a task or DAG run changed by hand in the UI or REST still sends no lineage event, because the api-server stays disabled; the lineage code mounts of the Airflow containers are present either way."
  type        = bool
  default     = true
  nullable    = false

  validation {
    condition     = !var.airflow_lineage_http_enabled || var.openmetadata_persistent_enabled
    error_message = "HTTP lineage goes only to the persistent OpenMetadata catalog: set openmetadata_persistent_enabled to true first."
  }
}

variable "airflow_lineage_url" {
  description = "Address of OpenMetadata's API as the scheduler reaches it, with no path: the client appends the lineage route."
  type        = string
  default     = "http://127.0.0.1:8585"
  nullable    = false

  validation {
    condition     = can(regex("^http://(127\\.0\\.0\\.1|10(\\.[0-9]{1,3}){3}|172\\.(1[6-9]|2[0-9]|3[01])(\\.[0-9]{1,3}){2}|192\\.168(\\.[0-9]{1,3}){2}):8585$", var.airflow_lineage_url))
    error_message = "Use http://<loopback or private IPv4 address>:8585 with no path, the only port OpenMetadata's API listens on."
  }
}

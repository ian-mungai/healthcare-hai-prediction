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
  description = "Foundation output: log group name for each component. The keys postgres, redis, polaris, airflow and marquez are required, mlflow when mlflow_enabled is true, and monitoring when monitoring_enabled is true; components may share a group."
  type        = map(string)

  validation {
    condition     = alltrue([for component in ["postgres", "redis", "polaris", "airflow", "marquez"] : contains(keys(var.log_group_names), component)])
    error_message = "Name a log group for postgres, redis, polaris, airflow and marquez."
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
  description = "Foundation output: ECR repository URL for each image. The keys postgres, redis, polaris, polaris_admin and airflow are required, marquez and marquez_web when marquez_enabled is true, mlflow when mlflow_enabled is true, model_api when model_api_enabled is true, streamlit when streamlit_enabled is true, and prometheus, grafana and statsd_exporter when monitoring_enabled is true."
  type        = map(string)

  validation {
    condition = alltrue([
      for image in concat(["postgres", "redis", "polaris", "polaris_admin", "airflow"], var.marquez_enabled ? ["marquez", "marquez_web"] : [], var.mlflow_enabled ? ["mlflow"] : [], var.model_api_enabled ? ["model_api"] : [], var.streamlit_enabled ? ["streamlit"] : [], var.monitoring_enabled ? ["prometheus", "grafana", "statsd_exporter"] : []) :
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
    condition     = !var.marquez_enabled || alltrue([for image in ["marquez", "marquez_web"] : contains(keys(var.image_digests), image)])
    error_message = "Supply a digest for marquez and marquez_web, or set marquez_enabled to false."
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
    condition     = !var.streamlit_enabled || contains(keys(var.image_digests), "streamlit")
    error_message = "Supply a digest for streamlit, or set streamlit_enabled to false."
  }

  validation {
    condition     = !var.monitoring_enabled || alltrue([for image in ["prometheus", "grafana", "statsd_exporter"] : contains(keys(var.image_digests), image)])
    error_message = "Supply a digest for prometheus, grafana and statsd_exporter, or set monitoring_enabled to false."
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

variable "mlflow_admin_password_name" {
  description = "Parameter Store SecureString name of the password of the MLflow administrator account. MLflow asks for a login on every request and grants nothing by default. The training jobs and the owner hold this one credential."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_admin_password"

  validation {
    condition     = length(var.mlflow_admin_password_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.mlflow_admin_password_name)) && !can(regex("^/(?i:aws|ssm)", var.mlflow_admin_password_name)) && var.mlflow_admin_password_name != var.mlflow_db_password_name
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores, other than the MLflow database password. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "broker_enabled" {
  description = "Run the lean job broker on the host. Airflow reaches it on loopback port 18790 with a token from Parameter Store, and it starts the named job tasks on Fargate. Set this to false to stop it: the ledger on the data volume stays."
  type        = bool
  default     = false
}

variable "broker_task_role_arn" {
  description = "Foundation output: the task role of the broker. It may run, stop and describe the named job families, pass their roles and read the code digest of a run. Required when broker_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = !var.broker_enabled || (var.broker_task_role_arn != null && can(regex("^arn:aws:iam::[0-9]{12}:role/", coalesce(var.broker_task_role_arn, "none"))))
    error_message = "Supply an IAM role ARN for the broker, or set broker_enabled to false."
  }

  validation {
    condition = !var.broker_enabled || (
      var.broker_task_role_arn != var.control_plane_task_role_arn && var.broker_task_role_arn != var.polaris_task_role_arn && var.broker_task_role_arn != var.mlflow_task_role_arn
    )
    error_message = "The broker must have its own task role, so the control plane, Polaris and MLflow hold no permission to start job tasks."
  }
}

variable "broker_token_name" {
  description = "Parameter Store SecureString name of the broker session token. The broker and the Airflow scheduler read it. Set its value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/broker_token"

  validation {
    condition     = length(var.broker_token_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.broker_token_name)) && !can(regex("^/(?i:aws|ssm)", var.broker_token_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "broker_signed_parameter_name" {
  description = "Parameter Store name of the record of the first signed silver and gold publishes: a JSON object with the keys silver and gold, each a run ID. The owner puts it after signing both reports. Until it exists and is valid, the broker refuses every chain job."
  type        = string
  default     = "/healthcare_hai_prediction/first_publishes_signed"

  validation {
    condition     = length(var.broker_signed_parameter_name) <= 512 && can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.broker_signed_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.broker_signed_parameter_name))
    error_message = "Use a parameter path of at most 512 characters that starts with a slash and has up to 15 parts of letters, digits and underscores. Dashes, wildcards, spaces and the prefixes aws and ssm are not allowed."
  }
}

variable "broker_jobs" {
  description = "Where the broker starts job tasks: the closed job subnet and security group, the jobs log group, the data bucket that holds the staged code digest, the private address of the host and the job families to enable. Required when broker_enabled is true."
  type = object({
    subnet_id          = string
    security_group_ids = map(string)
    log_group_name     = string
    bucket_name        = string
    host_ip            = string
    families           = list(string)
  })
  default = null

  validation {
    condition = !var.broker_enabled || (
      var.broker_jobs != null &&
      can(regex("^subnet-[a-z0-9]+$", try(var.broker_jobs.subnet_id, "!"))) &&
      length(try(var.broker_jobs.security_group_ids, {})) > 0 &&
      alltrue([for class, group in try(var.broker_jobs.security_group_ids, {}) : contains(["lakehouse", "training"], class) && can(regex("^sg-[a-z0-9]+$", group))]) &&
      can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", try(var.broker_jobs.bucket_name, "!"))) &&
      can(regex("^(10\\.[0-9]{1,3}|172\\.(1[6-9]|2[0-9]|3[01])|192\\.168)\\.[0-9]{1,3}\\.[0-9]{1,3}$", try(var.broker_jobs.host_ip, "!")))
    )
    error_message = "Supply the job subnet, a security group for each network class (lakehouse and training), the data bucket and the private address of the host, or set broker_enabled to false."
  }

  # Each family runs only in the network class that config/orchestration/ecs_broker_jobs.json approves for it. The lakehouse class reaches the catalog and Redis,
  # the training class also reaches MLflow. A test compares this map with the job table.
  validation {
    condition = !var.broker_enabled || (var.broker_jobs != null && alltrue([
      for family in try(var.broker_jobs.families, []) : contains(
        keys(try(var.broker_jobs.security_group_ids, {})),
        lookup({ train = "training", silver = "lakehouse", gold = "lakehouse", features = "lakehouse", drift = "training", promote = "training", metadata = "lakehouse", bronze = "lakehouse" }, family, "none")
      )
    ]))
    error_message = "Every enabled job family needs the security group of its own network class: silver, gold, features, metadata and bronze use lakehouse, and train, drift and promote use training."
  }

  validation {
    condition     = !var.broker_enabled || (var.broker_jobs != null && length(try(var.broker_jobs.families, [])) > 0 && alltrue([for family in try(var.broker_jobs.families, []) : contains(["train", "silver", "gold", "features", "drift", "promote", "metadata", "bronze"], family)]))
    error_message = "Name at least one job family and only from train, silver, gold, features, drift, promote, metadata and bronze."
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
  description = "Memory in MiB that stays free of control-plane reservations: the operating system, the ECS agent and the later services (MLflow, prediction API, Streamlit). The monitoring containers count in the control-plane reservations when monitoring_enabled is true."
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

variable "monitoring_enabled" {
  description = "Run Prometheus, Grafana and the StatsD exporter as one task on the host, all on loopback, and send the Airflow metrics to the exporter. Their images are mirrored first (push_images.py plan) and their digests recorded, and the Grafana password parameter exists; this stays false until then. False plans no monitoring resource and no Airflow change."
  type        = bool
  default     = false
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

variable "monitoring_memory_mib" {
  description = "Hard limit and reservation in MiB of each monitoring container. These are estimates for a few hundred series and one dashboard, not measurements: replace them after the silver and gold runs show the real peak (ECS reports task memory, and docker stats shows each container). Each limit must be at least 128 MiB for Prometheus and Grafana and 32 MiB for the exporter, and at least its reservation."
  type = object({
    prometheus      = object({ limit = number, reservation = number })
    grafana         = object({ limit = number, reservation = number })
    statsd_exporter = object({ limit = number, reservation = number })
  })
  default = {
    prometheus      = { limit = 256, reservation = 128 }
    grafana         = { limit = 256, reservation = 128 }
    statsd_exporter = { limit = 64, reservation = 32 }
  }
  nullable = false

  validation {
    condition = (
      var.monitoring_memory_mib.prometheus.limit >= 128 && var.monitoring_memory_mib.grafana.limit >= 128 && var.monitoring_memory_mib.statsd_exporter.limit >= 32
      && var.monitoring_memory_mib.prometheus.reservation >= 1 && var.monitoring_memory_mib.grafana.reservation >= 1 && var.monitoring_memory_mib.statsd_exporter.reservation >= 1
      && var.monitoring_memory_mib.prometheus.reservation <= var.monitoring_memory_mib.prometheus.limit
      && var.monitoring_memory_mib.grafana.reservation <= var.monitoring_memory_mib.grafana.limit
      && var.monitoring_memory_mib.statsd_exporter.reservation <= var.monitoring_memory_mib.statsd_exporter.limit
    )
    error_message = "Each limit needs at least 128 MiB (Prometheus, Grafana) or 32 MiB (the exporter), and each reservation at least 1 MiB and at most its limit."
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

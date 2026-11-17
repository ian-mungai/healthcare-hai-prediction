variable "openmetadata_ingestion_token_name" {
  description = "Parameter Store SecureString name of the ingestion bot token of the persistent catalog, one bot token per caller. The execution role injects it into the metadata host task. The persistent catalog bootstrap writes it. Terraform references the name and never reads the value."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_bot_ingestion"
  nullable    = false

  validation {
    condition     = !(var.host_jobs_enabled && contains(var.host_jobs, "metadata")) || var.openmetadata_ingestion_token_name == "/${var.project_name}/openmetadata_bot_ingestion"
    error_message = "Inject only the project ingestion bot token parameter."
  }
}

variable "mlflow_metadata_reader_password_name" {
  description = "Parameter Store SecureString name of the password of the read-only MLflow user metadata_reader, which the MLflow source and the model edge step of the metadata host task read the registry with. The execution role injects it. The owner step scripts/infrastructure/mlflow_reader_setup.py writes it. Terraform references the name and never reads a value."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_metadata_reader_password"
  nullable    = false

  validation {
    condition     = !(var.host_jobs_enabled && contains(var.host_jobs, "metadata")) || var.mlflow_metadata_reader_password_name == "/${var.project_name}/mlflow_metadata_reader_password"
    error_message = "Inject only the project metadata reader password parameter."
  }
}

variable "openmetadata_lineage_token_name" {
  description = "Parameter Store SecureString name of the lineage bot token of the persistent catalog, the one Airflow's scheduler sends with. The execution role injects it into the pin and upload containers of the layer jobs while job_lineage_enabled is true. The persistent catalog bootstrap writes it. Terraform references the name and never reads the value."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_bot_lineage"
  nullable    = false

  validation {
    condition     = !var.job_lineage_enabled || var.openmetadata_lineage_token_name == "/${var.project_name}/openmetadata_bot_lineage"
    error_message = "Inject only the project lineage bot token parameter."
  }
}

variable "job_lineage_enabled" {
  description = "Send the lineage events of the silver, gold and features host jobs (and the six model sets) from their pin and upload containers (orchestration/job_lineage.py): the HTTP transport settings at the host address and the lineage bot token as an ECS secret. Needs host_private_ip, and the shared job execution role must hold the lineage token grant (IAM input job_lineage_prepared) before the first launch of a revision that has it, or ECS cannot start the task. False adds no lineage setting and no lineage secret to any container."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.job_lineage_enabled || var.host_private_ip != null
    error_message = "Job lineage goes to the persistent catalog on the host: supply host_private_ip first."
  }
}

variable "openmetadata_images" {
  description = "Ingestion image URI from the project ECR repository <project>_openmetadata_ingestion_<environment>, pinned by digest. The metadata host task needs only this image. The ARM64 host runs it, so it must list linux/arm64."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = alltrue([for name, image in var.openmetadata_images :
      name == "ingestion" &&
      can(regex("^${var.expected_account_id}\\.dkr\\.ecr\\.${var.aws_region}\\.amazonaws\\.com/${var.project_name}_openmetadata_ingestion_${var.environment}@sha256:[a-f0-9]{64}$", image))
    ])
    error_message = "Supply the ingestion image from its project ECR repository with an immutable SHA-256 digest."
  }
}

variable "openmetadata_roles" {
  description = "The merged IAM role ARNs of the metadata task, keyed execution and openmetadata_task."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = alltrue([for name, arn in var.openmetadata_roles :
      contains(["execution", "openmetadata_task"], name) && arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_${name}_${var.environment}"
    ])
    error_message = "Bind only the exact merged execution role and catalog task role. A writer or another job's role is refused."
  }
}

locals {
  om_common_environment = [
    { name = "AWS_REGION", value = var.aws_region },
    { name = "AWS_DEFAULT_REGION", value = var.aws_region },
    { name = "AWS_EC2_METADATA_DISABLED", value = "true" },
    { name = "DATA_BUCKET_NAME", value = var.data_bucket_name },
    { name = "HOME", value = "/tmp" },
    { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
    { name = "PYTHONUNBUFFERED", value = "1" },
  ]
}

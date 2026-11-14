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

variable "register_definition" {
  description = "Register the reviewed candidate definition. This does not activate bronze or launch a task."
  type        = bool
  default     = false
  nullable    = false
}

variable "project_name" {
  description = "Project resource-name prefix."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Use dev or prod."
  }
}

variable "aws_region" {
  description = "AWS region from the reviewed target contract."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Use an AWS region name."
  }
}

variable "expected_account_id" {
  description = "Account that owns the reviewed task and parameter identities."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Use a twelve-digit account ID."
  }
}

variable "task_role_arn" {
  description = "Prepared bronze task role ARN from the IAM output."
  type        = string
}

variable "execution_role_arn" {
  description = "Job execution role ARN with the reviewed bronze-pair injection delta."
  type        = string
}

variable "image" {
  description = "Reviewed worker image pinned by its ECR SHA-256 digest."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com/[a-z0-9_./-]+@sha256:[a-f0-9]{64}$", var.image))
    error_message = "Use an ECR image URI with an immutable SHA-256 digest."
  }
}

variable "cpu" {
  description = "Fargate CPU units selected by the reviewed capacity contract."
  type        = number

  validation {
    condition     = contains([1024, 2048, 4096, 8192, 16384], var.cpu)
    error_message = "Use a supported whole-vCPU Fargate shape."
  }
}

variable "cpu_architecture" {
  description = "Worker image architecture from the reviewed target contract."
  type        = string

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "Use the architecture of the reviewed worker image."
  }
}

variable "memory" {
  description = "Fargate memory in MiB selected by the reviewed capacity contract."
  type        = number
}

variable "job_threads" {
  description = "Spark thread count from the reviewed capacity contract."
  type        = number
}

variable "spark_memory" {
  description = "Spark heap from the reviewed capacity contract, expressed as whole GiB."
  type        = string

  validation {
    condition     = can(regex("^[1-9][0-9]*g$", var.spark_memory))
    error_message = "Use a positive whole-GiB heap such as 1g."
  }
}

variable "polaris_uri" {
  description = "Private AWS catalog endpoint from the verified target contract."
  type        = string

  validation {
    condition     = can(regex("^https?://[^/]+/.+$", var.polaris_uri))
    error_message = "Supply the private catalog REST endpoint from the verified target."
  }
}

variable "log_group_name" {
  description = "Existing job log group managed by the jobs stack."
  type        = string
}

variable "bootstrap_entrypoint" {
  description = "Worker bootstrap that verifies the staged inputs and materializes the immutable binding before executing the command."
  type        = list(string)

  validation {
    condition     = length(var.bootstrap_entrypoint) > 0
    error_message = "Supply the reviewed worker bootstrap entrypoint."
  }
}

variable "run_binding_path" {
  description = "Immutable run-binding file materialized by the worker bootstrap."
  type        = string
  default     = "/run/job/bronze_binding.json"

  validation {
    condition     = can(regex("^/run/job/[A-Za-z0-9_]+\\.json$", var.run_binding_path))
    error_message = "Keep the binding in the isolated job run directory."
  }
}

variable "aws_polaris_bronze_client_id_name" {
  description = "Bronze client ID parameter name from the IAM contract."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_bronze_client_id"
}

variable "aws_polaris_bronze_client_secret_name" {
  description = "Bronze client secret parameter name from the IAM contract. No value enters this module."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_bronze_client_secret"
}

variable "tags" {
  description = "Explicit task-definition tags. Registration requires the Project tag used by the project's ECS policy."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition     = !var.register_definition || try(var.tags["Project"] == var.project_name, false)
    error_message = "Task registration requires tags.Project to match project_name."
  }
}

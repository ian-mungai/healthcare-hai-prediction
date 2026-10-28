variable "aws_profile" {
  description = "Project deployment profile from the environment renderer."
  type        = string
  nullable    = false

  validation {
    condition     = var.aws_profile == "${var.project_name}_${var.environment}" && length(var.aws_profile) <= 64
    error_message = "Use the project profile named <project>_<environment>."
  }
}

variable "aws_region" {
  description = "Region of the reviewed foundation outputs."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "Supply an AWS region code."
  }
}

variable "expected_account_id" {
  description = "Verified account that owns the merged roles, images and parameters."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "Supply a twelve-digit account ID."
  }
}

variable "project_name" {
  description = "Underscore project identifier and foundation Project tag."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{2,47}$", var.project_name))
    error_message = "Use 3 to 48 lowercase letters, digits and underscores, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment of the foundation and job roles."
  type        = string
  nullable    = false

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Use dev or prod."
  }
}

variable "data_bucket_name" {
  description = "Existing data bucket supplied by the base environment renderer."
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.data_bucket_name))
    error_message = "Supply the existing data bucket name."
  }
}

variable "jobs_enabled" {
  description = "Register selected application definitions only after their exact plan and image digests are reviewed."
  type        = bool
  default     = false
  nullable    = false
}

variable "enabled_definitions" {
  description = "Selected read and writer definitions. An empty selection creates no resources."
  type        = set(string)
  default     = []
  nullable    = false

  validation {
    condition = (
      (var.jobs_enabled || length(var.enabled_definitions) == 0) &&
      length(setsubtract(var.enabled_definitions, toset(keys(local.all_definition_specs)))) == 0
    )
    error_message = "Select only the five read or five writer definitions and enable jobs_enabled before selecting any definition."
  }
}

variable "images" {
  description = "Measured analytics, quality and Spark image URIs from the merged ECR repositories, pinned by digest."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, image in var.images :
        contains(["analytics", "quality", "spark"], name) &&
        can(regex("^${var.expected_account_id}\\.dkr\\.ecr\\.${var.aws_region}\\.amazonaws\\.com/${var.project_name}_${name}_${var.environment}@sha256:[a-f0-9]{64}$", image))
      ]) &&
      alltrue([for name in var.enabled_definitions : contains(keys(var.images), local.all_definition_specs[name].image) if contains(keys(local.all_definition_specs), name)])
    )
    error_message = "Supply every selected image from its project ECR repository with an immutable SHA-256 digest."
  }
}

variable "job_role_arns" {
  description = "ARNs from the merged IAM job_roles output, keyed by execution, offline_task, seal_task, read_task or the five write_<layer>_task keys."
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for name, arn in var.job_role_arns :
        contains(concat(["execution", "offline_task", "seal_task", "read_task"], keys(local.writer_role_layers)), name) &&
        arn == "arn:aws:iam::${var.expected_account_id}:role/${var.project_name}_job_${name}_${var.environment}"
      ]) &&
      (length(var.enabled_definitions) == 0 || contains(keys(var.job_role_arns), "execution")) &&
      alltrue([for name in var.enabled_definitions : contains(keys(var.job_role_arns), local.all_definition_specs[name].role) if contains(keys(local.all_definition_specs), name)])
    )
    error_message = "Bind only the exact merged job execution and selected class roles. Cross-layer and foundation role substitutions are refused."
  }
}

variable "task_resources" {
  description = "Reviewed Fargate CPU units, memory MiB and ephemeral storage GiB for each selected definition."
  type = map(object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  }))
  default  = {}
  nullable = false

  validation {
    condition = (
      alltrue([for name in var.enabled_definitions : contains(keys(var.task_resources), name)]) &&
      alltrue([for name, resources in var.task_resources : try(
        contains(keys(local.all_definition_specs), name) &&
        resources.memory >= local.memory_shapes[tostring(resources.cpu)].min &&
        resources.memory <= local.memory_shapes[tostring(resources.cpu)].max &&
        resources.memory % local.memory_shapes[tostring(resources.cpu)].step == 0 &&
        resources.ephemeral_storage_gib >= 20 && resources.ephemeral_storage_gib <= 200 &&
        resources.ephemeral_storage_gib == floor(resources.ephemeral_storage_gib), false
      )])
    )
    error_message = "Supply a valid whole-vCPU Fargate shape and 20 to 200 whole GiB of scratch for every selected definition."
  }
}

variable "cloud_limits" {
  description = "Fixed aggregate admission cap. The dispatcher must count pending, running and unknown tasks. Required when selecting definitions."
  type = object({
    max_tasks      = number
    max_vcpu       = number
    max_memory_gib = number
  })
  default = null

  validation {
    condition     = var.cloud_limits == null ? length(var.enabled_definitions) == 0 : var.cloud_limits == { max_tasks = 3, max_vcpu = 16, max_memory_gib = 120 }
    error_message = "Enabled definitions require the fixed cap of three tasks, 16 vCPU and 120 GiB."
  }
}

variable "log_group_name" {
  description = "Existing jobs log group from the merged platform output. This stack creates no log store."
  type        = string
  default     = null

  validation {
    condition     = var.log_group_name == null ? length(var.enabled_definitions) == 0 : var.log_group_name == "${var.project_name}_jobs_${var.environment}"
    error_message = "Supply the merged platform jobs log group for every enabled definition."
  }
}

variable "aws_polaris_reader_client_id_name" {
  description = "Reader client ID parameter name. Terraform references the name and never reads its value."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_reader_client_id"
  nullable    = false

  validation {
    condition     = !var.jobs_enabled || var.aws_polaris_reader_client_id_name == "/${var.project_name}/aws_polaris_reader_client_id"
    error_message = "Inject only the project AWS reader client ID parameter."
  }
}

variable "aws_polaris_reader_client_secret_name" {
  description = "Reader client secret parameter name. No writer or administrator credential is accepted."
  type        = string
  default     = "/healthcare_hai_prediction/aws_polaris_reader_client_secret"
  nullable    = false

  validation {
    condition     = !var.jobs_enabled || var.aws_polaris_reader_client_secret_name == "/${var.project_name}/aws_polaris_reader_client_secret"
    error_message = "Inject only the project AWS reader client secret parameter."
  }
}

variable "writer_parameter_names" {
  description = "Owner-issued parameter pairs from the writer IAM contracts, keyed by write_<layer>_task. Each list contains the client ID name followed by the client secret name."
  type        = map(list(string))
  default     = {}
  nullable    = false

  validation {
    condition = (
      alltrue([for role, names in var.writer_parameter_names : try(
        contains(keys(local.writer_role_layers), role) && length(names) == 2 &&
        alltrue([for index, suffix in ["id", "secret"] :
          length(names[index]) <= 512 &&
          can(regex("^/(?:[A-Za-z0-9_]+/){0,14}aws_polaris_${local.writer_role_layers[role]}_client_${suffix}$", names[index])) &&
          !can(regex("^/(?i:aws|ssm)", names[index]))
        ]), false
      )]) &&
      alltrue([for name in var.enabled_definitions : contains(keys(var.writer_parameter_names), local.writer_specs[name].role) if contains(keys(local.writer_specs), name)])
    )
    error_message = "Bind exactly the dedicated client ID and client secret parameter names for each selected writer role. Never substitute another layer or reader pair."
  }
}

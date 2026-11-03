variable "github_oidc_enabled" {
  description = "Create the GitHub OpenID Connect provider and the four delivery roles. Off by default: with it off the stack plans no change. The owner turns it on in a reviewed commit that changes this default."
  type        = bool
  default     = false
  nullable    = false
}

variable "github_repository" {
  description = "The one GitHub repository, owner/name, whose workflows may assume the delivery roles. A pattern is refused."
  type        = string
  default     = "ian-mungai/healthcare-hai-prediction"
  nullable    = false

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "Use exactly owner/name with letters, digits, dots, underscores and hyphens and no pattern characters."
  }
}

variable "github_oidc_provider_arn" {
  description = "ARN of a GitHub OpenID Connect provider that already exists in the account (AWS allows one per issuer). Empty creates it."
  type        = string
  default     = ""
  nullable    = false

  validation {
    condition     = var.github_oidc_provider_arn == "" || can(regex("^arn:aws:iam::[0-9]{12}:oidc-provider/token\\.actions\\.githubusercontent\\.com$", var.github_oidc_provider_arn))
    error_message = "Use the ARN of the token.actions.githubusercontent.com provider or leave it empty."
  }
}

variable "github_oidc_apply_policy_services" {
  description = "Services whose existing project policy (infra/iam/policies) the apply role attaches by ARN. Each must be a service with a policy file there. IAM allows 10 managed policies per role by default."
  type        = list(string)
  default     = ["ecs", "ecr", "ec2", "dlm", "logs", "cloudwatch", "sns", "ssm", "iam", "autoscaling"]
  nullable    = false

  validation {
    condition     = length(var.github_oidc_apply_policy_services) <= 10 && length(distinct(var.github_oidc_apply_policy_services)) == length(var.github_oidc_apply_policy_services)
    error_message = "Name at most 10 distinct services, the default IAM quota of managed policies per role."
  }
}

variable "github_oidc_deploy_services" {
  description = "Names of the ECS services in the project cluster that the deploy role may update. Each starts with <project>_<environment>_, the services stack naming, because the delivery boundary allows no other name. Empty until the model API names its service."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = alltrue([for name in var.github_oidc_deploy_services : can(regex("^[A-Za-z0-9_-]{1,255}$", name))])
    error_message = "Use exact ECS service names, without pattern characters."
  }
}

variable "github_oidc_deploy_task_role_names" {
  description = "Names of the task roles the deploy role may pass to ECS tasks. The delivery boundary allows only <project>_model_api_task_<environment>, <project>_streamlit_task_<environment> and <project>_task_execution_<environment>. Empty until the model API names its roles."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = alltrue([for name in var.github_oidc_deploy_task_role_names : can(regex("^[A-Za-z0-9_+=,.@-]{1,64}$", name))])
    error_message = "Use exact IAM role names, without pattern characters."
  }
}

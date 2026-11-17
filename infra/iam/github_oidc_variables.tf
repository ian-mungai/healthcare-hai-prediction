variable "github_oidc_enabled" {
  description = "Create the GitHub OpenID Connect provider and the four delivery roles. On by default: the committed default is the live setting, and false plans the removal of all of them."
  type        = bool
  default     = true
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

variable "github_oidc_subject_prefix" {
  description = "The subject prefix GitHub sends for this repository since its immutable subject setting is on: repo:<owner>@<owner id>/<name>@<repository id>. Every role trusts <prefix>:environment:<name> and the plan role also <prefix>:pull_request. Read it from the repository's sub_claim_prefix. The name form repo:<owner>/<name> never matches an immutable-subject token."
  type        = string
  default     = "repo:ian-mungai@29179213/healthcare-hai-prediction@1376048206"
  nullable    = false

  validation {
    condition     = can(regex("^repo:[A-Za-z0-9_.-]+@[0-9]+/[A-Za-z0-9_.-]+@[0-9]+$", var.github_oidc_subject_prefix))
    error_message = "Use the ID form repo:<owner>@<owner id>/<name>@<repository id>, with digits for both ids and no pattern characters or trailing claim."
  }

  validation {
    condition = (
      !can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository)) ||
      !can(regex("^repo:([A-Za-z0-9_.-]+)@[0-9]+/([A-Za-z0-9_.-]+)@[0-9]+$", var.github_oidc_subject_prefix)) ||
      join("/", regex("^repo:([A-Za-z0-9_.-]+)@[0-9]+/([A-Za-z0-9_.-]+)@[0-9]+$", var.github_oidc_subject_prefix)) == var.github_repository
    )
    error_message = "The owner and repository names in github_oidc_subject_prefix must be exactly those of github_repository. Change both together."
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

variable "github_oidc_plan_role_enabled" {
  description = "Create the read-only Terraform plan role, its policy and its trust of pull requests and the terraform-plan environment. Off until the remote state protection and the state move to the project bucket are done: the role reads state there. Needs github_oidc_enabled."
  type        = bool
  default     = false
  nullable    = false
}

variable "github_oidc_deploy_serving_enabled" {
  description = "Let the deploy role update the two serving services of the project cluster, named <project>_<environment>_model_api and <project>_<environment>_streamlit as the services stack names them. False removes that grant. github_oidc_deploy_services adds further names."
  type        = bool
  default     = true
  nullable    = false
}

variable "github_oidc_deploy_services" {
  description = "Names of further ECS services in the project cluster that the deploy role may update, besides the two serving services that github_oidc_deploy_serving_enabled grants. The deploy boundary allows only <project>_<environment>_model_api and <project>_<environment>_streamlit, so any other name fails the plan."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition     = alltrue([for name in var.github_oidc_deploy_services : can(regex("^[A-Za-z0-9_-]{1,255}$", name))])
    error_message = "Use exact ECS service names, without pattern characters."
  }
}

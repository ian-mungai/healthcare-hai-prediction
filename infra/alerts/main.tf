# The alert address root. Bootstrap applies it with the project profile after the platform stack: it subscribes the one address from .env to every alert topic
# the project creates, so the owner sees run failures and cost alerts from the first run. It is a root of its own because a subscription holds the address:
# the GitHub plan role never reads it (sns:GetSubscriptionAttributes stays forbidden for CI), so no pull request plan may refresh this state. The root is
# in no CI workflow and not in the plan role's stack list.
provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }

  # The IAM stack names the budget alerts topic format(local.platform_name, "budget_alerts") in infra/iam/budget.tf and the platform stack names the alerts
  # topic the same way. The project user's SNS policy names each of the two topics by its exact ARN, so a new alert topic needs its own entry in that policy
  # (the policy E2E holds the policy to the topics Terraform creates). A topic that does not exist yet fails the apply visibly.
  budget_topic_arn = "arn:aws:sns:${var.aws_region}:${var.expected_account_id}:${var.project_name}_budget_alerts_${var.environment}"

  # Whether an address is set is not a secret, the address is. An unset or empty address leaves every topic without a subscriber.
  subscribe = nonsensitive(var.alert_email != "")
  topics    = local.subscribe ? { alerts = var.alerts_topic_arn, budget = local.budget_topic_arn } : {}
}

# Each subscription stays pending until it is confirmed; Terraform cannot confirm it. The owner confirms the alerts topic subscription from the email link. The
# administrator confirms the budget one with AuthenticateOnUnsubscribe=true (bootstrap step 10), because a subscription confirmed from the link can be deleted
# without a signed request. An existing confirmed hand subscription is adopted with the documented terraform import step, not recreated, and a budget one only when
# ConfirmationWasAuthenticated is true. The project user holds sns:Unsubscribe on the platform alerts topic only. The administrator removes the budget
# subscription at teardown or when the address changes. That the project user and an unsigned caller are both refused is not verified against AWS.
resource "aws_sns_topic_subscription" "email" {
  for_each = local.topics

  topic_arn = each.value
  protocol  = "email"
  endpoint  = var.alert_email
}

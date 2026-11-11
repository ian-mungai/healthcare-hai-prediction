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
  # topic the same way. The project user's SNS policy names each of the two topics by its exact ARN, and the platform and IAM Terraform tests assert that each
  # stack's own topic ARN is in the rendered policy. A topic that does not exist yet fails the apply visibly.
  budget_topic_arn = "arn:aws:sns:${var.aws_region}:${var.expected_account_id}:${var.project_name}_budget_alerts_${var.environment}"

  # Whether an address is set is not a secret, the address is. An unset or empty address leaves every topic without a subscriber.
  subscribe = nonsensitive(var.alert_email != "")
  topics    = local.subscribe ? { alerts = var.alerts_topic_arn, budget = local.budget_topic_arn } : {}
}

# Each subscription stays pending until the owner clicks the confirmation link in the email that AWS sends for it; Terraform cannot confirm it. The project user holds
# sns:* on both topics and their subscriptions, so the same project profile creates the two subscriptions at bootstrap and destroys them at teardown.
resource "aws_sns_topic_subscription" "email" {
  for_each = local.topics

  topic_arn = each.value
  protocol  = "email"
  endpoint  = var.alert_email
}

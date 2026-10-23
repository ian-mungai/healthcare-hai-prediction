# Seven-day retention keeps logs within the CloudWatch free allowance (owner-accepted budget design). The groups use
# CloudWatch's default encryption; a customer-managed key adds cost the budget does not cover.
#trivy:ignore:AWS-0017
resource "aws_cloudwatch_log_group" "platform" {
  #checkov:skip=CKV_AWS_338:Seven-day retention is the owner-accepted budget design; logs are not the record of any result.
  #checkov:skip=CKV_AWS_158:Log groups use CloudWatch's default encryption; a customer-managed key adds cost the budget does not cover.
  for_each = var.log_groups

  name              = format(local.name, each.key)
  retention_in_days = var.log_retention_days

  tags = local.data_classification
}

# The run-failure alerts topic. The budget has its own topic in the IAM stack, which the project profile cannot change.
# The alerts carry run names and counts, not data, and a customer-managed key adds cost.
#trivy:ignore:AWS-0095
resource "aws_sns_topic" "alerts" {
  #checkov:skip=CKV_AWS_26:Alerts hold no data; a customer-managed key adds cost the budget does not cover.
  name = format(local.name, "alerts")
}

# The budget only reports: the owner tracks spend and acts on the alerts by hand, and no action stops spending. The budget has
# its own topic, which the project user's policy does not name: it cannot change or delete the topic and so cannot silence the alerts.
# AWS Budgets cannot publish to a topic encrypted with the AWS-managed SNS key, and a customer-managed key adds cost.
# Alerts carry spend figures, not data.
#trivy:ignore:AWS-0095
resource "aws_sns_topic" "budget_alerts" {
  #checkov:skip=CKV_AWS_26:AWS Budgets cannot publish to a topic encrypted with the AWS-managed key; alerts hold no data.
  name = format(local.platform_name, "budget_alerts")
}

resource "aws_sns_topic_policy" "budget_alerts" {
  arn = aws_sns_topic.budget_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "BudgetAlerts"
        Effect    = "Allow"
        Principal = { Service = "budgets.amazonaws.com" }
        Action    = "SNS:Publish"
        Resource  = aws_sns_topic.budget_alerts.arn
        Condition = {
          StringEquals = { "aws:SourceAccount" = var.expected_account_id }
          ArnLike      = { "aws:SourceArn" = "arn:aws:budgets::${var.expected_account_id}:*" }
        }
      }
    ]
  })
}

# A quarterly budget covers the whole build from October to mid-November; a monthly one would split the amount in two.
# The filter counts only resources that carry the Project tag AND belong to the services the platform adds: separate
# filters combine with AND and the values inside one filter with OR. The data bucket carries the same Project value, so
# the service list is what keeps S3 and Secrets Manager outside the budget. The names are the Cost Explorer SERVICE
# dimension values. The tag must be an active cost allocation tag.
resource "aws_budgets_budget" "platform" {
  name         = format(local.platform_name, "platform")
  budget_type  = "COST"
  limit_amount = format("%.2f", var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "QUARTERLY"

  cost_filter {
    name   = "TagKeyValue"
    values = [format("user:Project$%s", local.platform_project_tag)]
  }

  cost_filter {
    name   = "Service"
    values = local.budget_services
  }

  dynamic "notification" {
    for_each = var.budget_alert_percentages

    content {
      comparison_operator       = "GREATER_THAN"
      threshold                 = notification.value
      threshold_type            = "PERCENTAGE"
      notification_type         = "ACTUAL"
      subscriber_sns_topic_arns = [aws_sns_topic.budget_alerts.arn]
    }
  }

  depends_on = [aws_sns_topic_policy.budget_alerts]
}

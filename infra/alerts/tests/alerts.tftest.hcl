mock_provider "aws" {
  override_during = plan
}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  alerts_topic_arn    = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
}

# The alert address comes from .env through the rendered inputs. Without it the root creates nothing, so development without alerts works.
run "alert_email_unset_creates_no_subscription" {
  command = plan

  assert {
    condition     = length(aws_sns_topic_subscription.email) == 0
    error_message = "An unset alert address creates no subscription."
  }

  assert {
    condition     = length(output.subscribed_topic_arns) == 0
    error_message = "An unset alert address subscribes no topic."
  }
}

run "alert_email_empty_creates_no_subscription" {
  command = plan

  variables {
    alert_email = ""
  }

  assert {
    condition     = length(aws_sns_topic_subscription.email) == 0
    error_message = "An empty alert address creates no subscription."
  }
}

# The project user may subscribe to every alert topic the project creates: the platform alerts topic and the IAM stack's budget alerts topic.
run "alert_email_subscribes_the_address_to_every_alert_topic" {
  command = plan

  variables {
    alert_email = "owner@example.org"
  }

  assert {
    condition     = toset(keys(aws_sns_topic_subscription.email)) == toset(["alerts", "budget"])
    error_message = "A set alert address creates one subscription on the alerts topic and one on the budget alerts topic, and no other."
  }

  assert {
    condition = (
      aws_sns_topic_subscription.email["alerts"].protocol == "email" &&
      aws_sns_topic_subscription.email["alerts"].topic_arn == "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev" &&
      aws_sns_topic_subscription.email["budget"].protocol == "email" &&
      aws_sns_topic_subscription.email["budget"].topic_arn == "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev"
    )
    error_message = "Both subscriptions use the email protocol on the topic they are named for."
  }

  assert {
    condition     = issensitive(aws_sns_topic_subscription.email["alerts"].endpoint) && issensitive(aws_sns_topic_subscription.email["budget"].endpoint)
    error_message = "The subscription endpoint is the address, so the plan marks it sensitive."
  }

  assert {
    condition     = nonsensitive(aws_sns_topic_subscription.email["alerts"].endpoint) == "owner@example.org" && nonsensitive(aws_sns_topic_subscription.email["budget"].endpoint) == "owner@example.org"
    error_message = "Each subscription endpoint is the address from the input."
  }

  assert {
    condition     = jsonencode(output.subscribed_topic_arns) == jsonencode({ alerts = var.alerts_topic_arn, budget = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev" })
    error_message = "The output names the topics that carry a subscription and shows no address."
  }
}

run "the_budget_topic_follows_the_environment_and_region" {
  command = plan

  variables {
    environment      = "prod"
    aws_profile      = "example_project_prod"
    aws_region       = "us-west-2"
    alerts_topic_arn = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_prod"
    alert_email      = "owner@example.org"
  }

  assert {
    condition     = aws_sns_topic_subscription.email["budget"].topic_arn == "arn:aws:sns:us-west-2:111111111111:example_project_budget_alerts_prod"
    error_message = "The budget alerts topic is named by the IAM stack from the project name and the environment."
  }
}

run "reject_alert_email_list" {
  command = plan

  variables {
    alert_email = "owner@example.org,other@example.org"
  }

  expect_failures = [var.alert_email]
}

run "reject_alert_email_with_display_name" {
  command = plan

  variables {
    alert_email = "Owner <owner@example.org>"
  }

  expect_failures = [var.alert_email]
}

run "reject_alert_email_without_domain_dot" {
  command = plan

  variables {
    alert_email = "owner@example"
  }

  expect_failures = [var.alert_email]
}

run "reject_alert_email_with_space" {
  command = plan

  variables {
    alert_email = " owner@example.org"
  }

  expect_failures = [var.alert_email]
}

run "reject_alert_email_that_is_too_long" {
  command = plan

  variables {
    alert_email = "${join("", [for index in range(250) : "a"])}@example.org"
  }

  expect_failures = [var.alert_email]
}

# The topic ARN comes from the platform output. A value that is not the platform alerts topic of this project, environment, region and account is refused
# before any subscription is planned.
run "reject_the_budget_topic_as_the_alerts_topic" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev"
    alert_email      = "owner@example.org"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "reject_the_alerts_topic_of_another_project" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:other_project_alerts_dev"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "reject_the_alerts_topic_of_another_environment" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_prod"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "reject_the_alerts_topic_in_another_region" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:us-east-1:111111111111:example_project_alerts_dev"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "reject_the_alerts_topic_of_another_account" {
  command = plan

  variables {
    alerts_topic_arn    = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
    expected_account_id = join("", ["222222", "222222"])
  }

  expect_failures = [var.alerts_topic_arn]
}

run "reject_a_subscription_arn_as_the_alerts_topic" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev:01234567-89ab-cdef-0123-456789abcdef"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "reject_a_missing_alerts_topic" {
  command = plan

  variables {
    alerts_topic_arn = ""
  }

  expect_failures = [var.alerts_topic_arn]
}

run "only_the_project_profile_may_run_the_root" {
  command = plan

  variables {
    aws_profile = "example_administrator"
  }

  expect_failures = [var.aws_profile]
}

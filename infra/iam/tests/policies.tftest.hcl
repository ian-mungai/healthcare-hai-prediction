variable "project_profile" {
  description = "Approved profile supplied by the credential-free CI runner."
  type        = string
}

mock_provider "aws" {}

variables {
  aws_profile          = "example_admin"
  aws_region           = "us-west-2"
  expected_account_id  = "111111111111"
  data_bucket_name     = "example-project-ci-bucket"
  deployment_user_name = "example_project_user"
  project_name         = "example_project"
}

run "scoped_project_permissions" {
  command = plan

  assert {
    condition     = length(aws_iam_policy.service) == 1 && length(aws_iam_user_policy_attachment.service) == 1
    error_message = "Only the consolidated service policy may be attached."
  }

  assert {
    condition     = alltrue([for attachment in aws_iam_user_policy_attachment.service : attachment.user == var.deployment_user_name])
    error_message = "Policy attachments must target only the existing project user."
  }

  assert {
    condition = alltrue(flatten([
      for policy in aws_iam_policy.service : [
        for statement in jsondecode(policy.policy).Statement : [
          for action in flatten([statement.Action]) : startswith(action, "s3:") && !strcontains(action, "*") && !startswith(action, "s3:Delete")
        ]
      ]
    ]))
    error_message = "Project policies must not grant IAM administration, wildcard actions or data deletion."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement :
      statement.Resource == "arn:aws:s3:::${var.data_bucket_name}"
      if statement.Sid != "CaptureAndVerifyApprovedPrefixes"
    ])
    error_message = "Deployment permissions must be scoped to exactly the approved bucket."
  }

  assert {
    condition = alltrue([
      jsondecode(aws_iam_policy.service["s3"].policy).Statement[0].Action == "s3:CreateBucket",
      jsondecode(aws_iam_policy.service["s3"].policy).Statement[0].Condition.StringEquals["s3:LocationConstraint"] == var.aws_region
    ])
    error_message = "Bucket creation must be limited to the approved region."
  }

  assert {
    condition = toset(one([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement.Resource
      if statement.Sid == "CaptureAndVerifyApprovedPrefixes"
      ])) == toset([
      for prefix in ["raw", "reference", "manifests", "audit"] : "arn:aws:s3:::${var.data_bucket_name}/${prefix}/*"
    ])
    error_message = "Object permissions must be limited to the four acquisition prefixes."
  }

  assert {
    condition     = aws_iam_policy.service["s3"].name == "${var.project_name}_s3_policy"
    error_message = "Rendered policy names must use the project_name_service_name_policy convention."
  }
}

run "reject_invalid_bucket_scope" {
  command = plan

  variables {
    data_bucket_name = "example-*"
  }

  expect_failures = [var.data_bucket_name]
}

run "reject_invalid_user" {
  command = plan

  variables {
    deployment_user_name = "arn:aws:iam::111111111111:user/example_project_user"
  }

  expect_failures = [var.deployment_user_name]
}

run "reject_project_profile_for_iam" {
  command = plan

  variables {
    aws_profile = var.project_profile
  }

  expect_failures = [var.aws_profile]
}

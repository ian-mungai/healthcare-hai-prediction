mock_provider "aws" {}

variables {
  aws_profile         = "example_project_user"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
}

run "scoped_project_permissions" {
  command = plan

  assert {
    condition     = length(aws_iam_policy.service) == 1 && length(aws_iam_user_policy_attachment.service) == 1
    error_message = "Only the consolidated service policy may be attached."
  }

  assert {
    condition     = alltrue([for attachment in aws_iam_user_policy_attachment.service : attachment.user == var.aws_profile])
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
    error_message = "Project policies must not retain temporary cleanup permissions or grant IAM administration, wildcard actions or data deletion."
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
      ])) == toset(concat(
      [for prefix in ["raw", "reference", "manifests", "audit"] : "arn:aws:s3:::${var.data_bucket_name}/${prefix}/*"],
      [for category in ["data", "references", "manifests", "audit"] : "arn:aws:s3:::${var.data_bucket_name}/*/${category}/*"],
      ["arn:aws:s3:::${var.data_bucket_name}/*/*/datasets/*"]
    ))
    error_message = "Preserve legacy access and allow only dataset-first content categories within the approved bucket."
  }

  assert {
    condition     = aws_iam_policy.service["s3"].name == "${var.project_name}_s3_policy"
    error_message = "Rendered policy names must use the project_name_service_name_policy convention."
  }

  # Guard absent lifecycle access, wildcard scope and accidental object-level permission.
  assert {
    condition = one([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement.Resource
      if contains(flatten([statement.Action]), "s3:PutLifecycleConfiguration")
    ]) == "arn:aws:s3:::${var.data_bucket_name}"
    error_message = "Lifecycle configuration permission must occur once and target only the approved bucket."
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
    aws_profile = "arn:aws:iam::111111111111:user/example_project_user"
  }

  expect_failures = [var.aws_profile]
}

run "reject_project_profile_for_iam" {
  command = plan

  variables {
    admin_profile = "example_project_user"
  }

  expect_failures = [var.admin_profile]
}

run "accept_alternate_region" {
  command = plan

  variables {
    aws_region = "ap-southeast-2"
  }
}

run "reject_empty_region" {
  command = plan

  variables {
    aws_region = ""
  }

  expect_failures = [var.aws_region]
}

run "reject_malformed_region" {
  command = plan

  variables {
    aws_region = "ap_southeast_2"
  }

  expect_failures = [var.aws_region]
}

run "reject_padded_region" {
  command = plan

  variables {
    aws_region = " eu-west-1 "
  }

  expect_failures = [var.aws_region]
}

run "reject_null_region" {
  command = plan

  variables {
    aws_region = null
  }

  expect_failures = [var.aws_region]
}

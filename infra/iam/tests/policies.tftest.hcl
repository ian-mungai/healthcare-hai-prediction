mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
}

run "scoped_project_permissions" {
  command = plan

  assert {
    condition     = toset(keys(aws_iam_policy.service)) == toset(["s3", "secrets", "lakehouse"]) && length(aws_iam_user_policy_attachment.service) == 3
    error_message = "Only the consolidated S3 policy, the shared-secret read policy and the lakehouse table policy may be attached."
  }

  # Changing a policy description forces replacement, which prevent_destroy blocks; existing descriptions stay fixed.
  assert {
    condition = alltrue([
      for name in ["s3", "secrets"] : aws_iam_policy.service[name].description == "Project-scoped HAI storage permissions; no IAM administration or data deletion."
    ])
    error_message = "Existing policy descriptions must not change, or the policies would be replaced."
  }

  # Iceberg tables need write and delete rights, but only inside lakehouse/; originals elsewhere stay write-once.
  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["lakehouse"].policy).Statement : flatten([statement.Action])
      ])) == toset([
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
    ])
    error_message = "The lakehouse policy may only read, write and delete current objects; no version deletion or wildcard actions."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["lakehouse"].policy).Statement : flatten([statement.Resource])
    ])) == toset(["arn:aws:s3:::${var.data_bucket_name}/lakehouse/*"])
    error_message = "Lakehouse permissions must be limited to the lakehouse/ folder of the approved bucket."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_policy.service["lakehouse"].policy).Statement : statement.Effect == "Allow" && !can(statement.Condition)
    ])
    error_message = "The lakehouse policy holds only unconditional allow statements for its single folder."
  }

  assert {
    condition     = aws_iam_policy.service["lakehouse"].name == "${var.project_name}_lakehouse_policy" && strcontains(aws_iam_policy.service["lakehouse"].description, "lakehouse/")
    error_message = "The lakehouse policy must follow the naming convention and describe its folder-limited deletion."
  }

  assert {
    condition     = alltrue([for attachment in aws_iam_user_policy_attachment.service : attachment.user == var.aws_profile])
    error_message = "Policy attachments must target only the existing project user."
  }

  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : [
        for action in flatten([statement.Action]) : startswith(action, "s3:") && !strcontains(action, "*") && !startswith(action, "s3:Delete")
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

  # Shared API keys are created and filled manually; the project may only read these three by name.
  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : [
        for action in flatten([statement.Action]) : contains(["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"], action)
      ]
    ]))
    error_message = "The secrets policy may only read secret values and metadata."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : flatten([statement.Resource])
      ])) == toset([
      for name in ["bls_api_key", "census_api_key", "hud_api_key"] : "arn:aws:secretsmanager:${var.aws_region}:${var.expected_account_id}:secret:${name}-??????"
    ])
    error_message = "Secret read access must be limited to the named shared API keys in the approved account and region."
  }

  assert {
    condition     = aws_iam_policy.service["secrets"].name == "${var.project_name}_secrets_policy"
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
    aws_profile = "arn:aws:iam::111111111111:user/example_project_dev"
  }

  expect_failures = [var.aws_profile]
}

run "reject_project_profile_for_iam" {
  command = plan

  variables {
    admin_profile = "example_project_dev"
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

run "reject_unsupported_environment" {
  command = plan

  variables {
    environment = "development"
  }

  expect_failures = [var.environment]
}

run "reject_legacy_user_profile" {
  command = plan

  variables {
    aws_profile = "example_project_user"
  }

  expect_failures = [var.aws_profile]
}

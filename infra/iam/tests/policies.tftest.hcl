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
      if !contains(["CaptureAndVerifyApprovedPrefixes", "ReadSingleCopyContentObjects", "CreateSingleCopyContentObjectsOnce", "DenyOlderGrantsInSingleCopyFolders", "DenyPercentSignInSingleCopyKeys"], statement.Sid)
    ])
    error_message = "Deployment permissions must be scoped to exactly the approved bucket."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : flatten([statement.Resource])
      if statement.Sid == "ReadSingleCopyContentObjects"
      ])) == toset([
      "arn:aws:s3:::${var.data_bucket_name}/bronze_objects/sha256/*", "arn:aws:s3:::${var.data_bucket_name}/bronze_evidence/sha256/*"
    ])
    error_message = "The single-copy read statement must exist and cover exactly the two content folders."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : flatten([statement.Action])
      if statement.Sid == "ReadSingleCopyContentObjects"
    ])) == toset(["s3:GetObject", "s3:GetObjectVersion"])
    error_message = "The single-copy read statement may only read current objects and exact versions."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : flatten([statement.Resource])
      if statement.Sid == "CreateSingleCopyContentObjectsOnce"
      ])) == toset([
      for folder in ["bronze_objects", "bronze_evidence"] : "arn:aws:s3:::${var.data_bucket_name}/${folder}/sha256/${replace(format("%064d", 0), "0", "?")}"
    ])
    error_message = "The single-copy create statement must exist and cover exactly one 64-character leaf under each of the two content folders."
  }

  assert {
    condition = alltrue([
      length([for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : 1 if statement.Sid == "CreateSingleCopyContentObjectsOnce"]) == 1,
      one([
        for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement
        if statement.Sid == "CreateSingleCopyContentObjectsOnce"
      ]).Action == "s3:PutObject",
      one([
        for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement
        if statement.Sid == "CreateSingleCopyContentObjectsOnce"
      ]).Condition == { Null = { "s3:if-none-match" = "false" } }
    ])
    error_message = "The single-copy create statement must allow only s3:PutObject and only with the if-none-match header."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement :
      !can(statement.Condition) || statement.Sid != "ReadSingleCopyContentObjects"
    ])
    error_message = "The single-copy read statement must not carry a write condition."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement :
      !anytrue([for resource in flatten([statement.Resource]) : strcontains(resource, "bronze_") || resource == "arn:aws:s3:::${var.data_bucket_name}/*"])
      if !contains(["ReadSingleCopyContentObjects", "CreateSingleCopyContentObjectsOnce", "DenyOlderGrantsInSingleCopyFolders", "DenyPercentSignInSingleCopyKeys"], statement.Sid)
    ])
    error_message = "No other statement may reach the content folders or the whole bucket's objects."
  }

  assert {
    condition = alltrue([
      length([for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : 1 if statement.Sid == "DenyPercentSignInSingleCopyKeys"]) == 1,
      one([
        for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement
        if statement.Sid == "DenyPercentSignInSingleCopyKeys"
      ]).Effect == "Deny",
      one([
        for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement
        if statement.Sid == "DenyPercentSignInSingleCopyKeys"
      ]).Action == "s3:PutObject",
      toset(one([
        for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement : statement
        if statement.Sid == "DenyPercentSignInSingleCopyKeys"
        ]).Resource) == toset([
        for folder in ["bronze_objects", "bronze_evidence"] : "arn:aws:s3:::${var.data_bucket_name}/${folder}/sha256/*%*"
      ])
    ])
    error_message = "The percent-sign deny must exist once and cover exactly the two content folders."
  }

  assert {
    condition = alltrue([
      for case in concat(
        flatten([
          for folder in ["bronze_objects", "bronze_evidence"] : [
            for suffix in [
              "sha256/${format("%064d", 0)}/references/x", "sha256/${format("%064d", 0)}/data/x", "sha256/${format("%064d", 0)}/audit/x",
              "sha256/${format("%064d", 0)}/manifests/x", "sha256/${format("%064d", 0)}/datasets/x", "sha256/${format("%064d", 0)}/extra",
              "sha256/${format("%064d", 0)}/a/datasets/x", "references/x", "data/x", "audit/x", "manifests/x", "other/a/datasets/x"
              ] : [
              for action in ["s3:PutObject", "s3:PutObjectTagging", "s3:PutObjectVersionTagging", "s3:AbortMultipartUpload"] : [
                for header in [true, false] : { key = "${folder}/${suffix}", action = action, header = header, allowed = false }
              ]
            ]
          ]
        ]),
        flatten([
          for folder in ["bronze_objects", "bronze_evidence"] : [
            for leaf in [
              "", "abc123", "${format("%064d", 0)}a", "${format("%063d", 0)}", "${format("%064d", 0)}%2Fx", "${format("%061d", 0)}%2F", "%${format("%063d", 0)}",
              "${format("%063d", 0)}/"
              ] : [
              for header in [true, false] : { key = "${folder}/sha256/${leaf}", action = "s3:PutObject", header = header, allowed = false }
            ]
          ]
        ]),
        flatten([
          for folder in ["bronze_objects", "bronze_evidence"] : [
            { key = "${folder}/sha256/${format("%064d", 0)}", action = "s3:PutObject", header = true, allowed = true },
            { key = "${folder}/sha256/${format("%064d", 0)}", action = "s3:PutObject", header = false, allowed = false },
            { key = "${folder}/sha256/${format("%064d", 0)}", action = "s3:GetObject", header = false, allowed = true },
            { key = "${folder}/sha256/${format("%064d", 0)}", action = "s3:GetObjectVersion", header = false, allowed = true },
            { key = "${folder}/sha256/${format("%064d", 0)}", action = "s3:PutObjectTagging", header = true, allowed = false },
            { key = "${folder}/sha256/${format("%064d", 0)}", action = "s3:AbortMultipartUpload", header = true, allowed = false },
          ]
        ]),
        [
          { key = "cms/hospitals/references/capture/x", action = "s3:PutObject", header = false, allowed = true },
          { key = "cms/hospitals/datasets/ds/x", action = "s3:PutObjectTagging", header = false, allowed = true },
          { key = "cms/hospitals/audit/run/x", action = "s3:AbortMultipartUpload", header = false, allowed = true },
        ]
      ) :
      (
        anytrue([
          for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement :
          statement.Effect == "Allow" && contains(flatten([statement.Action]), case.action) && anytrue([
            for resource in flatten([statement.Resource]) :
            can(regex("^${replace(replace(resource, "*", ".*"), "?", ".")}$", "arn:aws:s3:::${var.data_bucket_name}/${case.key}"))
          ]) && (!can(statement.Condition) || (case.header && statement.Condition == { Null = { "s3:if-none-match" = "false" } }))
        ])
        && !anytrue([
          for statement in jsondecode(aws_iam_policy.service["s3"].policy).Statement :
          statement.Effect == "Deny" && contains(flatten([statement.Action]), case.action) && anytrue([
            for resource in flatten([statement.Resource]) :
            can(regex("^${replace(replace(resource, "*", ".*"), "?", ".")}$", "arn:aws:s3:::${var.data_bucket_name}/${case.key}"))
          ])
        ])
      ) == case.allowed
    ])
    error_message = "Inside the two single-copy folders only the conditional create of an exact 64-character sha256 key may write, and no older wildcard grant may reach a key in them."
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
      for name in ["bls_api_key", "census_api_key", "hud_api_key", "jev_api_key"] : "arn:aws:secretsmanager:${var.aws_region}:${var.expected_account_id}:secret:${name}-??????"
    ])
    error_message = "Secret read access must be limited to the named shared API keys in the approved account and region."
  }

  assert {
    condition     = aws_iam_policy.service["secrets"].name == "${var.project_name}_secrets_policy"
    error_message = "Rendered policy names must use the project_name_service_name_policy convention."
  }

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

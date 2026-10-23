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
    condition     = toset(keys(aws_iam_policy.service)) == toset(["s3", "secrets", "lakehouse", "assume_roles"]) && length(aws_iam_user_policy_attachment.service) == 4
    error_message = "Only the consolidated S3 policy, the shared-secret read policy, the lakehouse table policy and the policy that assumes the two lakehouse roles may be attached."
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
    condition     = toset([for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : statement.Sid]) == toset(["ReadProjectSecrets", "ManageProjectSecrets"])
    error_message = "The secrets policy holds exactly one read statement and one manage statement."
  }

  assert {
    condition     = alltrue([for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : statement.Effect == "Allow" && !can(statement.Condition)])
    error_message = "Every secrets policy statement is an unconditional allow."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : toset(flatten([statement.Action]))
      if statement.Sid == "ReadProjectSecrets"
    ]) == toset(["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"])
    error_message = "The shared secrets stay read-only: reading secret values and metadata, nothing else."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : toset(flatten([statement.Resource]))
      if statement.Sid == "ReadProjectSecrets"
      ]) == toset([
      for name in ["census_api_key", "bls_api_key", "hud_api_key", "jev_api_key", "healthcare-hai-prediction/airflow-admin"] : "arn:aws:secretsmanager:eu-west-1:111111111111:secret:${name}-??????"
    ])
    error_message = "Secret read access must be limited to the five shared project secrets in the approved account and region."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : toset(flatten([statement.Action]))
      if statement.Sid == "ManageProjectSecrets"
      ]) == toset([
      "secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret", "secretsmanager:CreateSecret", "secretsmanager:PutSecretValue"
    ])
    error_message = "The managed secrets allow exactly reading, describing, creating and putting a value."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_policy.service["secrets"].policy).Statement : toset(flatten([statement.Resource]))
      if statement.Sid == "ManageProjectSecrets"
      ]) == toset([
      for name in [
        "healthcare-hai-prediction/postgres-password",
        "healthcare-hai-prediction/polaris-root-secret",
        "healthcare-hai-prediction/polaris-reader-client-id",
        "healthcare-hai-prediction/polaris-reader-client-secret",
        "healthcare-hai-prediction/polaris-gold-client-id",
        "healthcare-hai-prediction/polaris-gold-client-secret",
        "healthcare-hai-prediction/polaris-silver-client-id",
        "healthcare-hai-prediction/polaris-silver-client-secret",
        "healthcare-hai-prediction/airflow-db-password",
        "healthcare-hai-prediction/airflow-fernet-key",
        "healthcare-hai-prediction/airflow-jwt-secret",
        "healthcare-hai-prediction/airflow-api-secret-key",
        "healthcare-hai-prediction/marquez-db-password",
      ] : "arn:aws:secretsmanager:eu-west-1:111111111111:secret:${name}-??????"
    ])
    error_message = "Secret write access must be limited to exactly the 13 managed secrets in the approved account and region."
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

run "lakehouse_roles" {
  command = plan

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_policy.service["assume_roles"].policy).Statement : flatten([statement.Resource])
      ])) == toset([
      "arn:aws:iam::111111111111:role/healthcare_hai_prediction_lakehouse_reader",
      "arn:aws:iam::111111111111:role/healthcare_hai_prediction_lakehouse_writer",
    ])
    error_message = "The project user may assume exactly the lakehouse reader and writer roles."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_policy.service["assume_roles"].policy).Statement :
      statement.Effect == "Allow" && statement.Action == "sts:AssumeRole" && !can(statement.Condition)
    ])
    error_message = "The role policy of the project user holds only unconditional allows of sts:AssumeRole."
  }

  assert {
    condition = (
      aws_iam_policy.service["assume_roles"].name == "${var.project_name}_assume_roles_policy"
      && aws_iam_policy.service["assume_roles"].description == "Lets the project user assume the two lakehouse roles and no other role."
    )
    error_message = "The role policy follows the naming convention and states its limit."
  }

  assert {
    condition = alltrue([
      for role in [aws_iam_role.lakehouse_reader, aws_iam_role.lakehouse_writer] :
      length(jsondecode(role.assume_role_policy).Statement) == 1
      && jsondecode(role.assume_role_policy).Statement[0].Effect == "Allow"
      && jsondecode(role.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
      && jsondecode(role.assume_role_policy).Statement[0].Principal == { AWS = "arn:aws:iam::111111111111:user/example_project_dev" }
      && !can(jsondecode(role.assume_role_policy).Statement[0].Condition)
    ])
    error_message = "Each lakehouse role trusts only the project's own IAM user."
  }

  assert {
    condition     = aws_iam_role.lakehouse_reader.name == "healthcare_hai_prediction_lakehouse_reader" && aws_iam_role.lakehouse_writer.name == "healthcare_hai_prediction_lakehouse_writer"
    error_message = "The role names default to their real names."
  }

  assert {
    condition     = aws_iam_role.lakehouse_reader.max_session_duration == 7200 && aws_iam_role.lakehouse_writer.max_session_duration == 7200
    error_message = "The longest session is 7200 seconds: the longest pipeline job deadline of 3600 seconds plus the launcher's margin, with room."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_reader.policy).Statement : flatten([statement.Action])
    ])) == toset(["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"])
    error_message = "The reader holds exactly reading an object, listing the bucket by prefix and locating the bucket."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_reader.policy).Statement : toset(flatten([statement.Resource]))
      if statement.Sid == "ReadTableFiles"
      ]) == toset([
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*",
    ])
    error_message = "The reader reads only the bronze, silver and gold prefixes."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_reader.policy).Statement : toset(flatten([statement.Condition.StringLike["s3:prefix"]]))
      if statement.Sid == "ListTablePrefixes"
      ]) == toset([
      "lakehouse/bronze", "lakehouse/bronze/*", "lakehouse/silver", "lakehouse/silver/*", "lakehouse/gold", "lakehouse/gold/*",
    ])
    error_message = "The reader lists only the bronze, silver and gold prefixes."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement : toset(flatten([statement.Action]))
      if statement.Sid == "ManageWrittenTableFiles"
    ]) == toset(flatten([for statement in jsondecode(aws_iam_policy.service["lakehouse"].policy).Statement : flatten([statement.Action])]))
    error_message = "The writer holds exactly the object actions of the current lakehouse policy."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement : toset(flatten([statement.Resource]))
      if statement.Sid == "ManageWrittenTableFiles"
      ]) == toset([
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver_e2e/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold_e2e/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/catalog_backups/*",
    ])
    error_message = "The writer writes only the silver, silver test, gold, gold test and catalog backup prefixes."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement : toset(flatten([statement.Condition.StringLike["s3:prefix"]]))
      if statement.Sid == "ListWrittenPrefixes"
      ]) == toset([
      "lakehouse/silver", "lakehouse/silver/*", "lakehouse/silver_e2e", "lakehouse/silver_e2e/*", "lakehouse/gold", "lakehouse/gold/*",
      "lakehouse/gold_e2e", "lakehouse/gold_e2e/*", "lakehouse/catalog_backups", "lakehouse/catalog_backups/*",
    ])
    error_message = "The writer lists only its own prefixes."
  }

  assert {
    condition = alltrue(flatten([
      for policy in [aws_iam_role_policy.lakehouse_reader.policy, aws_iam_role_policy.lakehouse_writer.policy] : [
        for statement in jsondecode(policy).Statement : [
          for action in flatten([statement.Action]) : (
            statement.Effect == "Allow"
            && startswith(action, "s3:")
            && !strcontains(action, "*")
            && contains(flatten([
              for existing in [aws_iam_policy.service["lakehouse"].policy, aws_iam_policy.service["s3"].policy] : [
                for known in jsondecode(existing).Statement : flatten([known.Action])
              ]
            ]), action)
          )
        ]
      ]
    ]))
    error_message = "Each role action is an allowed S3 action that the current project policies already grant, with no wildcard."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement :
      !contains(flatten([statement.Resource]), "arn:aws:s3:::example-project-ci-bucket/lakehouse/*")
    ])
    error_message = "The writer never gets the whole lakehouse folder."
  }
}

run "reject_equal_lakehouse_role_names" {
  command = plan

  variables {
    lakehouse_writer_role_name = "healthcare_hai_prediction_lakehouse_reader"
  }

  expect_failures = [aws_iam_role.lakehouse_writer]
}

run "reject_empty_lakehouse_reader_role_name" {
  command = plan

  variables {
    lakehouse_reader_role_name = ""
  }

  expect_failures = [var.lakehouse_reader_role_name]
}

run "reject_spaced_lakehouse_writer_role_name" {
  command = plan

  variables {
    lakehouse_writer_role_name = "lakehouse writer"
  }

  expect_failures = [var.lakehouse_writer_role_name]
}

run "reject_overlong_lakehouse_writer_role_name" {
  command = plan

  variables {
    lakehouse_writer_role_name = join("", [for index in range(65) : "a"])
  }

  expect_failures = [var.lakehouse_writer_role_name]
}

run "reject_short_lakehouse_session" {
  command = plan

  variables {
    lakehouse_session_max_seconds = 3599
  }

  expect_failures = [var.lakehouse_session_max_seconds]
}

run "reject_long_lakehouse_session" {
  command = plan

  variables {
    lakehouse_session_max_seconds = 43201
  }

  expect_failures = [var.lakehouse_session_max_seconds]
}

run "reject_fractional_lakehouse_session" {
  command = plan

  variables {
    lakehouse_session_max_seconds = 7200.5
  }

  expect_failures = [var.lakehouse_session_max_seconds]
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

run "reject_empty_secret_name" {
  command = plan

  variables {
    hud_secret_name = ""
  }

  expect_failures = [var.hud_secret_name]
}

run "reject_wildcard_secret_name" {
  command = plan

  variables {
    bls_secret_name = "example*"
  }

  expect_failures = [var.bls_secret_name]
}

run "reject_single_character_wildcard_secret_name" {
  command = plan

  variables {
    jev_secret_name = "example?key"
  }

  expect_failures = [var.jev_secret_name]
}

run "reject_secret_name_with_space" {
  command = plan

  variables {
    census_secret_name = "example key"
  }

  expect_failures = [var.census_secret_name]
}

run "reject_equal_secret_names" {
  command = plan

  variables {
    bls_secret_name = "census_api_key"
  }

  expect_failures = [aws_iam_policy.service]
}

run "reject_overlong_secret_name" {
  command = plan

  variables {
    hud_secret_name = join("", [for index in range(513) : "a"])
  }

  expect_failures = [var.hud_secret_name]
}

run "reject_empty_postgres_password_name" {
  command = plan

  variables {
    postgres_password_name = ""
  }

  expect_failures = [var.postgres_password_name]
}

run "reject_wildcard_polaris_root_secret_name" {
  command = plan

  variables {
    polaris_root_secret_name = "example*"
  }

  expect_failures = [var.polaris_root_secret_name]
}

run "reject_question_polaris_reader_client_id_name" {
  command = plan

  variables {
    polaris_reader_client_id_name = "example?key"
  }

  expect_failures = [var.polaris_reader_client_id_name]
}

run "reject_space_polaris_reader_client_secret_name" {
  command = plan

  variables {
    polaris_reader_client_secret_name = "example key"
  }

  expect_failures = [var.polaris_reader_client_secret_name]
}

run "reject_overlong_polaris_gold_client_id_name" {
  command = plan

  variables {
    polaris_gold_client_id_name = join("", [for index in range(513) : "a"])
  }

  expect_failures = [var.polaris_gold_client_id_name]
}

run "reject_empty_polaris_gold_client_secret_name" {
  command = plan

  variables {
    polaris_gold_client_secret_name = ""
  }

  expect_failures = [var.polaris_gold_client_secret_name]
}

run "reject_wildcard_polaris_silver_client_id_name" {
  command = plan

  variables {
    polaris_silver_client_id_name = "example*"
  }

  expect_failures = [var.polaris_silver_client_id_name]
}

run "reject_question_polaris_silver_client_secret_name" {
  command = plan

  variables {
    polaris_silver_client_secret_name = "example?key"
  }

  expect_failures = [var.polaris_silver_client_secret_name]
}

run "reject_space_airflow_db_password_name" {
  command = plan

  variables {
    airflow_db_password_name = "example key"
  }

  expect_failures = [var.airflow_db_password_name]
}

run "reject_overlong_airflow_fernet_key_name" {
  command = plan

  variables {
    airflow_fernet_key_name = join("", [for index in range(513) : "a"])
  }

  expect_failures = [var.airflow_fernet_key_name]
}

run "reject_empty_airflow_jwt_secret_name" {
  command = plan

  variables {
    airflow_jwt_secret_name = ""
  }

  expect_failures = [var.airflow_jwt_secret_name]
}

run "reject_wildcard_airflow_api_secret_key_name" {
  command = plan

  variables {
    airflow_api_secret_key_name = "example*"
  }

  expect_failures = [var.airflow_api_secret_key_name]
}

run "reject_question_marquez_db_password_name" {
  command = plan

  variables {
    marquez_db_password_name = "example?key"
  }

  expect_failures = [var.marquez_db_password_name]
}

run "reject_equal_managed_secret_names" {
  command = plan

  variables {
    airflow_jwt_secret_name = "healthcare-hai-prediction/airflow-api-secret-key"
  }

  expect_failures = [aws_iam_policy.service]
}

run "reject_managed_name_equal_to_shared_name" {
  command = plan

  variables {
    postgres_password_name = "healthcare-hai-prediction/airflow-admin"
  }

  expect_failures = [aws_iam_policy.service]
}

mock_provider "aws" {
  override_during = plan

  mock_resource "aws_s3_bucket" {
    defaults = {
      id  = "example-project-ci-bucket"
      arn = "arn:aws:s3:::example-project-ci-bucket"
    }
  }
}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
}

run "private_versioned_storage" {
  command = plan

  assert {
    condition     = aws_s3_bucket.data.bucket == var.data_bucket_name && !aws_s3_bucket.data.force_destroy
    error_message = "The approved bucket must not allow forced data destruction."
  }

  assert {
    condition     = aws_s3_bucket.data.tags["DataClassification"] == "Public"
    error_message = "The data bucket must carry its own DataClassification tag of Public (decision 158), set on the resource rather than in default_tags."
  }

  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.data.block_public_acls,
      aws_s3_bucket_public_access_block.data.block_public_policy,
      aws_s3_bucket_public_access_block.data.ignore_public_acls,
      aws_s3_bucket_public_access_block.data.restrict_public_buckets
    ])
    error_message = "All four public-access blocks must remain enabled."
  }

  assert {
    condition     = one(aws_s3_bucket_ownership_controls.data.rule).object_ownership == "BucketOwnerEnforced"
    error_message = "The bucket must own its objects with ACLs disabled."
  }

  assert {
    condition     = one(aws_s3_bucket_versioning.data.versioning_configuration).status == "Enabled"
    error_message = "Versioning must remain enabled."
  }

  assert {
    condition     = one(one(aws_s3_bucket_server_side_encryption_configuration.data.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"
    error_message = "Default encryption must remain enabled."
  }

  assert {
    condition = alltrue([
      jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Effect == "Deny",
      jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Principal == "*",
      jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Action == "s3:*",
      jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false"
    ])
    error_message = "The bucket policy must deny unencrypted transport for every principal and operation."
  }

  assert {
    condition     = toset(keys(output.acquisition_prefixes)) == toset(["raw", "reference", "manifests", "audit"])
    error_message = "Legacy evidence prefixes must remain available during the additive layout transition."
  }

  assert {
    condition = tomap(output.dataset_prefixes) == tomap({
      for category in ["data", "references", "manifests", "audit"] :
      category => "s3://${var.data_bucket_name}/{dataset_id}/${category}/"
    })
    error_message = "Every dataset must have its own data, references, manifests and audit paths."
  }

  assert {
    condition = tomap(output.collection_prefixes) == tomap({
      for category in ["datasets", "references", "manifests", "audit"] :
      category => "s3://${var.data_bucket_name}/{publisher}/{collection}/${category}/"
    })
    error_message = "All collections must share the publisher/collection layout without duplicating reference documents per table."
  }

  assert {
    condition = toset(jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Resource) == toset([
      "arn:aws:s3:::${var.data_bucket_name}", "arn:aws:s3:::${var.data_bucket_name}/*"
    ])
    error_message = "The TLS-only policy must protect both the approved bucket and its objects."
  }
}

# Preparation only: mocked plans cannot verify AWS scheduling, permissions or existing remote rules.
# Guard missing/duplicate/disabled rules, wrong bucket/delay/filter and accidental object expiry or transition.
# Replaced Iceberg file versions under lakehouse/ expire after 30 days; nothing else ever does.
# The provider computes the bucket-wide filter prefix and the expiration days and date only at apply, so a mocked plan
# cannot read them; the saved real plan is checked for them before any apply.
run "lifecycle_cleanup_rules" {
  command = plan

  assert {
    condition = (
      aws_s3_bucket_lifecycle_configuration.data.bucket == aws_s3_bucket.data.id &&
      length(aws_s3_bucket_lifecycle_configuration.data.rule) == 3 &&
      alltrue([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.status == "Enabled"]) &&
      toset([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.id]) == toset(["abort_incomplete_uploads", "expire_replaced_lakehouse_versions", "expire_replaced_dvc_versions"])
    )
    error_message = "A default plan must hold exactly three enabled rules on the existing data bucket: incomplete-upload cleanup, replaced lakehouse versions and replaced DVC versions. The Athena results rule and the MLflow rule stay out until their flags are set."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      one(rule.abort_incomplete_multipart_upload).days_after_initiation == 7 &&
      length(rule.expiration) == 0 &&
      length(rule.noncurrent_version_expiration) == 0
      if rule.id == "abort_incomplete_uploads"
    ])
    error_message = "The bucket-wide rule may only abort incomplete uploads after seven days; it never expires objects, versions or delete markers."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      one(rule.filter).prefix == "lakehouse/" &&
      one(rule.noncurrent_version_expiration).noncurrent_days == 30 &&
      one(rule.expiration).expired_object_delete_marker == true &&
      length(rule.abort_incomplete_multipart_upload) == 0
      if rule.id == "expire_replaced_lakehouse_versions"
    ])
    error_message = "Only replaced versions under lakehouse/ may expire, after 30 days; current lakehouse files never expire and orphaned delete markers are removed."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      one(rule.filter).prefix == "dvc/" &&
      one(rule.noncurrent_version_expiration).noncurrent_days == 30 &&
      one(rule.expiration).expired_object_delete_marker == true &&
      length(rule.abort_incomplete_multipart_upload) == 0
      if rule.id == "expire_replaced_dvc_versions"
    ])
    error_message = "Only replaced versions under dvc/ may expire, after 30 days; current DVC objects never expire and orphaned delete markers are removed."
  }

  # Which rule can expire which key. The bucket-wide upload rule never expires anything, so it is left out: the provider computes its filter at apply.
  assert {
    condition = alltrue([
      for key, expected in {
        "athena/results/abc.csv"                     = ""
        "athena/results/2026/10/30/abc.csv.metadata" = ""
        "athena/results_old/abc.csv"                 = ""
        "athena/other.csv"                           = ""
        "athena/"                                    = ""
        "lakehouse/gold/dim_state/data/a.parquet"    = "expire_replaced_lakehouse_versions"
        "dvc/ab/cdef"                                = "expire_replaced_dvc_versions"
        "raw/a.csv"                                  = ""
        "reference/a.csv"                            = ""
        "manifests/m.json"                           = ""
        "audit/a.json"                               = ""
        "other.txt"                                  = ""
        } : join(",", [
          for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.id
          if rule.id != "abort_incomplete_uploads" && startswith(key, one(rule.filter).prefix)
      ]) == expected
    ])
    error_message = "With no flag set, no rule reaches the athena/results/ prefix, and each other rule reaches its own prefix only."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      length(rule.transition) == 0 &&
      length(rule.noncurrent_version_transition) == 0 &&
      length(rule.filter) == 1 &&
      length(one(rule.filter).and) == 0 &&
      length(one(rule.filter).tag) == 0
    ])
    error_message = "Rules must never transition objects and must use a single filter without tag or compound restrictions."
  }
}

# The MLflow artifact rule is a live change to the bucket's lifecycle configuration, so it exists only when the owner turns the flag on.
run "mlflow_retention_rule_only_with_its_flag" {
  command = plan

  variables {
    mlflow_retention_enabled = true
  }

  assert {
    condition = (
      length(aws_s3_bucket_lifecycle_configuration.data.rule) == 4 &&
      alltrue([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.status == "Enabled"]) &&
      toset([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.id]) == toset(["abort_incomplete_uploads", "expire_replaced_lakehouse_versions", "expire_replaced_dvc_versions", "expire_replaced_mlflow_versions"])
    )
    error_message = "With mlflow_retention_enabled the plan must add exactly the MLflow rule to the three default rules."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      one(rule.filter).prefix == "mlflow/" &&
      one(rule.noncurrent_version_expiration).noncurrent_days == 30 &&
      one(rule.expiration).expired_object_delete_marker == true &&
      length(rule.abort_incomplete_multipart_upload) == 0
      if rule.id == "expire_replaced_mlflow_versions"
    ])
    error_message = "Only replaced versions under mlflow/ may expire, after 30 days; current MLflow artifacts never expire and orphaned delete markers are removed."
  }
}

# The Athena results rule is a live change to the bucket's lifecycle configuration, so it exists only when the owner turns the flag on, like the MLflow rule.
run "athena_results_rule_only_with_its_flag" {
  command = plan

  variables {
    athena_retention_enabled = true
  }

  assert {
    condition = (
      length(aws_s3_bucket_lifecycle_configuration.data.rule) == 4 &&
      alltrue([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.status == "Enabled"]) &&
      toset([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.id]) == toset(["abort_incomplete_uploads", "expire_replaced_lakehouse_versions", "expire_replaced_dvc_versions", "expire_athena_results"])
    )
    error_message = "With athena_retention_enabled the plan must add exactly the Athena results rule to the three default rules."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      one(rule.filter).prefix == "athena/results/" &&
      one(rule.expiration).days == 7 &&
      one(rule.noncurrent_version_expiration).noncurrent_days == 1 &&
      length(rule.abort_incomplete_multipart_upload) == 0
      if rule.id == "expire_athena_results"
    ])
    error_message = "Athena query results expire after seven days and their replaced versions after one day, under athena/results/ only."
  }

  assert {
    condition = alltrue([
      for key, expected in {
        "athena/results/abc.csv"                     = "expire_athena_results"
        "athena/results/2026/10/30/abc.csv.metadata" = "expire_athena_results"
        "athena/results_old/abc.csv"                 = ""
        "athena/other.csv"                           = ""
        "athena/"                                    = ""
        "lakehouse/gold/dim_state/data/a.parquet"    = "expire_replaced_lakehouse_versions"
        "dvc/ab/cdef"                                = "expire_replaced_dvc_versions"
        "other.txt"                                  = ""
        } : join(",", [
          for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.id
          if rule.id != "abort_incomplete_uploads" && startswith(key, one(rule.filter).prefix)
      ]) == expected
    ])
    error_message = "The Athena results rule reaches keys under athena/results/ only, and no other rule reaches that prefix."
  }
}

run "both_retention_flags_add_both_rules" {
  command = plan

  variables {
    athena_retention_enabled = true
    mlflow_retention_enabled = true
  }

  assert {
    condition     = length(aws_s3_bucket_lifecycle_configuration.data.rule) == 5
    error_message = "Both flags add their rule beside the three default rules and nothing else."
  }
}

run "reject_administrator_profile" {
  command = plan

  variables {
    aws_profile = "example_admin"
  }

  expect_failures = [var.aws_profile]
}

run "reject_invalid_bucket" {
  command = plan

  variables {
    data_bucket_name = "invalid_bucket"
  }

  expect_failures = [var.data_bucket_name]
}

run "reject_invalid_account" {
  command = plan

  variables {
    expected_account_id = "invalid"
  }

  expect_failures = [var.expected_account_id]
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

# Terraform state of the stacks lives in this bucket under terraform_state/<stack>/. Everything below is off by default.
run "state_prefix_is_off_by_default" {
  command = plan

  assert {
    condition = (
      length(aws_s3_bucket_lifecycle_configuration.data.rule) == 3 &&
      length(jsondecode(aws_s3_bucket_policy.tls.policy).Statement) == 1
    )
    error_message = "With the flag off the lifecycle rules and the bucket policy must stay as they are."
  }
}

run "state_prefix_rule_expires_replaced_versions_only" {
  command = plan

  variables {
    terraform_state_enabled          = true
    terraform_state_admin_principals = ["role/example_admin"]
  }

  assert {
    condition = (
      length(aws_s3_bucket_lifecycle_configuration.data.rule) == 4 &&
      alltrue([
        for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
        one(rule.filter).prefix == "terraform_state/" &&
        one(rule.noncurrent_version_expiration).noncurrent_days == 90 &&
        one(rule.expiration).expired_object_delete_marker == true &&
        length(rule.abort_incomplete_multipart_upload) == 0
        if rule.id == "expire_replaced_state_versions"
      ])
    )
    error_message = "Replaced state versions expire after 90 days under terraform_state/ only. Current state objects never expire."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      (length(rule.expiration) == 0 || one(rule.expiration).expired_object_delete_marker == true) &&
      (length(rule.noncurrent_version_expiration) == 0 || one(rule.filter).prefix != "")
      if rule.id != "expire_replaced_state_versions"
    ])
    error_message = "No other rule may expire a current object, and no other rule may expire versions of the whole bucket."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.data.rule :
      !startswith("terraform_state/", one(rule.filter).prefix) || rule.id == "expire_replaced_state_versions" || one(rule.filter).prefix == "terraform_state/"
      if rule.id != "abort_incomplete_uploads"
    ])
    error_message = "No rule except the state rule may cover the state prefix."
  }
}

run "state_keys_belong_to_the_project_user_and_ci_only" {
  command = plan

  variables {
    terraform_state_enabled          = true
    terraform_state_admin_principals = ["role/example_admin"]
  }

  assert {
    condition = (
      jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Sid == "DenyInsecureTransport" &&
      alltrue([
        for statement in jsondecode(aws_s3_bucket_policy.tls.policy).Statement :
        join(",", sort(tolist(statement.Resource))) == join(",", sort([
          "arn:aws:s3:::example-project-ci-bucket/terraform_state/jobs/terraform.tfstate",
          "arn:aws:s3:::example-project-ci-bucket/terraform_state/platform/terraform.tfstate",
          "arn:aws:s3:::example-project-ci-bucket/terraform_state/services/terraform.tfstate",
          "arn:aws:s3:::example-project-ci-bucket/terraform_state/storage/terraform.tfstate"
        ]))
        if statement.Sid == "ProjectUserUsesItsStateKeys"
      ]) &&
      alltrue([
        for statement in jsondecode(aws_s3_bucket_policy.tls.policy).Statement :
        statement.Principal.AWS == "arn:aws:iam::111111111111:user/example_project_dev"
        if statement.Effect == "Allow"
      ])
    )
    error_message = "The project user may use the storage, platform, services and jobs state keys and no other key."
  }

  assert {
    condition = (
      !strcontains(aws_s3_bucket_policy.tls.policy, "terraform_state/iam/terraform.tfstate\"") ||
      length([for s in jsondecode(aws_s3_bucket_policy.tls.policy).Statement : s if s.Effect == "Allow" && strcontains(jsonencode(s.Resource), "terraform_state/iam/")]) == 0
    )
    error_message = "No allow may name the iam stack's state."
  }
}

run "every_other_principal_is_denied_the_state_prefix" {
  command = plan

  variables {
    terraform_state_enabled          = true
    terraform_state_admin_principals = ["role/example_admin"]
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_s3_bucket_policy.tls.policy).Statement :
      statement.Sid == "DenyStatePrefixToEveryoneElse" &&
      statement.Effect == "Deny" &&
      statement.Principal == "*" &&
      statement.Action == "s3:*" &&
      join(",", tolist(statement.Resource)) == "arn:aws:s3:::example-project-ci-bucket/terraform_state/*" &&
      join(",", sort(tolist(statement.Condition.ArnNotLike["aws:PrincipalArn"]))) == join(",", sort([
        "arn:aws:iam::111111111111:user/example_project_dev",
        "arn:aws:iam::111111111111:role/example_project_gh_terraform_plan_dev",
        "arn:aws:iam::111111111111:role/example_admin"
      ]))
    ])
    error_message = "Only the project user, the Terraform plan role and the named administrator may touch terraform_state/. Every lakehouse, job, feature and Athena principal is denied by name-free exclusion."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_s3_bucket_policy.tls.policy).Statement :
      statement.Sid == "DenyIamStateToProjectIdentities" &&
      statement.Effect == "Deny" &&
      join(",", tolist(statement.Resource)) == "arn:aws:s3:::example-project-ci-bucket/terraform_state/iam/*" &&
      length(statement.Condition.ArnLike["aws:PrincipalArn"]) == 2
    ])
    error_message = "The project user and the plan role must be denied the iam stack's state."
  }
}

run "state_needs_a_named_administrator" {
  command = plan

  variables {
    terraform_state_enabled = true
  }

  expect_failures = [aws_s3_bucket_policy.tls]
}

run "access_logging_is_off_by_default" {
  command = plan

  assert {
    condition = (
      length(aws_s3_bucket.logs) == 0 &&
      length(aws_s3_bucket_logging.data) == 0 &&
      length(aws_s3_bucket_policy.logs) == 0
    )
    error_message = "With the flag off the stack must plan no logging bucket and no logging setting on the project bucket."
  }
}

run "logging_bucket_is_private_encrypted_tls_only_and_expires_logs" {
  command = plan

  variables {
    access_logging_enabled           = true
    access_logging_reader_principals = ["role/example_admin"]
  }

  assert {
    condition = (
      aws_s3_bucket.logs[0].bucket == "example-project-ci-bucket-logs" &&
      aws_s3_bucket.logs[0].force_destroy == false &&
      aws_s3_bucket.logs[0].tags["DataClassification"] == "Confidential" &&
      aws_s3_bucket_public_access_block.logs[0].block_public_acls &&
      aws_s3_bucket_public_access_block.logs[0].block_public_policy &&
      aws_s3_bucket_public_access_block.logs[0].ignore_public_acls &&
      aws_s3_bucket_public_access_block.logs[0].restrict_public_buckets &&
      one(one(aws_s3_bucket_server_side_encryption_configuration.logs[0].rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256" &&
      one(aws_s3_bucket_ownership_controls.logs[0].rule).object_ownership == "BucketOwnerEnforced"
    )
    error_message = "The logging bucket must be private, encrypted with SSE-S3 (the only kind a log destination supports) and own its objects."
  }

  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.logs[0].rule :
      one(rule.expiration).days == 30 && length(rule.noncurrent_version_expiration) == 0 && one(rule.filter).prefix == "data_bucket_logs/"
      if rule.id == "expire_access_logs"
    ]) && length(aws_s3_bucket_lifecycle_configuration.logs[0].rule) == 2
    error_message = "Access logs expire 30 days after delivery, and incomplete uploads are aborted."
  }

  assert {
    condition = (
      aws_s3_bucket_logging.data[0].target_prefix == "data_bucket_logs/" &&
      aws_s3_bucket_logging.data[0].bucket == aws_s3_bucket.data.id
    )
    error_message = "The project bucket must deliver its access logs to the logging bucket under data_bucket_logs/."
  }
}

run "log_delivery_is_scoped_to_the_project_bucket" {
  command = plan

  variables {
    access_logging_enabled           = true
    access_logging_reader_principals = ["role/example_admin"]
  }

  override_resource {
    target          = aws_s3_bucket.logs[0]
    override_during = plan
    values = {
      arn = "arn:aws:s3:::example-project-ci-bucket-logs"
    }
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_s3_bucket_policy.logs[0].policy).Statement :
      statement.Sid == "AllowLogDeliveryFromTheProjectBucket" &&
      statement.Effect == "Allow" &&
      statement.Principal.Service == "logging.s3.amazonaws.com" &&
      statement.Action == "s3:PutObject" &&
      statement.Resource == "arn:aws:s3:::example-project-ci-bucket-logs/data_bucket_logs/*" &&
      statement.Condition.ArnLike["aws:SourceArn"] == "arn:aws:s3:::example-project-ci-bucket" &&
      statement.Condition.StringEquals["aws:SourceAccount"] == "111111111111"
    ])
    error_message = "The log delivery service may write under data_bucket_logs/ only for the project bucket of this account."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_s3_bucket_policy.logs[0].policy).Statement :
      statement.Sid == "DenyInsecureTransport" && statement.Effect == "Deny" && statement.Condition.Bool["aws:SecureTransport"] == "false"
    ])
    error_message = "The logging bucket must refuse plain HTTP."
  }
}

run "only_the_owner_admin_path_reads_the_logs" {
  command = plan

  variables {
    access_logging_enabled           = true
    access_logging_reader_principals = ["role/example_admin"]
  }

  override_resource {
    target          = aws_s3_bucket.logs[0]
    override_during = plan
    values = {
      arn = "arn:aws:s3:::example-project-ci-bucket-logs"
    }
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_s3_bucket_policy.logs[0].policy).Statement :
      statement.Sid == "DenyLogReadsToEveryoneElse" &&
      statement.Effect == "Deny" &&
      statement.Principal == "*" &&
      join(",", sort(tolist(statement.Action))) == "s3:GetObject,s3:GetObjectAttributes,s3:GetObjectTagging,s3:GetObjectVersion" &&
      join(",", tolist(statement.Condition.ArnNotLike["aws:PrincipalArn"])) == "arn:aws:iam::111111111111:role/example_admin"
    ])
    error_message = "Every principal except the named administrator path must be denied reading and listing the logs."
  }
}

run "logging_needs_a_named_reader" {
  command = plan

  variables {
    access_logging_enabled = true
  }

  expect_failures = [aws_s3_bucket_policy.logs]
}

run "logging_bucket_name_must_fit" {
  command = plan

  variables {
    access_logging_enabled           = true
    access_logging_reader_principals = ["role/example_admin"]
    data_bucket_name                 = "example-project-ci-bucket-name-that-is-already-long-for-a-su"
  }

  expect_failures = [aws_s3_bucket.logs]
}

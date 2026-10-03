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
    condition     = aws_s3_bucket.data.tags["DataClassification"] == "Confidential"
    error_message = "The data bucket must carry its own DataClassification tag of Confidential, set on the resource rather than in default_tags."
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
# Owner decision, Oct 2 2026: replaced Iceberg file versions under lakehouse/ expire after 30 days; nothing else ever does.
# The provider computes the bucket-wide filter prefix and the expiration days and date only at apply, so a mocked plan
# cannot read them; the saved real plan is checked for them before any apply.
run "lifecycle_cleanup_rules" {
  command = plan

  assert {
    condition = (
      aws_s3_bucket_lifecycle_configuration.data.bucket == aws_s3_bucket.data.id &&
      length(aws_s3_bucket_lifecycle_configuration.data.rule) == 2 &&
      alltrue([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.status == "Enabled"]) &&
      toset([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.id]) == toset(["abort_incomplete_uploads", "expire_replaced_lakehouse_versions"])
    )
    error_message = "Exactly two enabled rules must target the existing data bucket: incomplete-upload cleanup and replaced lakehouse versions."
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
      length(rule.transition) == 0 &&
      length(rule.noncurrent_version_transition) == 0 &&
      length(rule.filter) == 1 &&
      length(one(rule.filter).and) == 0 &&
      length(one(rule.filter).tag) == 0
    ])
    error_message = "Rules must never transition objects and must use a single filter without tag or compound restrictions."
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

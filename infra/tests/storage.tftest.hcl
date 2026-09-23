variable "project_profile" {
  description = "Approved profile supplied by the credential-free CI runner."
  type        = string
}

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
  aws_profile         = var.project_profile
  aws_region          = "us-west-2"
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
    error_message = "Acquisition, references, receipts and audit evidence must retain separate prefixes."
  }

  assert {
    condition = toset(jsondecode(aws_s3_bucket_policy.tls.policy).Statement[0].Resource) == toset([
      "arn:aws:s3:::${var.data_bucket_name}", "arn:aws:s3:::${var.data_bucket_name}/*"
    ])
    error_message = "The TLS-only policy must protect both the approved bucket and its objects."
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

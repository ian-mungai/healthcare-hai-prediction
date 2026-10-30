mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = {
      insecure_value = "ami-0123456789abcdef0"
    }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      locations = ["us-west-2a", "us-west-2b", "us-west-2c"]
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:us-west-2:111111111111:example_project_budget_alerts_dev"
    }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = {
      arn = "arn:aws:autoscaling:us-west-2:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev"
    }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values = {
    id             = "lt-0123456789abcdef0"
    latest_version = 1
  }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values = {
    arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev"
  }
}

variables {
  aws_region          = "us-west-2"
  admin_profile       = "example_admin"
  aws_profile         = "example_project_dev"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-data-bucket"
}

run "bronze_writer_starts_unprepared" {
  command = plan

  assert {
    condition     = length(aws_iam_role.bronze_writer) == 0 && output.bronze_writer_contract.production_layer == "off"
    error_message = "The candidate must not prepare a role or activate bronze by default."
  }
}

run "bronze_preparation_needs_live_locations" {
  command = plan

  variables {
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
  }

  expect_failures = [aws_iam_role.bronze_writer]
}

run "bronze_prepared_role_and_pair" {
  command = plan

  variables {
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-data-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-data-bucket/lakehouse/bronze_dictionary/"
    }
  }

  assert {
    condition     = aws_iam_role.bronze_writer[0].name == "example_project_job_write_bronze_task_dev"
    error_message = "The role must be the separate bronze task identity."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[0].Resource == [
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_dictionary/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/smoke_check/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/duckdb_view_check/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_dictionary_e2e/*"
      ] &&
      output.bronze_writer_contract.principal == "bronze_writer" &&
      length(output.bronze_execution_delta.Statement[0].Resource) == 2 &&
      output.bronze_project_delta.Statement[0].Action == ["iam:PassRole"] &&
      output.bronze_writer_contract.production_layer == "off"
    )
    error_message = "Preparation must preserve exact storage scope, the separate pair and the inactive layer."
  }
}

run "bronze_qualification_cannot_read_an_unmapped_collection" {
  command = plan

  variables {
    bronze_writer_prepared          = true
    bronze_qualification_collection = "example/not_mapped"
    bronze_live_locations = {
      bronze            = "s3://example-data-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-data-bucket/lakehouse/bronze_dictionary/"
    }
  }

  expect_failures = [aws_iam_role.bronze_writer]
}


run "bronze_rendered_storage_scope" {
  command = plan

  variables {
    job_writer_e2e_prepared         = true
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-data-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-data-bucket/lakehouse/bronze_dictionary/"
    }
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[0].Action) == toset([
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ]) &&
      length(jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement) == 4
    )
    error_message = "The rendered writer policy must omit table version reads and bucket-location access."
  }

  assert {
    condition = toset(jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[1].Condition.StringLike["s3:prefix"]) == toset([
      "lakehouse/bronze/*", "lakehouse/bronze_dictionary/*", "lakehouse/smoke_check/*", "lakehouse/duckdb_view_check/*",
      "lakehouse/bronze_e2e/*", "lakehouse/bronze_dictionary_e2e/*"
    ])
    error_message = "The rendered table list must contain the four canonical prefixes and the two bronze test twins, and no other test namespace."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[2].Resource) == toset(concat([
        for collection in distinct([for table in jsondecode(file("../../config/lakehouse/bronze_tables.json")).tables : table.collection]) :
        "arn:aws:s3:::example-data-bucket/${collection}/*"
      ], ["arn:aws:s3:::example-data-bucket/bronze_objects/sha256/*"])) &&
      jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[2].Action == ["s3:GetObjectVersion"]
    )
    error_message = "The rendered source objects must match every mapped collection and the content-addressed payload prefix of the single-copy collector, and permit only version reads."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[3].Condition.StringLike["s3:prefix"]) == toset([
        for collection in distinct([for table in jsondecode(file("../../config/lakehouse/bronze_tables.json")).tables : table.collection]) : "${collection}/*"
      ]) &&
      jsondecode(aws_iam_role_policy.bronze_writer[0].policy).Statement[3].Action == ["s3:ListBucketVersions"]
    )
    error_message = "The rendered source list must match every mapped collection with its trailing wildcard."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[0].Resource) == toset([
        "arn:aws:s3:::example-data-bucket/lakehouse/silver_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/gold_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/features_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_dictionary_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/smoke_check_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[1].Condition.StringLike["s3:prefix"]) == toset([
        "lakehouse/silver_e2e/*", "lakehouse/gold_e2e/*", "lakehouse/features_e2e/*",
        "lakehouse/bronze_e2e/*", "lakehouse/bronze_dictionary_e2e/*", "lakehouse/smoke_check_e2e/*", "lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[2].Resource == ["arn:aws:s3:::example-data-bucket/cms/hospitals/*"] &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[3].Condition.StringLike["s3:prefix"] == ["cms/hospitals/*"]
    )
    error_message = "The attached qualification policy must contain seven twins and one source collection."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[2].Resource) == toset([
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/bronze_dictionary_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/smoke_check_e2e/*",
        "arn:aws:s3:::example-data-bucket/lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[3].Condition.StringLike["s3:prefix"]) == toset([
        "lakehouse/bronze_e2e/*", "lakehouse/bronze_dictionary_e2e/*", "lakehouse/smoke_check_e2e/*", "lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[2].Action == ["s3:GetObjectVersion"] &&
      jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[3].Action == ["s3:ListBucketVersions"]
    )
    error_message = "The attached reader policy must grant version reads and version listings on exactly four bronze twins."
  }
}

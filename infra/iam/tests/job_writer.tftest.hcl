mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["eu-west-1a", "eu-west-1b"] }
  }

  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev" }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = { arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev" }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values          = { id = "lt-0123456789abcdef0", latest_version = 1 }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values          = { arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev" }
}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
  owner_ip_cidrs      = []
}

run "test_writer_is_unprepared_by_default" {
  command = plan

  assert {
    condition = (
      length(aws_iam_role.job_write_e2e_task) == 0 &&
      length(aws_iam_role_policy.job_writer_e2e_storage) == 0 &&
      length(aws_iam_role_policy.job_writer_e2e_stage) == 0 &&
      length(aws_iam_role_policy.job_reader_e2e) == 0 &&
      length(aws_iam_role_policy.job_writer_e2e_execution) == 0 &&
      length(aws_iam_role.bronze_writer) == 0 &&
      !output.job_writer_e2e_contract.prepared &&
      output.job_writer_e2e_contract.task_role == null &&
      output.job_writer_e2e_contract.production_activation == false &&
      toset(keys(output.job_roles)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer"])
    )
    error_message = "Default preparation must create no test writer or new role grants and must preserve the five existing job roles."
  }
}

run "prepared_test_writer_needs_a_selected_source" {
  command = plan

  variables {
    job_writer_e2e_prepared = true
  }

  expect_failures = [aws_iam_role.job_write_e2e_task]
}

run "prepared_test_writer_refuses_an_unmapped_source" {
  command = plan

  variables {
    job_writer_e2e_prepared         = true
    bronze_qualification_collection = "example/not_mapped"
  }

  expect_failures = [aws_iam_role.job_write_e2e_task]
}

run "prepared_test_writer_has_exact_storage_and_bindings" {
  command = plan

  variables {
    job_writer_e2e_prepared         = true
    bronze_qualification_collection = "cms/hospitals"
  }

  assert {
    condition = (
      aws_iam_role.job_write_e2e_task[0].name == "example_project_job_write_e2e_task_dev" &&
      aws_iam_role.job_write_e2e_task[0].max_session_duration == 3600 &&
      aws_iam_role.job_write_e2e_task[0].tags.Project == "example_project" &&
      aws_iam_role.job_write_e2e_task[0].tags.Environment == "dev" &&
      aws_iam_role.job_write_e2e_task[0].tags.ManagedBy == "Terraform" &&
      length(aws_iam_role.bronze_writer) == 0 &&
      output.job_writer_e2e_contract.production_activation == false &&
      toset(keys(output.job_roles)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer", "write_e2e_task"])
    )
    error_message = "The test role needs the project's name, tags and one-hour session. No production writer is prepared."
  }

  assert {
    condition = jsondecode(aws_iam_role.job_write_e2e_task[0].assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Action    = "sts:AssumeRole"
        Principal = { Service = "ecs-tasks.amazonaws.com" }
        Condition = {
          StringEquals = { "aws:SourceAccount" = "111111111111" }
          ArnLike      = { "aws:SourceArn" = "arn:aws:ecs:eu-west-1:111111111111:*" }
        }
      }]
    }
    error_message = "The test role must trust only ECS tasks with the exact source account and regional ECS ARN condition."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement) == 4 &&
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[0].Action) == toset([
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[0].Resource) == toset([
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/features_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_dictionary_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/smoke_check_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[2].Action == ["s3:GetObjectVersion"] &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[2].Resource == ["arn:aws:s3:::example-project-ci-bucket/cms/hospitals/*"] &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[3].Action == ["s3:ListBucketVersions"] &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[3].Condition.StringLike["s3:prefix"] == ["cms/hospitals/*"]
    )
    error_message = "Test writes cover exactly seven twins. The single selected source allows versioned reads and version listings only."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[1].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[1].Condition.StringLike["s3:prefix"]) == toset([
        "lakehouse/silver_e2e/*", "lakehouse/gold_e2e/*", "lakehouse/features_e2e/*", "lakehouse/bronze_e2e/*",
        "lakehouse/bronze_dictionary_e2e/*", "lakehouse/smoke_check_e2e/*", "lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      aws_iam_role_policy.job_writer_e2e_storage[0].role == aws_iam_role.job_write_e2e_task[0].name &&
      aws_iam_role_policy.job_writer_e2e_stage[0].role == aws_iam_role.job_write_e2e_task[0].name &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_stage[0].policy) == jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy) &&
      aws_iam_role_policy.job_reader_e2e[0].role == aws_iam_role.job["read_task"].name &&
      aws_iam_role_policy.job_writer_e2e_execution[0].role == aws_iam_role.job["execution"].name
    )
    error_message = "Prefix conditions and policy attachments must preserve storage, grant, read and execution identity boundaries."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement) == 4 &&
      jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[0].Action == ["s3:GetObject"] &&
      toset(jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[0].Resource) == toset([
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/features_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_dictionary_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/smoke_check_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[2].Action == ["s3:GetObjectVersion"] &&
      toset(jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[2].Resource) == toset([
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_dictionary_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/smoke_check_e2e/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/duckdb_view_check_e2e/*"
      ]) &&
      jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[3].Action == ["s3:ListBucketVersions"] &&
      toset(jsondecode(aws_iam_role_policy.job_reader_e2e[0].policy).Statement[3].Condition.StringLike["s3:prefix"]) == toset([
        "lakehouse/bronze_e2e/*", "lakehouse/bronze_dictionary_e2e/*", "lakehouse/smoke_check_e2e/*", "lakehouse/duckdb_view_check_e2e/*"
      ])
    )
    error_message = "Reconciliation reads all seven twins, with versioned access added only for the four bronze twins."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_writer_e2e_execution[0].policy).Statement) == 1 &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_execution[0].policy).Statement[0].Action == ["ssm:GetParameters"] &&
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_execution[0].policy).Statement[0].Resource) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_e2e_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_e2e_client_secret"
      ]) &&
      length(jsondecode(aws_iam_role_policy.job_execution.policy).Statement[3].Resource) == 2 &&
      !contains(local.managed_parameter_names, var.aws_polaris_e2e_client_id_name) &&
      !contains(local.managed_parameter_names, var.aws_polaris_e2e_client_secret_name) &&
      !contains(local.read_parameter_names, var.aws_polaris_e2e_client_id_name) &&
      !contains(local.read_parameter_names, var.aws_polaris_e2e_client_secret_name)
    )
    error_message = "Only job_execution injects the test pair beside the reader pair. The project user never manages a writer pair."
  }
}

run "test_pair_uses_parameter_name_inputs" {
  command = plan

  variables {
    job_writer_e2e_prepared            = true
    bronze_qualification_collection    = "cms/hospitals"
    aws_polaris_e2e_client_id_name     = "/example_project/qualification/aws_polaris_e2e_client_id"
    aws_polaris_e2e_client_secret_name = "/example_project/qualification/aws_polaris_e2e_client_secret"
  }

  assert {
    condition = toset(jsondecode(aws_iam_role_policy.job_writer_e2e_execution[0].policy).Statement[0].Resource) == toset([
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/qualification/aws_polaris_e2e_client_id",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/qualification/aws_polaris_e2e_client_secret"
    ])
    error_message = "Execution rights must use the configured pair names without a second hard-coded name list."
  }
}

run "test_pair_cannot_alias_the_reader" {
  command = plan

  variables {
    job_writer_e2e_prepared           = true
    bronze_qualification_collection   = "cms/hospitals"
    aws_polaris_reader_client_id_name = "/healthcare_hai_prediction/aws_polaris_e2e_client_id"
  }

  expect_failures = [aws_iam_role.job_write_e2e_task]
}

run "test_pair_refuses_a_production_name" {
  command = plan

  variables {
    aws_polaris_e2e_client_secret_name = "/healthcare_hai_prediction/aws_polaris_gold_client_secret"
  }

  expect_failures = [var.aws_polaris_e2e_client_secret_name]
}

run "test_pair_refuses_a_wildcard" {
  command = plan

  variables {
    aws_polaris_e2e_client_id_name = "/healthcare_hai_prediction/*/aws_polaris_e2e_client_id"
  }

  expect_failures = [var.aws_polaris_e2e_client_id_name]
}

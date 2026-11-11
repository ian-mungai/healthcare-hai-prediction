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


run "source_roots_default_to_the_bucket_root_only" {
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
      alltrue([for arn in jsondecode(aws_iam_policy.bronze_writer["source_objects_root"].policy).Statement[0].Resource : !strcontains(arn, "/raw/")]) &&
      !contains(keys(aws_iam_policy.bronze_writer), "source_objects_raw") &&
      jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[2].Resource == ["arn:aws:s3:::example-data-bucket/cms/hospitals/*"]
    )
    error_message = "Without the raw root switch, source reads stay at the bucket root."
  }
}

run "source_roots_add_raw_for_the_bronze_writer_and_qualification_roles" {
  command = plan

  variables {
    source_raw_root_enabled         = true
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
      toset(concat(
        jsondecode(aws_iam_policy.bronze_writer["source_objects_root"].policy).Statement[0].Resource,
        jsondecode(aws_iam_policy.bronze_writer["source_objects_raw"].policy).Statement[0].Resource
        )) == toset(concat(flatten([
          for collection in distinct([for table in jsondecode(file("../../config/lakehouse/bronze_tables.json")).tables : table.collection]) :
          ["arn:aws:s3:::example-data-bucket/${collection}/*", "arn:aws:s3:::example-data-bucket/raw/${collection}/*"]
      ]), ["arn:aws:s3:::example-data-bucket/bronze_objects/sha256/*", "arn:aws:s3:::example-data-bucket/raw/bronze_objects/sha256/*"])) &&
      toset(jsondecode(aws_iam_policy.bronze_writer["source_lists"].policy).Statement[0].Condition.StringLike["s3:prefix"]) == toset(flatten([
        for collection in distinct([for table in jsondecode(file("../../config/lakehouse/bronze_tables.json")).tables : table.collection]) :
        ["${collection}/*", "raw/${collection}/*"]
      ])) &&
      jsondecode(aws_iam_policy.bronze_writer["source_objects_raw"].policy).Statement[0].Action == ["s3:GetObjectVersion"]
    )
    error_message = "The bronze writer must read each mapped collection at the root and under raw, with version reads only."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[2].Resource) == toset([
        "arn:aws:s3:::example-data-bucket/cms/hospitals/*", "arn:aws:s3:::example-data-bucket/raw/cms/hospitals/*"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_writer_e2e_storage[0].policy).Statement[3].Condition.StringLike["s3:prefix"]) == toset([
        "cms/hospitals/*", "raw/cms/hospitals/*"
      ])
    )
    error_message = "The qualification role must read its one collection at the root and under raw."
  }
}

run "publisher_job_class_is_off_by_default" {
  command = plan

  assert {
    condition     = length(aws_iam_role.job_publisher) == 0 && output.publisher_job_contract == null
    error_message = "The run 3 publisher class must create nothing by default."
  }
}

run "publisher_job_class_writes_raw_once_and_never_deletes" {
  command = plan

  variables {
    publisher_job_enabled = true
  }

  assert {
    condition     = aws_iam_role.job_publisher[0].name == "example_project_job_publisher_task_dev"
    error_message = "The publisher class must have its own task role."
  }

  assert {
    condition = (
      !strcontains(aws_iam_role_policy.job_publisher[0].policy, "Delete") &&
      !strcontains(aws_iam_role_policy.job_publisher[0].policy, "lakehouse") &&
      strcontains(aws_iam_role_policy.job_publisher[0].policy, "s3:if-none-match") &&
      alltrue([for statement in jsondecode(aws_iam_role_policy.job_publisher[0].policy).Statement : (
        statement.Sid == "ReadStoredPayloadVersions" ? true : !can(regex("example-data-bucket/[^r]", jsonencode(statement.Resource)))
      )])
    )
    error_message = "Outside the stored payload read the publisher role must reach only raw, create objects once and hold no delete action."
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.job_publisher[0].policy).Statement : statement if statement.Sid == "ReadStoredPayloadVersions"][0].Action == ["s3:GetObjectVersion"] &&
      toset([for statement in jsondecode(aws_iam_role_policy.job_publisher[0].policy).Statement : statement if statement.Sid == "ReadStoredPayloadVersions"][0].Resource) == toset([
        "arn:aws:s3:::example-data-bucket/*/*/datasets/*",
        "arn:aws:s3:::example-data-bucket/*/*/references/*",
        "arn:aws:s3:::example-data-bucket/bronze_objects/sha256/*",
        "arn:aws:s3:::example-data-bucket/bronze_evidence/sha256/*",
      ])
    )
    error_message = "The stored payload read is version reads only, on the payload, reference and single-copy prefixes."
  }

  assert {
    condition = (
      output.publisher_job_contract.egress == "host_job_firewall_tcp_443" && !contains(keys(output.publisher_job_contract), "security_group") &&
      output.publisher_job_contract.host_limit == "application_queue_hosts"
    )
    error_message = "The publisher runs on the host bridge network: it has no security group of its own, its egress is the host job firewall's TCP 443 and the queue gate limits its hosts."
  }
}

run "publisher_exports_are_absent_by_default" {
  command = plan

  assert {
    condition = (
      !contains(keys(output.job_roles), "publisher_task") && output.publisher_role == null &&
      output.publisher_project_delta == null && toset(keys(output.job_network.security_group_ids)) == toset(["offline", "lakehouse"])
    )
    error_message = "By default no publisher role, network or grant is exported and the private job network is unchanged."
  }
}

run "publisher_exports_leave_the_target_role_set_untouched" {
  command = plan

  variables {
    publisher_job_enabled = true
  }

  assert {
    condition = (
      !contains(keys(output.job_roles), "publisher_task") &&
      toset(keys(output.job_roles)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer"]) &&
      toset(keys(output.job_network.security_group_ids)) == toset(["offline", "lakehouse"])
    )
    error_message = "Enabling the publisher must not add a key to job_roles or job_network, which the closed target schema reads."
  }

  assert {
    condition = (
      output.publisher_role.name == "example_project_job_publisher_task_dev" && output.publisher_job_contract.network == "host_bridge"
    )
    error_message = "The publisher role comes out of its own output for the jobs root, and the publisher runs on the host bridge network: no subnet, security group or public IP of its own."
  }
}

run "publisher_pass_role_grant_names_only_the_publisher_role" {
  command = plan

  variables {
    publisher_job_enabled = true
  }

  assert {
    condition = (
      output.publisher_project_delta.Statement[0].Action == ["iam:PassRole"] &&
      output.publisher_project_delta.Statement[0].Resource == "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev" &&
      output.publisher_project_delta.Statement[0].Condition.StringEquals["iam:PassedToService"] == "ecs-tasks.amazonaws.com" &&
      length(output.publisher_project_delta.Statement) == 1
    )
    error_message = "The project user may pass only the publisher task role, only to ECS tasks."
  }
}

run "publisher_logs_and_image_pull_come_from_the_execution_role" {
  command = plan

  variables {
    publisher_job_enabled = true
  }

  assert {
    condition = (
      strcontains(aws_iam_role_policy.job_execution.policy, "log-group:example_project_jobs_dev:log-stream:*") &&
      strcontains(aws_iam_role_policy.job_execution.policy, "repository/example_project_analytics_dev") &&
      !strcontains(aws_iam_role_policy.job_publisher[0].policy, "logs:") &&
      !strcontains(aws_iam_role_policy.job_publisher[0].policy, "ecr:") &&
      !strcontains(aws_iam_role_policy.job_publisher[0].policy, "ssm:")
    )
    error_message = "Logs and image pull belong to the shared execution role. The publisher task role must hold none of them."
  }
}

run "bronze_contract_states_the_read_roots_that_match_the_grants" {
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
    condition = toset(keys(output.bronze_writer_contract)) == toset([
      "principal", "task_role_key", "task_role_name", "task_role_arn", "parameter_names", "source_collections", "qualification_collection",
      "namespaces", "test_namespaces", "production_layer", "credentials_owner", "catalog_grant_contract"
    ])
    error_message = "With the raw grant off the bronze contract must keep exactly its earlier keys."
  }
}

run "bronze_contract_sets_the_raw_read_environment_with_the_grant" {
  command = plan

  variables {
    source_raw_root_enabled         = true
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-data-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-data-bucket/lakehouse/bronze_dictionary/"
    }
  }

  assert {
    condition     = jsonencode(output.bronze_writer_contract.read_roots) == jsonencode(["", "raw/"]) && jsonencode(output.bronze_writer_contract.read_environment) == jsonencode({ HAI_READ_RAW_ROOT = "true" })
    error_message = "With the raw grant on the contract must state both roots and tell dispatch to set the raw read variable."
  }
}

run "publisher_keys_are_off_by_default" {
  command = plan

  variables {
    publisher_job_enabled = true
  }

  assert {
    condition     = length(aws_iam_role_policy.job_publisher_keys) == 0 && !strcontains(aws_iam_role_policy.job_publisher[0].policy, "secretsmanager") && length(output.publisher_job_contract.secrets) == 0
    error_message = "The publisher task role reads no secret until the owner switches the keys on."
  }
}

run "publisher_keys_read_only_the_three_exact_secrets" {
  command = plan

  variables {
    publisher_job_enabled  = true
    publisher_keys_enabled = true
    publisher_secret_arns = {
      census_api_key = "arn:aws:secretsmanager:us-west-2:111111111111:secret:census_api_key-AbCdEf"
      bls_api_key    = "arn:aws:secretsmanager:us-west-2:111111111111:secret:bls_api_key-GhIjKl"
      hud_api_key    = "arn:aws:secretsmanager:us-west-2:111111111111:secret:hud_api_key-MnOpQr"
    }
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_publisher_keys[0].policy).Statement) == 1 &&
      jsondecode(aws_iam_role_policy.job_publisher_keys[0].policy).Statement[0].Action == ["secretsmanager:GetSecretValue"] &&
      toset(jsondecode(aws_iam_role_policy.job_publisher_keys[0].policy).Statement[0].Resource) == toset([
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:census_api_key-AbCdEf",
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:bls_api_key-GhIjKl",
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:hud_api_key-MnOpQr",
      ]) &&
      !strcontains(aws_iam_role_policy.job_publisher_keys[0].policy, "*") &&
      join(",", output.publisher_job_contract.secrets) == "bls_api_key,census_api_key,hud_api_key"
    )
    error_message = "The publisher task role may read exactly the three key secrets by exact ARN and nothing else."
  }
}

run "publisher_keys_refuse_a_wildcard_arn" {
  command = plan

  variables {
    publisher_job_enabled  = true
    publisher_keys_enabled = true
    publisher_secret_arns = {
      census_api_key = "arn:aws:secretsmanager:us-west-2:111111111111:secret:census_api_key-*"
      bls_api_key    = "arn:aws:secretsmanager:us-west-2:111111111111:secret:bls_api_key-GhIjKl"
      hud_api_key    = "arn:aws:secretsmanager:us-west-2:111111111111:secret:hud_api_key-MnOpQr"
    }
  }

  expect_failures = [var.publisher_secret_arns]
}

run "publisher_keys_refuse_a_fourth_or_a_missing_secret" {
  command = plan

  variables {
    publisher_job_enabled  = true
    publisher_keys_enabled = true
    publisher_secret_arns = {
      census_api_key = "arn:aws:secretsmanager:us-west-2:111111111111:secret:census_api_key-AbCdEf"
      other_secret   = "arn:aws:secretsmanager:us-west-2:111111111111:secret:other_secret-GhIjKl"
    }
  }

  expect_failures = [var.publisher_secret_arns]
}

run "publisher_keys_need_the_class" {
  command = plan

  variables {
    publisher_keys_enabled = true
    publisher_secret_arns = {
      census_api_key = "arn:aws:secretsmanager:us-west-2:111111111111:secret:census_api_key-AbCdEf"
      bls_api_key    = "arn:aws:secretsmanager:us-west-2:111111111111:secret:bls_api_key-GhIjKl"
      hud_api_key    = "arn:aws:secretsmanager:us-west-2:111111111111:secret:hud_api_key-MnOpQr"
    }
  }

  assert {
    condition     = length(aws_iam_role_policy.job_publisher_keys) == 0
    error_message = "With the class off there is no role to grant anything to."
  }
}

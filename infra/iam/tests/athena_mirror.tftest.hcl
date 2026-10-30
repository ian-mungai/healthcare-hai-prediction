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

  mock_resource "aws_iam_policy" {
    defaults = { arn = "arn:aws:iam::111111111111:policy/example_project_athena_reader_dev" }
  }

  mock_resource "aws_athena_workgroup" {
    defaults = { arn = "arn:aws:athena:eu-west-1:111111111111:workgroup/hai_mirror" }
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

run "athena_mirror_defaults_to_off" {
  command = plan

  assert {
    condition = (
      !var.athena_mirror_prepared &&
      output.athena_mirror_contract.prepared == false &&
      length(aws_glue_catalog_database.athena_mirror) == 0 &&
      length(aws_athena_workgroup.athena_mirror) == 0 &&
      length(aws_iam_role.athena_reader) == 0 &&
      length(aws_iam_role_policy.athena_reader) == 0 &&
      length(aws_iam_role_policy.athena_mirror_writer) == 0
    )
    error_message = "The Athena mirror creates no database, workgroup, policy or grant until the switch is set."
  }
}

run "prepared_mirror_has_one_gold_database_and_a_limited_workgroup" {
  command = plan
  variables {
    athena_mirror_prepared = true
    athena_owner_user      = "example_owner"
  }

  assert {
    condition = (
      toset(keys(aws_glue_catalog_database.athena_mirror)) == toset(["gold"]) &&
      aws_glue_catalog_database.athena_mirror["gold"].name == "hai_mirror_gold" &&
      aws_glue_catalog_database.athena_mirror["gold"].tags.DataClassification == "Confidential"
    )
    error_message = "One Glue database for the gold layer only, named by the tracked config and tagged Confidential because it lists every gold table."
  }

  assert {
    condition = (
      aws_athena_workgroup.athena_mirror[0].name == "hai_mirror" &&
      aws_athena_workgroup.athena_mirror[0].configuration[0].enforce_workgroup_configuration == true &&
      aws_athena_workgroup.athena_mirror[0].configuration[0].bytes_scanned_cutoff_per_query == 1073741824 &&
      aws_athena_workgroup.athena_mirror[0].configuration[0].result_configuration[0].output_location == "s3://example-project-ci-bucket/athena/results/" &&
      aws_athena_workgroup.athena_mirror[0].configuration[0].result_configuration[0].encryption_configuration[0].encryption_option == "SSE_S3"
    )
    error_message = "The workgroup enforces its settings, cancels a query past the configured bytes scanned and writes encrypted results under athena/results/."
  }

  assert {
    condition     = local.athena_mirror_config.workgroup.bytes_scanned_limit >= 10485760 && local.athena_mirror_config.workgroup.bytes_scanned_limit <= 10737418240
    error_message = "The limit must be at least the Athena minimum of 10 MB and at most 10 GiB."
  }
}

run "gold_writer_grants_cover_only_the_gold_mirror_database" {
  command = plan
  variables {
    athena_mirror_prepared     = true
    athena_owner_user          = "example_owner"
    job_writer_layers_prepared = true
  }

  assert {
    condition     = toset(keys(aws_iam_role_policy.athena_mirror_writer)) == toset(["gold"])
    error_message = "Only the gold writer gets a grant, and never silver, bronze, features, the qualification role or the project user."
  }

  assert {
    condition = (
      aws_iam_role_policy.athena_mirror_writer["gold"].role == aws_iam_role.job_write_layer_task["gold"].name &&
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_mirror_writer["gold"].policy).Statement : s.Action])) ==
      toset(["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]) &&
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_mirror_writer["gold"].policy).Statement : s.Resource])) == toset([
        "arn:aws:glue:eu-west-1:111111111111:catalog",
        "arn:aws:glue:eu-west-1:111111111111:database/hai_mirror_gold",
        "arn:aws:glue:eu-west-1:111111111111:table/hai_mirror_gold/*",
      ])
    )
    error_message = "The gold writer may read its database and read, create and update tables in it only, with no delete and no other database."
  }
}

run "reader_policy_reads_the_mirror_and_writes_only_its_results" {
  command = plan
  variables {
    athena_mirror_prepared = true
    athena_owner_user      = "example_owner"
  }

  assert {
    condition = (
      length(aws_iam_role.athena_reader) == 1 &&
      aws_iam_role.athena_reader[0].name == "example_project_athena_reader_dev" &&
      aws_iam_role_policy.athena_reader[0].role == aws_iam_role.athena_reader[0].name
    )
    error_message = "A prepared mirror with a named owner creates one reader role that carries the reader policy inline."
  }

  assert {
    condition = alltrue([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement :
      s.Effect == "Allow" && !anytrue([for a in s.Action : endswith(a, "*")]) && !anytrue([for r in s.Resource : r == "*"])
    ])
    error_message = "The reader policy has no wildcard action and no all-resources grant."
  }

  assert {
    condition     = toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : [for a in s.Action : a if startswith(a, "glue:")]])) == toset(["glue:GetDatabase", "glue:GetTable"])
    error_message = "The reader may read Glue registrations and nothing else in Glue."
  }

  assert {
    condition     = toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if startswith(r, "arn:aws:athena:")]])) == toset(["arn:aws:athena:eu-west-1:111111111111:workgroup/hai_mirror"])
    error_message = "Athena actions are limited to the mirror workgroup."
  }

  assert {
    condition = (
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if startswith(r, "arn:aws:glue:") && !endswith(r, ":catalog")]])) == toset([
        "arn:aws:glue:eu-west-1:111111111111:database/hai_mirror_gold",
        "arn:aws:glue:eu-west-1:111111111111:table/hai_mirror_gold/*",
      ])
    )
    error_message = "Glue reads are limited to the gold mirror database and its tables."
  }

  assert {
    condition = (
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if startswith(r, "arn:aws:s3:::") && contains(s.Action, "s3:GetObject") && !contains(s.Action, "s3:PutObject")]])) ==
      toset(["arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*"]) &&
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if contains(s.Action, "s3:PutObject")]])) == toset(["arn:aws:s3:::example-project-ci-bucket/athena/results/*"]) &&
      !anytrue([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : contains(s.Action, "s3:DeleteObject")])
    )
    error_message = "Data reads reach only the gold namespace, writes reach only the results prefix, and nothing deletes."
  }
}

run "reader_policy_names_no_principal_and_no_wider_resource" {
  command = plan
  variables {
    athena_mirror_prepared = true
    athena_owner_user      = "example_owner"
  }

  assert {
    condition = alltrue([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement :
      s.Effect == "Allow" && !contains(keys(s), "Principal") && !contains(keys(s), "NotPrincipal") && !contains(keys(s), "NotAction") && !contains(keys(s), "NotResource")
    ])
    error_message = "The reader policy is an identity policy with Allow statements only and names no principal."
  }

  assert {
    condition = toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_reader[0].policy).Statement : s.Resource])) == toset([
      "arn:aws:athena:eu-west-1:111111111111:workgroup/hai_mirror",
      "arn:aws:glue:eu-west-1:111111111111:catalog",
      "arn:aws:glue:eu-west-1:111111111111:database/hai_mirror_gold",
      "arn:aws:glue:eu-west-1:111111111111:table/hai_mirror_gold/*",
      "arn:aws:s3:::example-project-ci-bucket",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*",
      "arn:aws:s3:::example-project-ci-bucket/athena/results/*",
    ])
    error_message = "The reader policy reaches exactly the mirror workgroup, database, gold folders, results prefix and bucket, nothing wider."
  }
}

run "reader_access_needs_a_named_owner" {
  command = plan
  variables {
    athena_mirror_prepared = true
  }

  assert {
    condition     = var.athena_owner_user == "" && length(aws_iam_role.athena_reader) == 0 && length(aws_iam_role_policy.athena_reader) == 0
    error_message = "Until the owner names a user, no role or policy gives anyone Athena access."
  }
}

run "reader_role_trusts_only_the_owner_user" {
  command = plan
  variables {
    athena_mirror_prepared = true
    athena_owner_user      = "example_owner"
  }

  assert {
    condition = jsondecode(aws_iam_role.athena_reader[0].assume_role_policy) == {
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Action    = "sts:AssumeRole"
        Principal = { AWS = "arn:aws:iam::111111111111:user/example_owner" }
      }]
    }
    error_message = "The reader role trusts exactly the one user the owner names, with no service, wildcard or other account."
  }

  assert {
    condition     = aws_iam_role.athena_reader[0].max_session_duration == 3600
    error_message = "A reader session lasts one hour."
  }
}

run "reader_role_needs_the_mirror_switch" {
  command = plan
  variables {
    athena_owner_user = "example_owner"
  }

  assert {
    condition     = length(aws_iam_role.athena_reader) == 0 && length(aws_iam_role_policy.athena_reader) == 0
    error_message = "Naming a user creates nothing while the mirror is not prepared."
  }
}

run "reject_wildcard_owner_user" {
  command = plan
  variables {
    athena_mirror_prepared = true
    athena_owner_user      = "*"
  }
  expect_failures = [var.athena_owner_user]
}

run "reject_owner_user_with_a_path" {
  command = plan
  variables {
    athena_mirror_prepared = true
    athena_owner_user      = "team/example_owner"
  }
  expect_failures = [var.athena_owner_user]
}

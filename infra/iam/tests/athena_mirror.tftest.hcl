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
      length(aws_iam_policy.athena_reader) == 0 &&
      length(aws_iam_role_policy.athena_mirror_writer) == 0
    )
    error_message = "The Athena mirror creates no database, workgroup, policy or grant until the switch is set."
  }
}

run "prepared_mirror_has_two_databases_and_a_limited_workgroup" {
  command = plan
  variables {
    athena_mirror_prepared = true
  }

  assert {
    condition = (
      toset(keys(aws_glue_catalog_database.athena_mirror)) == toset(["silver", "gold"]) &&
      aws_glue_catalog_database.athena_mirror["silver"].name == "hai_mirror_silver" &&
      aws_glue_catalog_database.athena_mirror["gold"].name == "hai_mirror_gold" &&
      aws_glue_catalog_database.athena_mirror["gold"].tags.DataClassification == "Public"
    )
    error_message = "One Glue database per mirrored layer, named by the tracked config and tagged with the class of what it lists."
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

run "writer_grants_cover_only_the_listed_tables_of_their_own_layer" {
  command = plan
  variables {
    athena_mirror_prepared     = true
    job_writer_layers_prepared = true
  }

  assert {
    condition     = toset(keys(aws_iam_role_policy.athena_mirror_writer)) == toset(["gold"])
    error_message = "Only a layer that lists a table gets a grant, and never bronze, features, the qualification role or the project user."
  }

  assert {
    condition = (
      aws_iam_role_policy.athena_mirror_writer["gold"].role == aws_iam_role.job_write_layer_task["gold"].name &&
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_mirror_writer["gold"].policy).Statement : s.Action])) ==
      toset(["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]) &&
      toset(flatten([for s in jsondecode(aws_iam_role_policy.athena_mirror_writer["gold"].policy).Statement : s.Resource])) == toset([
        "arn:aws:glue:eu-west-1:111111111111:catalog",
        "arn:aws:glue:eu-west-1:111111111111:database/hai_mirror_gold",
        "arn:aws:glue:eu-west-1:111111111111:table/hai_mirror_gold/bridge_county_neighbor",
      ])
    )
    error_message = "The gold writer may read a database and read, create and update exactly the listed table, with no delete and no other database."
  }
}

run "reader_policy_reads_the_mirror_and_writes_only_its_results" {
  command = plan
  variables {
    athena_mirror_prepared = true
  }

  assert {
    condition = (
      length(aws_iam_policy.athena_reader) == 1 &&
      aws_iam_policy.athena_reader[0].name == "example_project_athena_reader_dev"
    )
    error_message = "A prepared mirror creates one unattached reader policy."
  }

  assert {
    condition = alltrue([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement :
      s.Effect == "Allow" && !anytrue([for a in s.Action : endswith(a, "*")]) && !anytrue([for r in s.Resource : r == "*"])
    ])
    error_message = "The reader policy has no wildcard action and no all-resources grant."
  }

  assert {
    condition     = toset(flatten([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement : [for a in s.Action : a if startswith(a, "glue:")]])) == toset(["glue:GetDatabase", "glue:GetTable"])
    error_message = "The reader may read Glue registrations and nothing else in Glue."
  }

  assert {
    condition     = toset(flatten([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if startswith(r, "arn:aws:athena:")]])) == toset(["arn:aws:athena:eu-west-1:111111111111:workgroup/hai_mirror"])
    error_message = "Athena actions are limited to the mirror workgroup."
  }

  assert {
    condition = (
      toset(flatten([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if startswith(r, "arn:aws:glue:") && !endswith(r, ":catalog")]])) == toset([
        "arn:aws:glue:eu-west-1:111111111111:database/hai_mirror_gold",
        "arn:aws:glue:eu-west-1:111111111111:table/hai_mirror_gold/bridge_county_neighbor",
      ])
    )
    error_message = "Glue reads are limited to the mirror databases and tables that the config lists."
  }

  assert {
    condition = (
      toset(flatten([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if startswith(r, "arn:aws:s3:::") && contains(s.Action, "s3:GetObject") && !contains(s.Action, "s3:PutObject")]])) ==
      toset(["arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/bridge_county_neighbor*"]) &&
      toset(flatten([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement : [for r in s.Resource : r if contains(s.Action, "s3:PutObject")]])) == toset(["arn:aws:s3:::example-project-ci-bucket/athena/results/*"]) &&
      !anytrue([for s in jsondecode(aws_iam_policy.athena_reader[0].policy).Statement : contains(s.Action, "s3:DeleteObject")])
    )
    error_message = "Data reads reach only the listed tables' folders, writes reach only the results prefix, and nothing deletes."
  }
}

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


run "production_writers_and_fences_default_off" {
  command = plan

  assert {
    condition     = !var.job_writer_layers_prepared && !var.bronze_writer_prepared && !var.catalog_cutover_fenced && !var.aws_writers_fenced
    error_message = "Every production preparation switch and both fence requests must default to false."
  }

  assert {
    condition     = toset(keys(output.job_roles)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer"])
    error_message = "Only the five existing roles may exist in the default plan."
  }

  assert {
    condition = (
      output.job_writer_layer_contract.production_activation == false &&
      output.job_writer_layer_contract.fence_requests == {
        catalog_cutover_fenced = false
        aws_writers_fenced     = false
      } &&
      !can(aws_iam_role_policy.job_writer_execution[0].role) &&
      !can(aws_iam_role_policy.job_writer_layer_storage["silver"].role) &&
      !can(aws_iam_role_policy.job_writer_layer_storage["gold"].role) &&
      !can(aws_iam_role_policy.job_writer_layer_storage["features"].role) &&
      !can(aws_iam_role_policy.job_writer_stage["silver"].role) &&
      !can(aws_iam_role_policy.job_writer_stage["gold"].role) &&
      !can(aws_iam_role_policy.job_writer_stage["features"].role) &&
      !can(aws_iam_role_policy.job_writer_stage["bronze"].role)
    )
    error_message = "Default preparation creates no production storage, stage or execution grant and leaves activation and fence requests off."
  }
}

run "prepared_layers_have_exact_roles_trust_and_stage_attachments" {
  command = plan
  variables {
    job_writer_layers_prepared      = true
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-project-ci-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-project-ci-bucket/lakehouse/bronze_dictionary/"
    }
  }

  assert {
    condition = (
      toset(keys(aws_iam_role.job_write_layer_task)) == toset(["silver", "gold", "features"]) &&
      alltrue([for layer, role in aws_iam_role.job_write_layer_task :
        role.name == format("example_project_job_write_%s_task_dev", layer) &&
        role.max_session_duration == 3600 &&
        role.tags.Project == "example_project" && role.tags.Environment == "dev" && role.tags.ManagedBy == "Terraform"
      ]) &&
      aws_iam_role.bronze_writer[0].name == "example_project_job_write_bronze_task_dev" &&
      aws_iam_role.bronze_writer[0].max_session_duration == 3600 &&
      aws_iam_role.bronze_writer[0].tags.Project == "example_project" &&
      length(aws_iam_role.job_write_e2e_task) == 0 &&
      output.job_writer_layer_contract.production_activation == false &&
      toset(keys(output.job_roles)) == toset([
        "execution", "offline_task", "seal_task", "read_task", "try_writer",
        "write_silver_task", "write_gold_task", "write_features_task", "write_bronze_task"
      ])
    )
    error_message = "Exactly four production roles must preserve names, one-hour sessions, tags and target-file identities without enabling the test role or activation."
  }

  assert {
    condition = alltrue([for role in concat(values(aws_iam_role.job_write_layer_task), aws_iam_role.bronze_writer) :
      jsondecode(role.assume_role_policy) == {
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
    ])
    error_message = "Every production writer must trust only ECS tasks with the source-account and regional ECS ARN conditions."
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.job_writer_stage)) == toset(["silver", "gold", "features", "bronze"]) &&
      aws_iam_role_policy.job_writer_execution[0].role == aws_iam_role.job["execution"].name
    )
    error_message = "Exactly four writer stage policies attach, and only job_execution receives production injection rights."
  }

  assert {
    condition = (
      aws_iam_role_policy.job_writer_stage["silver"].role == output.job_roles["write_silver_task"].name &&
      jsondecode(aws_iam_role_policy.job_writer_stage["silver"].policy) == jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy) &&
      aws_iam_role_policy.job_writer_layer_storage["silver"].role == aws_iam_role.job_write_layer_task["silver"].name
    )
    error_message = "silver must retain its own storage identity and the existing pinned-input and own-grant read policy."
  }

  assert {
    condition = (
      aws_iam_role_policy.job_writer_stage["gold"].role == output.job_roles["write_gold_task"].name &&
      jsondecode(aws_iam_role_policy.job_writer_stage["gold"].policy) == jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy) &&
      aws_iam_role_policy.job_writer_layer_storage["gold"].role == aws_iam_role.job_write_layer_task["gold"].name
    )
    error_message = "gold must retain its own storage identity and the existing pinned-input and own-grant read policy."
  }

  assert {
    condition = (
      aws_iam_role_policy.job_writer_stage["features"].role == output.job_roles["write_features_task"].name &&
      jsondecode(aws_iam_role_policy.job_writer_stage["features"].policy) == jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy) &&
      aws_iam_role_policy.job_writer_layer_storage["features"].role == aws_iam_role.job_write_layer_task["features"].name
    )
    error_message = "features must retain its own storage identity and the existing pinned-input and own-grant read policy."
  }

  assert {
    condition = (
      aws_iam_role_policy.job_writer_stage["bronze"].role == output.job_roles["write_bronze_task"].name &&
      jsondecode(aws_iam_role_policy.job_writer_stage["bronze"].policy) == jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy)
    )
    error_message = "bronze must retain its own storage identity and the existing pinned-input and own-grant read policy."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_writer_execution[0].policy).Statement) == 1 &&
      jsondecode(aws_iam_role_policy.job_writer_execution[0].policy).Statement[0].Action == ["ssm:GetParameters"] &&
      toset(jsondecode(aws_iam_role_policy.job_writer_execution[0].policy).Statement[0].Resource) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_silver_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_silver_client_secret",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_gold_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_gold_client_secret",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_features_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_features_client_secret",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_bronze_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_bronze_client_secret"
      ]) &&
      alltrue([for name in flatten(values(output.job_writer_layer_contract.parameter_names)) :
        !contains(local.managed_parameter_names, name) && !contains(local.read_parameter_names, name)
      ])
    )
    error_message = "Production injection is exactly GetParameters on eight application parameters. No production pair enters project-user management or reads."
  }
}

run "silver_storage_has_only_its_approved_prefixes" {
  command = plan
  variables {
    job_writer_layers_prepared = true
  }
  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_writer_layer_storage["silver"].policy).Statement) == 2 &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["silver"].policy).Statement[0].Action) == toset([
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["silver"].policy).Statement[0].Resource) == toset(["arn:aws:s3:::example-project-ci-bucket/lakehouse/silver/*"]) &&
      jsondecode(aws_iam_role_policy.job_writer_layer_storage["silver"].policy).Statement[1].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_layer_storage["silver"].policy).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["silver"].policy).Statement[1].Condition.StringLike["s3:prefix"]) == toset(["lakehouse/silver/*"])
    )
    error_message = "silver must have only the five object operations and scoped listing on its approved table and backup prefixes."
  }
}

run "gold_storage_has_only_its_approved_prefixes" {
  command = plan
  variables {
    job_writer_layers_prepared = true
  }
  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_writer_layer_storage["gold"].policy).Statement) == 2 &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["gold"].policy).Statement[0].Action) == toset([
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["gold"].policy).Statement[0].Resource) == toset(["arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*", "arn:aws:s3:::example-project-ci-bucket/lakehouse/catalog_backups/*"]) &&
      jsondecode(aws_iam_role_policy.job_writer_layer_storage["gold"].policy).Statement[1].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_layer_storage["gold"].policy).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["gold"].policy).Statement[1].Condition.StringLike["s3:prefix"]) == toset(["lakehouse/gold/*", "lakehouse/catalog_backups/*"])
    )
    error_message = "gold must have only the five object operations and scoped listing on its approved table and backup prefixes."
  }
}

run "features_storage_has_only_its_approved_prefixes" {
  command = plan
  variables {
    job_writer_layers_prepared = true
  }
  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.job_writer_layer_storage["features"].policy).Statement) == 2 &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["features"].policy).Statement[0].Action) == toset([
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ]) &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["features"].policy).Statement[0].Resource) == toset(["arn:aws:s3:::example-project-ci-bucket/lakehouse/features/*"]) &&
      jsondecode(aws_iam_role_policy.job_writer_layer_storage["features"].policy).Statement[1].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_layer_storage["features"].policy).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      toset(jsondecode(aws_iam_role_policy.job_writer_layer_storage["features"].policy).Statement[1].Condition.StringLike["s3:prefix"]) == toset(["lakehouse/features/*"])
    )
    error_message = "features must have only the five object operations and scoped listing on its approved table and backup prefixes."
  }
}

run "three_layer_preparation_does_not_prepare_bronze" {
  command = plan
  variables {
    job_writer_layers_prepared = true
  }
  assert {
    condition = (
      length(aws_iam_role.bronze_writer) == 0 &&
      length(aws_iam_role_policy.bronze_writer) == 0 &&
      length(aws_iam_role_policy.job_writer_stage) == 3 &&
      length(jsondecode(aws_iam_role_policy.job_writer_execution[0].policy).Statement[0].Resource) == 6
    )
    error_message = "The three-layer switch must not bypass bronze preparation or inject the bronze pair."
  }
  assert {
    condition = output.job_writer_layer_contract.prepared == {
      silver   = true
      gold     = true
      features = true
      bronze   = false
    }
    error_message = "The exported preparation flags must follow the independent layer and bronze switches."
  }
}

run "bronze_preparation_does_not_prepare_other_layers" {
  command = plan
  variables {
    job_writer_layers_prepared      = false
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-project-ci-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-project-ci-bucket/lakehouse/bronze_dictionary/"
    }
  }
  assert {
    condition = (
      toset(keys(output.job_roles)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer", "write_bronze_task"]) &&
      !can(aws_iam_role_policy.job_writer_stage["silver"].role) &&
      !can(aws_iam_role_policy.job_writer_stage["gold"].role) &&
      !can(aws_iam_role_policy.job_writer_stage["features"].role) &&
      aws_iam_role_policy.job_writer_stage["bronze"].role == aws_iam_role.bronze_writer[0].name &&
      toset(jsondecode(aws_iam_role_policy.job_writer_execution[0].policy).Statement[0].Resource) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_bronze_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_bronze_client_secret"
      ])
    )
    error_message = "Bronze preparation attaches only bronze's stage read and two execution parameters."
  }
  assert {
    condition = output.job_writer_layer_contract.prepared == {
      silver   = false
      gold     = false
      features = false
      bronze   = true
    }
    error_message = "The exported preparation flags must follow the independent layer and bronze switches."
  }
}

run "production_parameter_rights_follow_configured_names" {
  command = plan
  variables {
    job_writer_layers_prepared      = true
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-project-ci-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-project-ci-bucket/lakehouse/bronze_dictionary/"
    }
    aws_polaris_silver_client_id_name       = "/example_project/production/aws_polaris_silver_client_id"
    aws_polaris_silver_client_secret_name   = "/example_project/production/aws_polaris_silver_client_secret"
    aws_polaris_gold_client_id_name         = "/example_project/production/aws_polaris_gold_client_id"
    aws_polaris_gold_client_secret_name     = "/example_project/production/aws_polaris_gold_client_secret"
    aws_polaris_features_client_id_name     = "/example_project/production/aws_polaris_features_client_id"
    aws_polaris_features_client_secret_name = "/example_project/production/aws_polaris_features_client_secret"
    aws_polaris_bronze_client_id_name       = "/example_project/production/aws_polaris_bronze_client_id"
    aws_polaris_bronze_client_secret_name   = "/example_project/production/aws_polaris_bronze_client_secret"
  }
  assert {
    condition = toset(jsondecode(aws_iam_role_policy.job_writer_execution[0].policy).Statement[0].Resource) == toset([
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_silver_client_id",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_silver_client_secret",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_gold_client_id",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_gold_client_secret",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_features_client_id",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_features_client_secret",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_bronze_client_id",
      "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/production/aws_polaris_bronze_client_secret",
    ])
    error_message = "The attached execution policy must use each configured production parameter name."
  }
}

run "silver_client_id_cannot_alias_another_identity" {
  command = plan
  variables {
    job_writer_layers_prepared        = true
    aws_polaris_reader_client_id_name = "/healthcare_hai_prediction/aws_polaris_silver_client_id"
  }
  expect_failures = [aws_iam_role.job_write_layer_task]
}

run "silver_client_secret_cannot_alias_another_identity" {
  command = plan
  variables {
    job_writer_layers_prepared = true
    polaris_root_secret_name   = "/healthcare_hai_prediction/aws_polaris_silver_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_layer_task, output.project_secret_names]
}

run "test_preparation_checks_silver_pair_distinctness" {
  command = plan
  variables {
    job_writer_e2e_prepared               = true
    bronze_qualification_collection       = "cms/hospitals"
    aws_polaris_reader_client_secret_name = "/healthcare_hai_prediction/aws_polaris_silver_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_e2e_task]
}

run "silver_client_id_refuses_wildcard_names" {
  command = plan
  variables {
    aws_polaris_silver_client_id_name = "/healthcare_hai_prediction/*/aws_polaris_silver_client_id"
  }
  expect_failures = [var.aws_polaris_silver_client_id_name]
}

run "silver_client_secret_refuses_wildcard_names" {
  command = plan
  variables {
    aws_polaris_silver_client_secret_name = "/healthcare_hai_prediction/*/aws_polaris_silver_client_secret"
  }
  expect_failures = [var.aws_polaris_silver_client_secret_name]
}

run "gold_client_id_cannot_alias_another_identity" {
  command = plan
  variables {
    job_writer_layers_prepared        = true
    aws_polaris_reader_client_id_name = "/healthcare_hai_prediction/aws_polaris_gold_client_id"
  }
  expect_failures = [aws_iam_role.job_write_layer_task]
}

run "gold_client_secret_cannot_alias_another_identity" {
  command = plan
  variables {
    job_writer_layers_prepared = true
    polaris_root_secret_name   = "/healthcare_hai_prediction/aws_polaris_gold_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_layer_task, output.project_secret_names]
}

run "test_preparation_checks_gold_pair_distinctness" {
  command = plan
  variables {
    job_writer_e2e_prepared               = true
    bronze_qualification_collection       = "cms/hospitals"
    aws_polaris_reader_client_secret_name = "/healthcare_hai_prediction/aws_polaris_gold_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_e2e_task]
}

run "gold_client_id_refuses_wildcard_names" {
  command = plan
  variables {
    aws_polaris_gold_client_id_name = "/healthcare_hai_prediction/*/aws_polaris_gold_client_id"
  }
  expect_failures = [var.aws_polaris_gold_client_id_name]
}

run "gold_client_secret_refuses_wildcard_names" {
  command = plan
  variables {
    aws_polaris_gold_client_secret_name = "/healthcare_hai_prediction/*/aws_polaris_gold_client_secret"
  }
  expect_failures = [var.aws_polaris_gold_client_secret_name]
}

run "features_client_id_cannot_alias_another_identity" {
  command = plan
  variables {
    job_writer_layers_prepared        = true
    aws_polaris_reader_client_id_name = "/healthcare_hai_prediction/aws_polaris_features_client_id"
  }
  expect_failures = [aws_iam_role.job_write_layer_task]
}

run "features_client_secret_cannot_alias_another_identity" {
  command = plan
  variables {
    job_writer_layers_prepared = true
    polaris_root_secret_name   = "/healthcare_hai_prediction/aws_polaris_features_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_layer_task]
}

run "test_preparation_checks_features_pair_distinctness" {
  command = plan
  variables {
    job_writer_e2e_prepared               = true
    bronze_qualification_collection       = "cms/hospitals"
    aws_polaris_reader_client_secret_name = "/healthcare_hai_prediction/aws_polaris_features_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_e2e_task]
}

run "features_client_id_refuses_wildcard_names" {
  command = plan
  variables {
    aws_polaris_features_client_id_name = "/healthcare_hai_prediction/*/aws_polaris_features_client_id"
  }
  expect_failures = [var.aws_polaris_features_client_id_name]
}

run "features_client_secret_refuses_wildcard_names" {
  command = plan
  variables {
    aws_polaris_features_client_secret_name = "/healthcare_hai_prediction/*/aws_polaris_features_client_secret"
  }
  expect_failures = [var.aws_polaris_features_client_secret_name]
}

run "bronze_live_location_mismatch_blocks_preparation" {
  command = plan
  variables {
    job_writer_layers_prepared      = false
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-project-ci-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-project-ci-bucket/lakehouse/silver/"
    }
  }
  expect_failures = [aws_iam_role.bronze_writer]
}

run "bronze_pair_cannot_alias_the_aws_reader" {
  command = plan
  variables {
    job_writer_layers_prepared      = false
    bronze_writer_prepared          = true
    bronze_qualification_collection = "cms/hospitals"
    bronze_live_locations = {
      bronze            = "s3://example-project-ci-bucket/lakehouse/bronze/"
      bronze_dictionary = "s3://example-project-ci-bucket/lakehouse/bronze_dictionary/"
    }

    aws_polaris_reader_client_id_name = "/healthcare_hai_prediction/aws_polaris_bronze_client_id"
  }
  expect_failures = [aws_iam_role.bronze_writer]
}

run "catalog_cutover_fenced_refuses_true_string" {
  command = plan
  variables {
    catalog_cutover_fenced = "true"
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_false_string" {
  command = plan
  variables {
    catalog_cutover_fenced = "false"
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_text" {
  command = plan
  variables {
    catalog_cutover_fenced = "enabled"
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_number" {
  command = plan
  variables {
    catalog_cutover_fenced = 1
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_zero" {
  command = plan
  variables {
    catalog_cutover_fenced = 0
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_null" {
  command = plan
  variables {
    catalog_cutover_fenced = null
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_list" {
  command = plan
  variables {
    catalog_cutover_fenced = []
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "catalog_cutover_fenced_refuses_map" {
  command = plan
  variables {
    catalog_cutover_fenced = {}
  }
  expect_failures = [var.catalog_cutover_fenced]
}

run "aws_writers_fenced_refuses_true_string" {
  command = plan
  variables {
    aws_writers_fenced = "true"
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_false_string" {
  command = plan
  variables {
    aws_writers_fenced = "false"
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_text" {
  command = plan
  variables {
    aws_writers_fenced = "enabled"
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_number" {
  command = plan
  variables {
    aws_writers_fenced = 1
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_zero" {
  command = plan
  variables {
    aws_writers_fenced = 0
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_null" {
  command = plan
  variables {
    aws_writers_fenced = null
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_list" {
  command = plan
  variables {
    aws_writers_fenced = []
  }
  expect_failures = [var.aws_writers_fenced]
}

run "aws_writers_fenced_refuses_map" {
  command = plan
  variables {
    aws_writers_fenced = {}
  }
  expect_failures = [var.aws_writers_fenced]
}

run "fence_requests_alone_prepare_no_writer" {
  command = plan
  variables {
    catalog_cutover_fenced = true
    aws_writers_fenced     = true
  }
  assert {
    condition = (
      output.job_writer_layer_contract.fence_requests == { catalog_cutover_fenced = true, aws_writers_fenced = true } &&
      toset(keys(output.job_roles)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer"]) &&
      length(aws_iam_role_policy.job_writer_execution) == 0
    )
    error_message = "Fence variables are requests for later integration and must not prepare writer roles or grants in this batch."
  }
}

run "layer_preparation_refuses_mac_reader_alias" {
  command = plan
  variables {
    job_writer_layers_prepared    = true
    polaris_reader_client_id_name = "/healthcare_hai_prediction/aws_polaris_silver_client_id"
  }
  expect_failures = [aws_iam_role.job_write_layer_task, output.project_secret_names]
}

run "layer_preparation_refuses_mac_silver_alias" {
  command = plan
  variables {
    job_writer_layers_prepared        = true
    polaris_silver_client_secret_name = "/healthcare_hai_prediction/aws_polaris_silver_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_layer_task, output.project_secret_names]
}

run "layer_preparation_refuses_mac_gold_alias" {
  command = plan
  variables {
    job_writer_layers_prepared      = true
    polaris_gold_client_secret_name = "/healthcare_hai_prediction/aws_polaris_gold_client_secret"
  }
  expect_failures = [aws_iam_role.job_write_layer_task, output.project_secret_names]
}

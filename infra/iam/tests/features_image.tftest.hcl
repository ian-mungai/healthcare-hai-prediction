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

run "the_features_image_grant_defaults_off" {
  command = plan

  assert {
    condition = (
      !var.job_features_image_prepared &&
      length(aws_iam_role_policy.job_features_image_pull) == 0 &&
      length(aws_iam_role_policy.train_image_pull) == 0
    )
    error_message = "The features image grant defaults to off and the execution role pulls no feature store image."
  }
}

run "the_features_image_grant_needs_the_layer_run_grants" {
  command = plan

  variables {
    job_features_image_prepared = true
  }

  expect_failures = [var.job_features_image_prepared]
}

run "the_features_image_grant_names_one_repository_and_needs_no_training" {
  command = plan

  variables {
    job_writer_layers_prepared  = true
    job_layer_runs_prepared     = true
    job_features_image_prepared = true
  }

  assert {
    condition = (
      length(aws_iam_role_policy.job_features_image_pull) == 1 &&
      length(aws_iam_role_policy.train_image_pull) == 0 &&
      aws_iam_role_policy.job_features_image_pull[0].role == aws_iam_role.job["execution"].name &&
      length(jsondecode(aws_iam_role_policy.job_features_image_pull[0].policy).Statement) == 1 &&
      jsondecode(aws_iam_role_policy.job_features_image_pull[0].policy).Statement[0].Action == ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"] &&
      jsondecode(aws_iam_role_policy.job_features_image_pull[0].policy).Statement[0].Resource == ["arn:aws:ecr:eu-west-1:111111111111:repository/example_project_feature_store_dev"]
    )
    error_message = "The features image grant lets the execution role pull the feature store repository and no other, and it does not turn on the training grants."
  }
}

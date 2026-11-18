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


# The restore after a teardown: three task roles, each with the smallest grant its step needs. Every case reads the plan only.
run "default_prepares_no_restore_roles" {
  command = plan

  assert {
    condition     = length(aws_iam_role.restore_after_teardown) == 0 && length(aws_iam_role_policy.restore_after_teardown) == 0 && length(output.restore_after_teardown_roles) == 0
    error_message = "The restore roles exist only while restore_after_teardown_prepared is true."
  }
}

run "the_backup_role_creates_objects_once_and_nothing_else" {
  command = plan

  variables {
    restore_after_teardown_prepared = true
  }

  assert {
    condition = (
      aws_iam_role.restore_after_teardown["postgres_backup"].name == "example_project_postgres_backup_task_dev"
      && jsondecode(aws_iam_role.restore_after_teardown["postgres_backup"].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
      && length(jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_backup"].policy).Statement) == 1
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_backup"].policy).Statement[0].Action == ["s3:PutObject"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_backup"].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/pg_backups/*"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_backup"].policy).Statement[0].Condition == { StringEquals = { "s3:if-none-match" = "*" } }
    )
    error_message = "The backup role may only put objects under pg_backups/, and only where the key does not exist: no read, no listing, no delete."
  }
}

run "the_restore_role_reads_and_lists_the_backup_prefix_only" {
  command = plan

  variables {
    restore_after_teardown_prepared = true
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_restore"].policy).Statement) == 2
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_restore"].policy).Statement[0].Action == ["s3:GetObject"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_restore"].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/pg_backups/*"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_restore"].policy).Statement[1].Action == ["s3:ListBucket"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_restore"].policy).Statement[1].Resource == ["arn:aws:s3:::example-project-ci-bucket"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["postgres_restore"].policy).Statement[1].Condition == { StringLike = { "s3:prefix" = ["pg_backups/*"] } }
    )
    error_message = "The restore role has one read of pg_backups/* and a listing limited to that prefix: no write."
  }
}

run "the_mlflow_user_role_reads_exactly_three_parameters" {
  command = plan

  variables {
    restore_after_teardown_prepared = true
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.restore_after_teardown["mlflow_user"].policy).Statement) == 1
      && jsondecode(aws_iam_role_policy.restore_after_teardown["mlflow_user"].policy).Statement[0].Action == ["ssm:GetParameter"]
      && jsondecode(aws_iam_role_policy.restore_after_teardown["mlflow_user"].policy).Statement[0].Resource == [
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/mlflow_admin_password",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_user",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_credential",
      ]
    )
    error_message = "The MLflow user role reads the administrator password and the two model API login parameters, and nothing else."
  }
}

run "no_restore_role_holds_a_wildcard_or_a_delete" {
  command = plan

  variables {
    restore_after_teardown_prepared = true
  }

  assert {
    condition = alltrue([
      for name in ["postgres_backup", "postgres_restore", "mlflow_user"] :
      !strcontains(aws_iam_role_policy.restore_after_teardown[name].policy, "\"Resource\":[\"*\"]") && !strcontains(aws_iam_role_policy.restore_after_teardown[name].policy, ":*\"],\"Sid") && !strcontains(aws_iam_role_policy.restore_after_teardown[name].policy, "Delete") && !strcontains(aws_iam_role_policy.restore_after_teardown[name].policy, "s3:*")
    ])
    error_message = "No restore role may hold a wildcard action, a wildcard resource or a delete."
  }

  assert {
    condition     = length(aws_iam_role.restore_after_teardown) == 3 && contains(keys(aws_iam_role.restore_after_teardown), "mlflow_user")
    error_message = "The roles output names the three roles for the services stack."
  }
}

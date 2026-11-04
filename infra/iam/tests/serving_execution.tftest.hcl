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

# D18 model releases: GitHub registers serving task definition revisions, and the revisions name this role as their execution role. It is
# trusted by ECS tasks only and reads none of the control-plane secrets, so a crafted task definition cannot pull them.
run "the_serving_execution_role_is_off_until_the_model_api_is_prepared" {
  command = plan

  assert {
    condition     = length(aws_iam_role.serving_execution) == 0 && length(aws_iam_role_policy.serving_execution) == 0 && output.serving_execution_role == null
    error_message = "The default plan must create no serving execution role."
  }
}

run "the_serving_execution_role_is_trusted_by_ecs_tasks_only" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition     = aws_iam_role.serving_execution[0].name == "example_project_serving_execution_dev"
    error_message = "The role keeps the project name pattern."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role.serving_execution[0].assume_role_policy).Statement) == 1 &&
      alltrue([
        for statement in jsondecode(aws_iam_role.serving_execution[0].assume_role_policy).Statement :
        statement.Effect == "Allow" &&
        statement.Action == "sts:AssumeRole" &&
        join(",", keys(statement.Principal)) == "Service" &&
        statement.Principal.Service == "ecs-tasks.amazonaws.com" &&
        statement.Condition.StringEquals["aws:SourceAccount"] == "111111111111"
      ])
    )
    error_message = "Only ecs-tasks.amazonaws.com, from this account, may assume the serving execution role. No AWS principal, no other service."
  }
}

run "the_serving_execution_role_injects_the_serving_parameters_and_nothing_else" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition = (
      join(",", sort(flatten([
        for statement in jsondecode(aws_iam_role_policy.serving_execution[0].policy).Statement : flatten([statement.Resource]) if statement.Sid == "InjectServingParameters"
        ]))) == join(",", sort([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_serving_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_serving_client_secret",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_key_hashes",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_credential",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_user",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/streamlit_model_api_key"
      ]))
    )
    error_message = "The role injects exactly the six serving parameters."
  }

  assert {
    condition = !anytrue([
      for name in [
        var.postgres_password_name, var.airflow_db_password_name, var.marquez_db_password_name, var.airflow_fernet_key_name, var.airflow_jwt_secret_name,
        var.airflow_api_secret_key_name, var.redis_password_name, var.polaris_root_secret_name, var.grafana_admin_password_name, var.mlflow_db_password_name,
        var.mlflow_admin_password_name, var.broker_token_name
      ] : strcontains(aws_iam_role_policy.serving_execution[0].policy, name)
    ])
    error_message = "The serving execution role can read none of the control-plane secrets."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.serving_execution[0].policy).Statement :
      statement.Effect == "Allow" && !anytrue([for action in flatten([statement.Action]) : startswith(action, "secretsmanager:") || startswith(action, "kms:") || action == "ssm:GetParameter" || action == "ssm:GetParametersByPath" || action == "*"])
    ])
    error_message = "The role holds no Secrets Manager, KMS or single-parameter read, and no wildcard action."
  }
}

run "the_serving_execution_role_pulls_two_images_and_writes_two_log_streams" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition = (
      join(",", sort(flatten([
        for statement in jsondecode(aws_iam_role_policy.serving_execution[0].policy).Statement : flatten([statement.Resource]) if statement.Sid == "PullServingImages"
        ]))) == join(",", sort([
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_model_api_dev",
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_streamlit_dev"
      ])) &&
      join(",", sort(flatten([
        for statement in jsondecode(aws_iam_role_policy.serving_execution[0].policy).Statement : flatten([statement.Resource]) if statement.Sid == "WriteServingLogs"
        ]))) == join(",", sort([
        "arn:aws:logs:eu-west-1:111111111111:log-group:example_project_airflow_dev:log-stream:model_api/*",
        "arn:aws:logs:eu-west-1:111111111111:log-group:example_project_airflow_dev:log-stream:streamlit/*"
      ]))
    )
    error_message = "The role pulls the model API and dashboard images and writes only their own log streams, in the shared log group that the services stack gives them. The shared role's project-wide patterns are not used."
  }
}

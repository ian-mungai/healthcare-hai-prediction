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


run "model_api_role_and_prediction_writes_default_off" {
  command = plan

  assert {
    condition     = !var.model_api_prepared && !var.model_api_prediction_writes
    error_message = "The model API role and its prediction writes must default to false."
  }

  assert {
    condition     = length(aws_iam_role.model_api_task) == 0 && length(aws_iam_role_policy.model_api_read) == 0 && length(aws_iam_role_policy.model_api_prediction_writes) == 0
    error_message = "The default plan must create no model API role and no policy."
  }

  assert {
    condition     = output.model_api_contract.prepared == false && output.model_api_contract.prediction_writes == false && output.model_api_contract.task_role == null
    error_message = "The contract output must say that nothing is prepared."
  }
}

run "model_api_role_reads_only_without_prediction_writes" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition     = aws_iam_role.model_api_task[0].name == "example_project_model_api_task_dev" && aws_iam_role.model_api_task[0].max_session_duration == 3600
    error_message = "The role keeps the project name pattern and a one-hour session."
  }

  assert {
    condition     = length(aws_iam_role_policy.model_api_prediction_writes) == 0
    error_message = "Without the writes switch the role has no write policy."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.model_api_read[0].policy).Statement :
      alltrue([for action in tolist(statement.Action) : startswith(action, "s3:Get") || action == "s3:ListBucket"])
    ])
    error_message = "The read policy may only get objects and list the bucket."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.model_api_read[0].policy, "lakehouse/serving") && strcontains(aws_iam_role_policy.model_api_read[0].policy, "arn:aws:s3:::example-project-ci-bucket/lakehouse/feast/registry.pb")
    error_message = "The read policy names the registry object and never the prediction table prefix."
  }

  assert {
    condition     = strcontains(aws_iam_role_policy.model_api_read[0].policy, "arn:aws:s3:::example-project-ci-bucket/serving/deployments/*") && strcontains(aws_iam_role_policy.model_api_read[0].policy, "s3:ListBucket")
    error_message = "The role reads the deployment records and may list their prefix, so an absent record reads as absent and not as denied."
  }
}

run "model_api_prediction_writes_cover_the_serving_prefix_only" {
  command = plan

  variables {
    model_api_prepared          = true
    model_api_prediction_writes = true
    model_api_registry_key      = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix   = "serving/deployments"
  }

  assert {
    condition = (
      length(aws_iam_role_policy.model_api_prediction_writes) == 1 &&
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.model_api_prediction_writes[0].policy).Statement :
        alltrue([for resource in tolist(statement.Resource) : resource == "arn:aws:s3:::example-project-ci-bucket" || startswith(resource, "arn:aws:s3:::example-project-ci-bucket/lakehouse/serving/")])
      ])
    )
    error_message = "Every resource of the write policy is the bucket (for listing) or an object under lakehouse/serving/."
  }

  assert {
    condition = (
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.model_api_prediction_writes[0].policy).Statement :
        statement.Effect == "Allow" && !contains(tolist(statement.Action), "s3:DeleteObject") && !contains(tolist(statement.Action), "s3:*")
      ]) &&
      anytrue([for statement in jsondecode(aws_iam_role_policy.model_api_prediction_writes[0].policy).Statement : contains(tolist(statement.Action), "s3:PutObject")])
    )
    error_message = "The write policy puts and aborts uploads but deletes nothing and uses no wildcard action."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.model_api_prediction_writes[0].policy).Statement :
      alltrue([for prefix in ["lakehouse/silver", "lakehouse/gold", "lakehouse/bronze", "lakehouse/feast", "lakehouse/catalog_backups", "serving/deployments", "mlflow", "data_stage"] : !strcontains(jsonencode(statement.Resource), prefix)])
    ])
    error_message = "No other prefix of the bucket may appear in the write policy."
  }

  assert {
    condition = (
      alltrue([for statement in jsondecode(aws_iam_role_policy.model_api_read[0].policy).Statement : !contains(tolist(statement.Action), "s3:PutObject")]) &&
      output.model_api_contract.prediction_writes == true && output.model_api_contract.write_prefix == "lakehouse/serving/"
    )
    error_message = "The read policy stays read only and the contract names the single write prefix."
  }
}

run "model_api_writes_without_the_role_are_refused" {
  command = plan

  variables {
    model_api_prediction_writes = true
  }

  expect_failures = [var.model_api_prediction_writes]
}

run "model_api_role_without_its_read_locations_is_refused" {
  command = plan

  variables {
    model_api_prepared = true
  }

  expect_failures = [var.model_api_registry_key, var.model_api_manifest_prefix]
}

run "model_api_wildcard_or_rooted_locations_are_refused" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "lakehouse/*"
  }

  expect_failures = [var.model_api_manifest_prefix]
}

run "model_api_reading_the_prediction_table_prefix_as_a_deployment_prefix_is_refused" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "lakehouse/serving/deployments"
  }

  expect_failures = [var.model_api_manifest_prefix]
}

run "execution_role_injects_the_six_serving_parameters_only_while_prepared" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.task_execution_model_api) == 0 && !strcontains(aws_iam_role_policy.task_execution.policy, "model_api") && !strcontains(aws_iam_role_policy.task_execution.policy, "streamlit")
    error_message = "While the model API is not prepared the execution role holds no serving parameter, in the base policy or in a separate one."
  }
}

run "execution_role_names_exactly_the_six_serving_parameters" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition = (
      aws_iam_role_policy.task_execution_model_api[0].name == "task_execution_model_api" &&
      jsondecode(aws_iam_role_policy.task_execution_model_api[0].policy).Statement == [
        {
          Sid    = "InjectServingParameters"
          Effect = "Allow"
          Action = "ssm:GetParameters"
          Resource = [
            "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_key_hashes",
            "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_serving_client_id",
            "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_serving_client_secret",
            "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_user",
            "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_credential",
            "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/streamlit_model_api_key",
          ]
        }
      ]
    )
    error_message = "The execution role may inject exactly the key hashes, the serving writer pair, the MLflow pair and the page key, and read nothing else."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.task_execution.policy, "serving") && !strcontains(aws_iam_role_policy.task_execution.policy, "model_api")
    error_message = "The base execution policy stays unchanged."
  }
}

run "serving_parameter_names_stay_under_the_project_prefix" {
  command = plan

  variables {
    model_api_prepared               = true
    model_api_registry_key           = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix        = "serving/deployments"
    streamlit_model_api_key_name     = "/healthcare_hai_prediction/*"
    model_api_mlflow_user_name       = "/healthcare_hai_prediction/model_api_mlflow_user"
    model_api_mlflow_credential_name = "/healthcare_hai_prediction/model_api_mlflow_credential"
  }

  expect_failures = [var.streamlit_model_api_key_name]
}

run "the_page_role_exists_only_with_the_model_api_and_grants_nothing" {
  command = plan

  assert {
    condition     = length(aws_iam_role.streamlit_task) == 0 && output.model_api_contract.streamlit_task_role == null
    error_message = "Without model_api_prepared the page role must not exist."
  }
}

run "the_page_role_holds_no_policy" {
  command = plan

  variables {
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition = (
      aws_iam_role.streamlit_task[0].name == "example_project_streamlit_task_dev" &&
      jsondecode(aws_iam_role.streamlit_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com" &&
      aws_iam_role.streamlit_task[0].name != aws_iam_role.model_api_task[0].name
    )
    error_message = "The page has its own role, assumed by ECS tasks only and named apart from the model API role."
  }

  assert {
    condition     = aws_iam_role_policy.model_api_read[0].role == aws_iam_role.model_api_task[0].name
    error_message = "The read policy belongs to the model API role and not to the page role."
  }
}

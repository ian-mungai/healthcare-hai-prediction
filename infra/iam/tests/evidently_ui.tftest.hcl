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

# The Evidently UI shows the snapshots of the live drift check from a read-only folder on the host and calls no AWS service. Its task role exists only to give the
# service a role of its own, apart from every other service, and it holds no policy at all. Every case reads the plan only.
run "default_prepares_no_evidently_ui_role" {
  command = plan

  assert {
    condition     = !var.evidently_ui_prepared && length(aws_iam_role.evidently_ui_task) == 0 && output.evidently_ui_role == null
    error_message = "The Evidently UI role exists only while evidently_ui_prepared is true."
  }
}

run "the_evidently_ui_role_is_assumed_by_ecs_tasks_and_holds_no_policy" {
  command = plan

  variables {
    evidently_ui_prepared = true
  }

  assert {
    condition = (
      length(aws_iam_role.evidently_ui_task) == 1 &&
      aws_iam_role.evidently_ui_task[0].name == "example_project_evidently_ui_task_dev" &&
      jsondecode(aws_iam_role.evidently_ui_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com" &&
      length(jsondecode(aws_iam_role.evidently_ui_task[0].assume_role_policy).Statement) == 1 &&
      aws_iam_role.evidently_ui_task[0].max_session_duration == 3600
    )
    error_message = "The role is the Evidently UI task role, assumed only by ECS tasks."
  }

  assert {
    condition     = output.evidently_ui_role.name == "example_project_evidently_ui_task_dev"
    error_message = "The services root takes the role from the evidently_ui_role output as evidently_ui_task_role_arn."
  }

  assert {
    condition = alltrue([
      for key, role in { host_code_fetch = try(aws_iam_role.host_code_fetch_task[0].name, "none"), model_api = try(aws_iam_role.model_api_task[0].name, "none"), streamlit = try(aws_iam_role.streamlit_task[0].name, "none") } :
      role != aws_iam_role.evidently_ui_task[0].name
    ])
    error_message = "The UI role is named apart from every other service role."
  }
}

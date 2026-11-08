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

# The code one-shot on the container host: one task role that reads the reviewed code archive of a run and nothing else. Every case reads the plan only.
run "default_prepares_no_code_fetch_role" {
  command = plan

  assert {
    condition     = !var.host_code_fetch_prepared && length(aws_iam_role.host_code_fetch_task) == 0 && length(aws_iam_role_policy.host_code_fetch_task) == 0
    error_message = "The code one-shot role exists only while host_code_fetch_prepared is true."
  }
}

run "the_code_fetch_role_reads_only_the_code_archive" {
  command = plan

  variables {
    host_code_fetch_prepared = true
  }

  assert {
    condition = (
      length(aws_iam_role.host_code_fetch_task) == 1
      && aws_iam_role.host_code_fetch_task[0].name == "example_project_host_code_fetch_task_dev"
      && jsondecode(aws_iam_role.host_code_fetch_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
    )
    error_message = "The role is the host code fetch task role, assumed only by ECS tasks."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.host_code_fetch_task[0].policy).Statement) == 1
      && jsondecode(aws_iam_role_policy.host_code_fetch_task[0].policy).Statement[0].Effect == "Allow"
      && jsondecode(aws_iam_role_policy.host_code_fetch_task[0].policy).Statement[0].Action == ["s3:GetObject"]
      && jsondecode(aws_iam_role_policy.host_code_fetch_task[0].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/code.tar.gz"]
    )
    error_message = "The only grant is a read of the staged code archive under run_stage/*/inputs: no write, no listing and no other prefix."
  }

  assert {
    condition     = output.host_code_fetch_role.name == "example_project_host_code_fetch_task_dev"
    error_message = "The services root takes the role from the host_code_fetch_role output."
  }
}

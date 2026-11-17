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

# Prometheus history in S3: a shipper role that only creates and lists blocks under prometheus/blocks/, and a restore role that only reads and lists them. Every
# case reads the plan only.
run "default_prepares_no_prometheus_history_roles" {
  command = plan

  assert {
    condition = (
      !var.prometheus_history_prepared
      && length(aws_iam_role.prometheus_history_task) == 0 && length(aws_iam_role.prometheus_restore_task) == 0
      && length(aws_iam_role_policy.prometheus_history_task) == 0 && length(aws_iam_role_policy.prometheus_restore_task) == 0
      && output.prometheus_history_roles == null
    )
    error_message = "The two roles exist only while prometheus_history_prepared is true."
  }
}

run "the_shipper_creates_and_lists_blocks_and_nothing_else" {
  command = plan

  variables {
    prometheus_history_prepared = true
  }

  assert {
    condition = (
      aws_iam_role.prometheus_history_task[0].name == "example_project_prometheus_history_task_dev"
      && jsondecode(aws_iam_role.prometheus_history_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
    )
    error_message = "The shipper role is assumed only by ECS tasks."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.prometheus_history_task[0].policy).Statement) == 2
      && jsondecode(aws_iam_role_policy.prometheus_history_task[0].policy).Statement[0].Action == "s3:PutObject"
      && jsondecode(aws_iam_role_policy.prometheus_history_task[0].policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/prometheus/blocks/*"
      && jsondecode(aws_iam_role_policy.prometheus_history_task[0].policy).Statement[0].Condition.StringEquals["s3:if-none-match"] == "*"
      && jsondecode(aws_iam_role_policy.prometheus_history_task[0].policy).Statement[1].Action == "s3:ListBucket"
      && jsondecode(aws_iam_role_policy.prometheus_history_task[0].policy).Statement[1].Condition.StringLike["s3:prefix"] == ["prometheus/blocks/*"]
    )
    error_message = "The shipper may only create objects once under prometheus/blocks/ and list that prefix: no read, no delete, no overwrite."
  }
}

run "the_restore_role_reads_and_lists_blocks_and_never_writes" {
  command = plan

  variables {
    prometheus_history_prepared = true
  }

  assert {
    condition = (
      aws_iam_role.prometheus_restore_task[0].name == "example_project_prometheus_restore_task_dev"
      && jsondecode(aws_iam_role.prometheus_restore_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
    )
    error_message = "The restore role is assumed only by ECS tasks."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.prometheus_restore_task[0].policy).Statement) == 2
      && jsondecode(aws_iam_role_policy.prometheus_restore_task[0].policy).Statement[0].Action == "s3:GetObject"
      && jsondecode(aws_iam_role_policy.prometheus_restore_task[0].policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/prometheus/blocks/*"
      && jsondecode(aws_iam_role_policy.prometheus_restore_task[0].policy).Statement[1].Action == "s3:ListBucket"
      && jsondecode(aws_iam_role_policy.prometheus_restore_task[0].policy).Statement[1].Condition.StringLike["s3:prefix"] == ["prometheus/blocks/*"]
      && !strcontains(aws_iam_role_policy.prometheus_restore_task[0].policy, "Put")
      && !strcontains(aws_iam_role_policy.prometheus_restore_task[0].policy, "Delete")
    )
    error_message = "The restore role may only read and list prometheus/blocks/: no write of any kind."
  }

  assert {
    condition = (
      output.prometheus_history_roles.shipper.name == "example_project_prometheus_history_task_dev"
      && output.prometheus_history_roles.restore.name == "example_project_prometheus_restore_task_dev"
      && output.prometheus_history_roles.shipper.name != output.prometheus_history_roles.restore.name
    )
    error_message = "The services root takes the two roles from the prometheus_history_roles output, and they are two roles."
  }
}

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

  mock_resource "aws_vpc_endpoint" {
    defaults = { prefix_list_id = "pl-0123456789abcdef0" }
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

override_resource {
  target          = aws_vpc.platform
  override_during = plan
  values          = { id = "vpc-0123456789abcdef0" }
}

override_resource {
  target          = aws_security_group.host
  override_during = plan
  values          = { id = "sg-00000000000000001" }
}

override_resource {
  target          = aws_security_group.job["offline"]
  override_during = plan
  values          = { id = "sg-00000000000000002" }
}

override_resource {
  target          = aws_security_group.job["lakehouse"]
  override_during = plan
  values          = { id = "sg-00000000000000003" }
}

override_resource {
  target          = aws_security_group.job["endpoints"]
  override_during = plan
  values          = { id = "sg-00000000000000004" }
}

override_resource {
  target          = aws_security_group.job["ssm_endpoint"]
  override_during = plan
  values          = { id = "sg-00000000000000005" }
}

override_resource {
  target          = aws_subnet.public
  override_during = plan
  values          = { id = "subnet-0123456789abcdef0" }
}

override_resource {
  target          = aws_iam_role.job["execution"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLEEXECUTION" }
}

override_resource {
  target          = aws_iam_role.job["offline_task"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLEOFFLINETASK" }
}

override_resource {
  target          = aws_iam_role.job["seal_task"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLESEALTASK" }
}

override_resource {
  target          = aws_iam_role.job["read_task"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLEREADTASK" }
}

override_resource {
  target          = aws_iam_role.job["try_writer"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLETRYWRITER" }
}

override_resource {
  target          = aws_route_table.public
  override_during = plan
  values          = { id = "rtb-0123456789abcdef0" }
}

override_resource {
  target          = aws_security_group.task
  override_during = plan
  values          = { id = "sg-00000000000000006" }
}

override_resource {
  target          = aws_iam_role.broker_task
  override_during = plan
  values          = { unique_id = "AROEXAMPLEBROKERTASK" }
}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
  owner_ip_cidrs      = ["192.0.2.10/32"]
  host_ingress_ports  = [8443]
  broker_prepared     = true
}

run "broker_is_off_by_default" {
  command = plan

  variables {
    broker_prepared = false
  }

  assert {
    condition = (
      length(aws_iam_role.broker_task) == 0 && length(aws_iam_role_policy.broker_jobs) == 0 && length(aws_iam_role_policy.broker_parameter) == 0 &&
      output.broker_roles == {}
    )
    error_message = "Without broker_prepared the IAM root adds no broker role and no broker grant."
  }
}

run "broker_role_is_narrow" {
  command = plan

  assert {
    condition = (
      aws_iam_role.broker_task[0].name == "example_project_broker_task_dev" &&
      aws_iam_role.broker_task[0].assume_role_policy == local.ecs_tasks_trust &&
      aws_iam_role.broker_task[0].max_session_duration == 3600 &&
      lookup(coalesce(aws_iam_role.broker_task[0].tags, {}), "Project", "") == "example_project" &&
      aws_iam_role_policy.broker_jobs[0].role == aws_iam_role.broker_task[0].name &&
      aws_iam_role_policy.broker_parameter[0].role == aws_iam_role.task_execution.name
    )
    error_message = "The broker role carries the project prefix, the ECS task trust, a one hour session cap and the Project tag, and the token grant sits on the host execution role."
  }
}

run "default_family_is_train_only" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement[0].Resource == ["arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train:*"] &&
      jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement[0].Condition.StringEquals["aws:RequestTag/HaiJob"] == ["train"]
    )
    error_message = "Without other families the run grant names the train definition and tag only."
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
      ]
    )
    error_message = "The pass grant names the execution role and the train task role only."
  }
}

run "named_families_widen_the_grant_by_name_only" {
  command = plan

  variables {
    broker_job_families = ["gold", "silver", "train"]
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement[0].Resource == [
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_gold:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_silver:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train:*",
      ] &&
      [for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev",
      ]
    )
    error_message = "Each named family adds its own task definition and its own task role, and nothing else."
  }
}

run "promote_family_reuses_the_train_role" {
  command = plan

  variables {
    broker_job_families = ["promote", "train"]
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement[0].Resource == [
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_promote:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train:*",
      ] &&
      [for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
      ]
    )
    error_message = "The promote job has its own task definition and tag and passes the training task role, with no new role."
  }

  assert {
    condition = [
      for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadStagedRunRecords"
      ][0] == [
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/code.sha256",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/promotion_approval.json",
    ]
    error_message = "The broker reads the staged code digest and the promotion approval of a run and no other object."
  }
}

run "no_statement_holds_a_wildcard_action_or_resource_except_list_tasks" {
  command = plan

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement :
      !contains(flatten([statement.Action]), "*") && !anytrue([for action in flatten([statement.Action]) : endswith(action, ":*")]) &&
      (statement.Sid == "ListClusterTasks" || !contains(flatten([statement.Resource]), "*"))
    ])
    error_message = "Only ListTasks takes a resource wildcard, and it is bound to the cluster. No action is a wildcard."
  }
}

run "stop_and_describe_need_the_job_tag" {
  command = plan

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Condition.StringEquals["aws:ResourceTag/HaiJob"] if statement.Sid == "StopAndDescribeNamedJobTasks"][0] == ["train"] &&
      [for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Action if statement.Sid == "StopAndDescribeNamedJobTasks"][0] == ["ecs:StopTask", "ecs:DescribeTasks"]
    )
    error_message = "Stop and describe apply to tasks that carry a named job tag only, so service tasks are out of reach."
  }
}

run "token_grant_is_one_parameter" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.broker_parameter[0].policy).Statement[0].Resource == "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/broker_token" &&
      jsondecode(aws_iam_role_policy.broker_parameter[0].policy).Statement[0].Action == "ssm:GetParameters"
    )
    error_message = "The host execution role may inject exactly the broker token parameter."
  }
}

run "unknown_family_is_refused" {
  command = plan

  variables {
    broker_job_families = ["train", "write_gold"]
  }

  expect_failures = [var.broker_job_families]
}

run "duplicate_family_is_refused" {
  command = plan

  variables {
    broker_job_families = ["train", "train"]
  }

  expect_failures = [var.broker_job_families]
}

run "no_family_is_refused_while_prepared" {
  command = plan

  variables {
    broker_job_families = []
  }

  expect_failures = [var.broker_job_families]
}

run "token_name_under_the_aws_prefix_is_refused" {
  command = plan

  variables {
    broker_token_name = "/aws/broker_token"
  }

  expect_failures = [var.broker_token_name]
}

run "metadata_family_passes_the_catalog_role" {
  command = plan

  variables {
    broker_job_families = ["metadata", "train"]
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement[0].Resource == [
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_metadata:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train:*",
      ] &&
      [for statement in jsondecode(aws_iam_role_policy.broker_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
      ]
    )
    error_message = "The metadata job has its own task definition and tag and passes the read-only catalog role."
  }
}

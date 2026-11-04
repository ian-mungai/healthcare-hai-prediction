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
  target          = aws_iam_role.airflow_launch_task
  override_during = plan
  values          = { unique_id = "AROEXAMPLEAIRFLOWLAUNCH" }
}

variables {
  aws_profile             = "example_project_dev"
  environment             = "dev"
  admin_profile           = "example_admin"
  aws_region              = "eu-west-1"
  expected_account_id     = "111111111111"
  data_bucket_name        = "example-project-ci-bucket"
  project_name            = "example_project"
  owner_ip_cidrs          = ["192.0.2.10/32"]
  host_ingress_ports      = [8443]
  airflow_launch_prepared = true
}

run "airflow_launch_is_off_by_default" {
  command = plan

  variables {
    airflow_launch_prepared = false
  }

  assert {
    condition = (
      length(aws_iam_role.airflow_launch_task) == 0 && length(aws_iam_role_policy.airflow_launch_jobs) == 0 &&
      length(aws_iam_role_policy.airflow_launch_records) == 0 && length(aws_iam_role_policy.airflow_launch_requests) == 0 &&
      output.airflow_launch_roles == {}
    )
    error_message = "Without airflow_launch_prepared the IAM root adds no launch role and no launch grant."
  }
}

run "airflow_launch_role_is_narrow" {
  command = plan

  assert {
    condition = (
      aws_iam_role.airflow_launch_task[0].name == "example_project_airflow_launch_task_dev" &&
      aws_iam_role.airflow_launch_task[0].assume_role_policy == local.ecs_tasks_trust &&
      aws_iam_role.airflow_launch_task[0].max_session_duration == 3600 &&
      lookup(coalesce(aws_iam_role.airflow_launch_task[0].tags, {}), "Project", "") == "example_project" &&
      aws_iam_role_policy.airflow_launch_jobs[0].role == aws_iam_role.airflow_launch_task[0].name &&
      aws_iam_role_policy.airflow_launch_records[0].role == aws_iam_role.airflow_launch_task[0].name
    )
    error_message = "The launch role carries the project prefix, the ECS task trust, a one hour session cap and the Project tag, and every grant sits on this role only."
  }

  assert {
    condition     = jsonencode(output.airflow_launch_roles["airflow_launch_task"].families) == jsonencode(["train"]) && output.airflow_launch_roles["airflow_launch_task"].record_prefix == "lakehouse/launch_records/"
    error_message = "The output names the role, the families and the record prefix, so the services stack and the owner read the same values."
  }
}

run "default_family_is_train_host_only" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Sid == "RunNamedHostJobs" &&
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Resource == ["arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train_host:*"] &&
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Condition.StringEquals["aws:RequestTag/HaiJob"] == ["train"]
    )
    error_message = "Without other families the run grant names the train host definition and the train tag only, and no Fargate definition."
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
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
    airflow_launch_families = ["gold", "silver", "train"]
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Resource == [
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_gold_host:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_silver_host:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train_host:*",
      ] &&
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Condition.StringEquals["aws:RequestTag/HaiJob"] == ["gold", "silver", "train"] &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev",
      ]
    )
    error_message = "Each named family adds its own host definition, its own tag and its own task role, and nothing else."
  }
}

run "every_family_passes_its_own_role_and_no_other" {
  command = plan

  variables {
    airflow_launch_families = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev",
      ] &&
      length(jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Resource) == 9 &&
      alltrue([for arn in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Resource : endswith(arn, "_host:*")])
    )
    error_message = "All nine families pass the execution role and eight task roles (promote reuses train), and every run grant names a host definition."
  }
}

run "the_launch_role_never_passes_itself_or_the_broker_role" {
  command = plan

  variables {
    airflow_launch_families = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]
  }

  assert {
    condition = !anytrue([
      for arn in [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] :
      strcontains(arn, "airflow_launch") || strcontains(arn, "broker") || strcontains(arn, "task_execution") || strcontains(arn, "control_plane")
    ])
    error_message = "The pass grant never names the launch role, the broker role, the host execution role or the control plane role."
  }
}

run "no_statement_holds_a_wildcard_action_or_resource_except_the_cluster_lists" {
  command = plan

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement :
      !contains(flatten([statement.Action]), "*") && !anytrue([for action in flatten([statement.Action]) : endswith(action, ":*")]) &&
      (contains(["ListClusterTasks", "ListClusterContainerInstances"], statement.Sid) || !contains(flatten([statement.Resource]), "*"))
    ])
    error_message = "Only the two cluster lists take a resource wildcard, and each is bound to the cluster. No action is a wildcard."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement :
      !contains(flatten([statement.Action]), "*") && alltrue([for resource in flatten([statement.Resource]) : resource != "arn:aws:s3:::example-project-ci-bucket/*"])
    ])
    error_message = "No statement takes the whole bucket."
  }
}

run "every_statement_but_the_reads_has_a_condition" {
  command = plan

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement :
      contains(["ReadStagedRunRecords", "ReadJobLogs", "ReadSignedPublishRecord"], statement.Sid) || can(statement.Condition)
    ])
    error_message = "Each launch, stop, describe, list and pass grant carries a condition. Only the three reads of fixed objects go without one."
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition.ArnEquals["ecs:cluster"] if statement.Sid == "RunNamedHostJobs"][0] == "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition["ForAllValues:StringEquals"]["aws:TagKeys"] if statement.Sid == "RunNamedHostJobs"][0] == ["Project", "HaiJob"] &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition.StringEquals["iam:PassedToService"] if statement.Sid == "PassNamedJobRoles"][0] == "ecs-tasks.amazonaws.com"
    )
    error_message = "The run grant is bound to the cluster, the Project tag and exactly the two tag keys, and the pass grant to ECS tasks."
  }
}

run "stop_and_describe_need_the_job_tag" {
  command = plan

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition.StringEquals["aws:ResourceTag/HaiJob"] if statement.Sid == "StopAndDescribeNamedJobTasks"][0] == ["train"] &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "StopAndDescribeNamedJobTasks"][0] == ["ecs:StopTask", "ecs:DescribeTasks"] &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "StopAndDescribeNamedJobTasks"][0] == "arn:aws:ecs:eu-west-1:111111111111:task/example_project_cluster_dev/*"
    )
    error_message = "Stop and describe apply to tasks of the cluster that carry a named job tag only, so service tasks are out of reach."
  }
}

run "container_instance_checks_stay_in_the_cluster" {
  command = plan

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "ListClusterContainerInstances"][0] == "ecs:ListContainerInstances" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition if statement.Sid == "ListClusterContainerInstances"][0] == { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } } &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "DescribeClusterContainerInstances"][0] == "ecs:DescribeContainerInstances" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "DescribeClusterContainerInstances"][0] == "arn:aws:ecs:eu-west-1:111111111111:container-instance/example_project_cluster_dev/*" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition if statement.Sid == "DescribeClusterContainerInstances"][0] == { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } }
    )
    error_message = "The fit check lists and describes the container instances of the project cluster and no other cluster, with no action beyond those two."
  }
}

run "run_records_and_signed_record_reads_are_exact_objects" {
  command = plan

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadStagedRunRecords"][0] == [
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/code.sha256",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/promotion_approval.json",
      ] &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "ReadStagedRunRecords"][0] == "s3:GetObject"
    )
    error_message = "The launch role reads the staged code digest and the promotion approval of a run and no other object, with one get action."
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadSignedPublishRecord"][0] == "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/first_publishes_signed" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "ReadSignedPublishRecord"][0] == "ssm:GetParameter" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadJobLogs"][0] == "arn:aws:logs:eu-west-1:111111111111:log-group:example_project_jobs_dev:log-stream:*"
    )
    error_message = "The launch role reads exactly the signed publish record parameter and the log streams of the jobs log group."
  }
}

run "signed_record_follows_the_named_parameter" {
  command = plan

  variables {
    broker_signed_parameter_name = "/example_project/signed_record"
  }

  assert {
    condition     = [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadSignedPublishRecord"][0] == "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/signed_record"
    error_message = "The signed record grant follows the signed parameter name."
  }
}

run "launch_records_are_written_once_under_their_prefix" {
  command = plan

  assert {
    condition = jsondecode(aws_iam_role_policy.airflow_launch_records[0].policy).Statement == [
      {
        Sid       = "WriteLaunchRecordsOnce"
        Effect    = "Allow"
        Action    = "s3:PutObject"
        Resource  = "arn:aws:s3:::example-project-ci-bucket/lakehouse/launch_records/*"
        Condition = { StringEquals = { "s3:if-none-match" = "*" } }
      },
      {
        Sid      = "ReadLaunchRecords"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "arn:aws:s3:::example-project-ci-bucket/lakehouse/launch_records/*"
      },
    ]
    error_message = "A launch record is created with a conditional write that fails when the object exists, is read back and is never replaced or deleted, under the record prefix only."
  }
}

run "the_record_prefix_follows_the_variable" {
  command = plan

  variables {
    airflow_launch_record_prefix = "example/records/"
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_records[0].policy).Statement : statement.Resource][0] == "arn:aws:s3:::example-project-ci-bucket/example/records/*" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_records[0].policy).Statement : statement.Resource][1] == "arn:aws:s3:::example-project-ci-bucket/example/records/*" &&
      output.airflow_launch_roles["airflow_launch_task"].record_prefix == "example/records/"
    )
    error_message = "Both record grants and the output follow the record prefix variable."
  }
}

run "no_request_grant_without_the_bronze_or_serve_family" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.airflow_launch_requests) == 0
    error_message = "Without the bronze or serve family the launch role holds no read grant on a staged request."
  }
}

run "the_bronze_and_serve_families_add_one_request_read_each" {
  command = plan

  variables {
    airflow_launch_families = ["bronze", "serve", "train"]
  }

  assert {
    condition = (
      aws_iam_role_policy.airflow_launch_requests[0].role == aws_iam_role.airflow_launch_task[0].name &&
      jsondecode(aws_iam_role_policy.airflow_launch_requests[0].policy).Statement == [
        {
          Sid    = "ReadStagedRequests"
          Effect = "Allow"
          Action = "s3:GetObject"
          Resource = [
            "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/bronze_request.json",
            "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/serving_request.json",
          ]
        }
      ]
    )
    error_message = "Bronze adds the staged bronze request and serve adds the staged serving request, one get action on exactly those keys."
  }
}

run "the_bronze_family_alone_adds_the_bronze_request_only" {
  command = plan

  variables {
    airflow_launch_families = ["bronze", "train"]
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.airflow_launch_requests[0].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/bronze_request.json"]
    error_message = "The bronze family adds the bronze request key and not the serving request key."
  }
}

run "unknown_family_is_refused" {
  command = plan

  variables {
    airflow_launch_families = ["train", "write_gold"]
  }

  expect_failures = [var.airflow_launch_families]
}

run "duplicate_family_is_refused" {
  command = plan

  variables {
    airflow_launch_families = ["train", "train"]
  }

  expect_failures = [var.airflow_launch_families]
}

run "no_family_is_refused_while_prepared" {
  command = plan

  variables {
    airflow_launch_families = []
  }

  expect_failures = [var.airflow_launch_families]
}

run "record_prefix_with_a_leading_slash_is_refused" {
  command = plan

  variables {
    airflow_launch_record_prefix = "/lakehouse/launch_records/"
  }

  expect_failures = [var.airflow_launch_record_prefix]
}

run "record_prefix_without_a_trailing_slash_is_refused" {
  command = plan

  variables {
    airflow_launch_record_prefix = "lakehouse/launch_records"
  }

  expect_failures = [var.airflow_launch_record_prefix]
}

run "record_prefix_with_a_wildcard_is_refused" {
  command = plan

  variables {
    airflow_launch_record_prefix = "lakehouse/*/"
  }

  expect_failures = [var.airflow_launch_record_prefix]
}

run "record_prefix_with_a_parent_step_is_refused" {
  command = plan

  variables {
    airflow_launch_record_prefix = "lakehouse/../"
  }

  expect_failures = [var.airflow_launch_record_prefix]
}

run "empty_record_prefix_is_refused" {
  command = plan

  variables {
    airflow_launch_record_prefix = ""
  }

  expect_failures = [var.airflow_launch_record_prefix]
}

run "record_prefix_with_a_policy_variable_is_refused" {
  command = plan

  variables {
    airflow_launch_record_prefix = "records/$${aws:username}/"
  }

  expect_failures = [var.airflow_launch_record_prefix]
}

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
      length(aws_iam_role_policy.airflow_launch_bronze_record) == 0 && length(aws_iam_role_policy.airflow_launch_code_runs) == 0 &&
      length(aws_iam_role_policy.airflow_launch_drift_records) == 0 && output.airflow_launch_roles == {}
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

run "no_statement_holds_a_wildcard_action_or_resource_except_the_task_list_and_the_revision_read" {
  command = plan

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement :
      !contains(flatten([statement.Action]), "*") && !anytrue([for action in flatten([statement.Action]) : endswith(action, ":*")]) &&
      (contains(["ListClusterTasks", "DescribeHostJobRevisions"], statement.Sid) || !contains(flatten([statement.Resource]), "*"))
    ])
    error_message = "Only the task list and the revision read take a resource wildcard. The container instance list names the cluster ARN. No action is a wildcard."
  }

  # ecs:DescribeTaskDefinition takes no resource-level permission, so the preflight's read of the latest host revision (host launch capacity binding,
  # owner A, Nov 10 2026) names "*". It is that one read action, held to the stack's region.
  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "DescribeHostJobRevisions"][0] == "ecs:DescribeTaskDefinition" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "DescribeHostJobRevisions"][0] == "*" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition if statement.Sid == "DescribeHostJobRevisions"][0] == { StringEquals = { "aws:RequestedRegion" = "eu-west-1" } }
    )
    error_message = "The revision read is ecs:DescribeTaskDefinition alone, on the only resource the action takes, in the stack's region."
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
      contains(["ReadStagedRunRecords", "ReadJobLogs", "ReadHostJobCap", "ListClusterContainerInstances"], statement.Sid) || can(statement.Condition)
    ])
    error_message = "Each launch, stop, describe, task list and pass grant carries a condition. Only the three reads of fixed objects and the container instance list, which names the cluster ARN, go without one."
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
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ListClusterContainerInstances"][0] == "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" &&
      !anytrue([for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : can(statement.Condition) if statement.Sid == "ListClusterContainerInstances"]) &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "DescribeClusterContainerInstances"][0] == "ecs:DescribeContainerInstances" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "DescribeClusterContainerInstances"][0] == "arn:aws:ecs:eu-west-1:111111111111:container-instance/example_project_cluster_dev/*" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Condition if statement.Sid == "DescribeClusterContainerInstances"][0] == { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } }
    )
    error_message = "The fit check lists the container instances on the project cluster ARN (AWS supplies no ecs:cluster key for that call, so the statement holds no condition) and describes those of the cluster, with no action beyond those two."
  }
}

run "run_records_and_log_reads_are_exact_objects" {
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
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadJobLogs"][0] == "arn:aws:logs:eu-west-1:111111111111:log-group:example_project_jobs_dev:log-stream:*"
    )
    error_message = "The launch role reads exactly the log streams of the jobs log group."
  }
}

run "no_grant_reads_the_signed_record" {
  command = plan

  variables {
    broker_signed_parameter_name = "/example_project/signed_record"
  }

  assert {
    condition = !anytrue(flatten([
      for policy in [aws_iam_role_policy.airflow_launch_jobs[0].policy, aws_iam_role_policy.airflow_launch_records[0].policy] : [
        for statement in jsondecode(policy).Statement : statement.Sid != "ReadHostJobCap" && anytrue([for action in flatten([statement.Action]) : startswith(action, "ssm:")])
      ]
    ]))
    error_message = "Signing the first publishes is a manual step: the launch role reads no Parameter Store value but the host job cap, whatever the broker's signed parameter name is."
  }
}

run "launch_role_reads_only_the_host_job_cap_parameter" {
  command = plan

  # Current host job cap at launch (owner A, Nov 10 2026): one read of one parameter, no batch read, no write and no other name.
  assert {
    condition = (
      length([for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement if statement.Sid == "ReadHostJobCap"]) == 1 &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadHostJobCap"][0] == ["arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/host_job_cap", "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/host_launch_hold", "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/services_apply_lock"] &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Action if statement.Sid == "ReadHostJobCap"][0] == "ssm:GetParameter" &&
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Effect if statement.Sid == "ReadHostJobCap"][0] == "Allow"
    )
    error_message = "The launch role reads exactly the host job cap, launch hold and apply lock parameters, with ssm:GetParameter only."
  }
}

run "host_job_cap_grant_follows_the_named_parameter" {
  command = plan

  variables {
    host_job_cap_parameter_name = "/example_project/host_job_cap"
  }

  assert {
    condition     = contains([for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadHostJobCap"][0], "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/host_job_cap")
    error_message = "The host job cap grant follows the parameter name the services stack writes."
  }
}

run "launch_hold_grant_follows_the_named_parameter" {
  command = plan

  variables {
    host_launch_hold_parameter_name = "/example_project/launch_hold"
  }

  assert {
    condition     = contains([for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "ReadHostJobCap"][0], "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/launch_hold")
    error_message = "The launch role reads the launch hold the services apply command writes, by its parameter name."
  }
}

run "a_host_job_cap_parameter_name_with_a_wildcard_is_refused" {
  command = plan

  variables {
    host_job_cap_parameter_name = "/healthcare_hai_prediction/*"
  }

  expect_failures = [var.host_job_cap_parameter_name]
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
        Sid       = "ReplaceDecisionRecords"
        Effect    = "Allow"
        Action    = "s3:PutObject"
        Resource  = "arn:aws:s3:::example-project-ci-bucket/lakehouse/launch_records/*/decision.json"
        Condition = { Null = { "s3:if-match" = "false" } }
      },
      {
        Sid      = "ReadLaunchRecords"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "arn:aws:s3:::example-project-ci-bucket/lakehouse/launch_records/*"
      },
      {
        Sid       = "ListLaunchRecords"
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = "arn:aws:s3:::example-project-ci-bucket"
        Condition = { StringLike = { "s3:prefix" = ["lakehouse/launch_records/*"] } }
      },
    ]
    error_message = "A launch record is created with a conditional write that fails when the object exists, is read back, is listed under the record prefix only, is replaced only for the decision record of a run and only with a version check, and is never deleted."
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
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_records[0].policy).Statement : statement.Resource][2] == "arn:aws:s3:::example-project-ci-bucket/example/records/*" &&
      jsondecode(aws_iam_role_policy.airflow_launch_records[0].policy).Statement[3].Condition.StringLike["s3:prefix"] == ["example/records/*"] &&
      jsondecode(aws_iam_role_policy.airflow_launch_records[0].policy).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket/example/records/*/decision.json" &&
      output.airflow_launch_roles["airflow_launch_task"].record_prefix == "example/records/"
    )
    error_message = "The record grants, the list condition and the output follow the record prefix variable."
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

run "the_publisher_family_starts_its_host_task_and_reads_its_staged_request" {
  command = plan

  variables {
    airflow_launch_families = ["publisher", "train"]
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Resource == [
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_publisher_host:*",
        "arn:aws:ecs:eu-west-1:111111111111:task-definition/example_project_dev_job_train_host:*",
      ] &&
      contains([for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0], "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev") &&
      jsondecode(aws_iam_role_policy.airflow_launch_requests[0].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/publisher_request.json"]
    )
    error_message = "The publisher family runs only the publisher host definition, passes the publisher task role and reads the staged publisher_request.json, which orchestration/publisher_launch.py checks before the launch. No broker grant is involved."
  }
}

run "no_bronze_record_grant_without_the_bronze_family" {
  command = plan

  variables {
    airflow_launch_families = ["serve", "train"]
  }

  assert {
    condition     = length(aws_iam_role_policy.airflow_launch_bronze_record) == 0
    error_message = "Without the bronze family the launch role holds no read grant on a bronze run record."
  }
}

run "the_bronze_family_reads_only_the_bronze_run_record" {
  command = plan

  variables {
    airflow_launch_families = ["bronze", "train"]
  }

  assert {
    condition = (
      aws_iam_role_policy.airflow_launch_bronze_record[0].role == aws_iam_role.airflow_launch_task[0].name &&
      jsondecode(aws_iam_role_policy.airflow_launch_bronze_record[0].policy).Statement == [
        {
          Sid      = "ReadBronzeRunRecords"
          Effect   = "Allow"
          Action   = "s3:GetObject"
          Resource = "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze_runs/*/check.json"
        }
      ]
    )
    error_message = "The bronze family adds one get action on the run record name lakehouse/bronze_runs/<run>/check.json of the data bucket, with no list and no other key."
  }
}

run "every_launch_role_binds_code_runs_once" {
  command = plan

  assert {
    condition = (
      aws_iam_role_policy.airflow_launch_code_runs[0].role == aws_iam_role.airflow_launch_task[0].name &&
      jsondecode(aws_iam_role_policy.airflow_launch_code_runs[0].policy).Statement == [
        {
          Sid      = "ReadCodeRunBindings"
          Effect   = "Allow"
          Action   = "s3:GetObject"
          Resource = "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/code_run.json"
        },
        {
          Sid       = "BindCodeRunsOnce"
          Effect    = "Allow"
          Action    = "s3:PutObject"
          Resource  = "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/code_run.json"
          Condition = { StringEquals = { "s3:if-none-match" = "*" } }
        },
        {
          Sid       = "ListCodeRunBindings"
          Effect    = "Allow"
          Action    = "s3:ListBucket"
          Resource  = "arn:aws:s3:::example-project-ci-bucket"
          Condition = { StringLike = { "s3:prefix" = ["lakehouse/run_stage/*/inputs/code_run.json"] } }
        },
      ]
    )
    error_message = "Every launch role reads the code run binding of a run, creates it once and lists only that key, so a missing binding reads as absent; nothing else of the staged inputs."
  }
}

run "no_drift_record_read_without_the_drift_family" {
  command = plan

  variables {
    airflow_launch_families = ["promote", "serve", "train"]
  }

  assert {
    condition     = length(aws_iam_role_policy.airflow_launch_drift_records) == 0
    error_message = "Without the drift family the launch role reads no drift record."
  }
}

run "the_drift_family_reads_only_the_drift_records" {
  command = plan

  variables {
    airflow_launch_families = ["drift", "promote", "serve", "train"]
  }

  assert {
    condition = (
      aws_iam_role_policy.airflow_launch_drift_records[0].role == aws_iam_role.airflow_launch_task[0].name &&
      jsondecode(aws_iam_role_policy.airflow_launch_drift_records[0].policy).Statement == [
        {
          Sid      = "ReadDriftRecords"
          Effect   = "Allow"
          Action   = ["s3:GetObject"]
          Resource = "arn:aws:s3:::example-project-ci-bucket/drift/*"
        }
      ]
    )
    error_message = "The drift family adds one get action on the drift records of the data bucket to the launch role, for the drift DAG's decision: no list, no write and no other prefix."
  }

  assert {
    condition = (
      [for statement in jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement : statement.Resource if statement.Sid == "PassNamedJobRoles"][0] == [
        "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_execution_dev",
        "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev",
        "arn:aws:iam::111111111111:role/example_project_job_train_task_dev",
      ] &&
      jsondecode(aws_iam_role_policy.airflow_launch_jobs[0].policy).Statement[0].Condition.StringEquals["aws:RequestTag/HaiJob"] == ["drift", "promote", "serve", "train"] &&
      length(aws_iam_role_policy.airflow_launch_requests) == 1 &&
      jsondecode(aws_iam_role_policy.airflow_launch_requests[0].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/serving_request.json"]
    )
    error_message = "The four families of the Airflow DAG launches pass the drift, serve and train task roles (promote reuses the train role) with the execution role, tag their four jobs and read the staged serving request."
  }
}

run "the_control_plane_role_gains_no_launch_grant" {
  command = plan

  variables {
    airflow_launch_families          = ["drift", "promote", "serve", "train"]
    drift_prepared                   = true
    job_endpoints_enabled            = true
    drift_mlflow_admin_password_name = "/example_project/mlflow_admin_password"
  }

  assert {
    condition = alltrue([
      for policy in concat(
        aws_iam_role_policy.airflow_launch_jobs, aws_iam_role_policy.airflow_launch_records, aws_iam_role_policy.airflow_launch_requests,
        aws_iam_role_policy.airflow_launch_code_runs, aws_iam_role_policy.airflow_launch_drift_records,
      ) : policy.role == aws_iam_role.airflow_launch_task[0].name
    ])
    error_message = "Every launch grant sits on the launch role; the control-plane role of the other Airflow services gains none."
  }

  assert {
    condition     = aws_iam_role_policy.drift_records_read[0].role == aws_iam_role.control_plane_task.name && jsondecode(aws_iam_role_policy.drift_records_read[0].policy).Statement == jsondecode(aws_iam_role_policy.airflow_launch_drift_records[0].policy).Statement
    error_message = "The launch role reads the drift records with the same document as the drift stack's read, and the control-plane read stays as it was."
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

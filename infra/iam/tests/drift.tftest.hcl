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

variables {
  aws_profile                      = "example_project_dev"
  environment                      = "dev"
  admin_profile                    = "example_admin"
  aws_region                       = "eu-west-1"
  expected_account_id              = "111111111111"
  data_bucket_name                 = "example-project-ci-bucket"
  project_name                     = "example_project"
  owner_ip_cidrs                   = ["192.0.2.10/32"]
  host_ingress_ports               = [8443]
  job_endpoints_enabled            = true
  drift_prepared                   = true
  drift_mlflow_admin_password_name = "/example_project/mlflow_admin_password"
}

run "drift_is_off_by_default" {
  command = plan

  variables {
    drift_prepared = false
  }

  assert {
    condition = (
      length(aws_iam_role.drift_task) == 0 && length(aws_iam_role_policy.drift_task) == 0 && length(aws_iam_role_policy.drift_image_pull) == 0 &&
      length(aws_iam_role_policy.drift_parameter) == 0 && length(aws_iam_role_policy.drift_records_read) == 0 && output.drift_roles == {}
    )
    error_message = "Without drift_prepared the IAM root adds no role and no grant for the drift task."
  }
}

run "drift_role_is_narrow" {
  command = plan

  assert {
    condition = (
      aws_iam_role.drift_task[0].name == "example_project_job_drift_task_dev" &&
      toset(keys(aws_iam_role_policy.drift_task)) == toset(["stage_read", "features_read", "dvc_baseline_read", "drift_records"]) &&
      jsondecode(aws_iam_role.drift_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
    )
    error_message = "The drift role is assumed by ECS tasks only and carries stage reads, the feature tables, the DVC remote and the drift records."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.drift_task["drift_records"].policy).Statement[0].Action == ["s3:PutObject"] &&
      jsondecode(aws_iam_role_policy.drift_task["drift_records"].policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/drift/*" &&
      length(jsondecode(aws_iam_role_policy.drift_task["drift_records"].policy).Statement) == 1
    )
    error_message = "The drift task may only create objects under drift/. It cannot read, list or delete them."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.drift_task["dvc_baseline_read"].policy).Statement[0].Action == ["s3:GetObject"] &&
      jsondecode(aws_iam_role_policy.drift_task["dvc_baseline_read"].policy).Statement[1].Action == "s3:ListBucket" &&
      length(jsondecode(aws_iam_role_policy.drift_task["dvc_baseline_read"].policy).Statement) == 2
    )
    error_message = "The drift task only loads a baseline that training pushed: it may read and list the DVC remote and never write it."
  }

  assert {
    condition = alltrue([
      for name, policy in aws_iam_role_policy.drift_task : name == "drift_records" ||
      !anytrue([for statement in jsondecode(policy.policy).Statement : length(setintersection(toset(flatten([statement.Action])), toset(["s3:PutObject", "s3:DeleteObject"]))) > 0 && statement.Effect == "Allow"])
    ])
    error_message = "Only the drift records can be written. The stage, feature and DVC policies grant no write."
  }
}

run "execution_role_gets_exactly_what_the_drift_task_injects" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.drift_image_pull[0].policy).Statement[0].Resource == [
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_training_dev",
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_feature_store_dev",
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_drift_dev",
      ] &&
      jsondecode(aws_iam_role_policy.drift_parameter[0].policy).Statement[0].Action == "ssm:GetParameters" &&
      jsondecode(aws_iam_role_policy.drift_parameter[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/mlflow_admin_password"] &&
      aws_iam_role_policy.drift_image_pull[0].role == aws_iam_role.job["execution"].name && aws_iam_role_policy.drift_parameter[0].role == aws_iam_role.job["execution"].name
    )
    error_message = "The execution role pulls exactly the three images and reads exactly the MLflow password parameter, and only while drift_prepared is true."
  }
}

run "airflow_reads_only_the_drift_records" {
  command = plan

  assert {
    condition = (
      aws_iam_role_policy.drift_records_read[0].role == aws_iam_role.control_plane_task.name &&
      jsondecode(aws_iam_role_policy.drift_records_read[0].policy).Statement[0].Action == ["s3:GetObject"] &&
      jsondecode(aws_iam_role_policy.drift_records_read[0].policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/drift/*" &&
      length(jsondecode(aws_iam_role_policy.drift_records_read[0].policy).Statement) == 1
    )
    error_message = "The control plane role, which runs Airflow, reads objects under drift/ and nothing else."
  }
}

run "drift_roles_output" {
  command = plan

  assert {
    condition     = toset(keys(output.drift_roles)) == toset(["drift_task"]) && output.drift_roles.drift_task.name == "example_project_job_drift_task_dev"
    error_message = "The output names the prepared drift role for the jobs root."
  }
}

run "other_password_name_is_refused" {
  command = plan

  variables {
    drift_mlflow_admin_password_name = "/example_project/aws_polaris_reader_client_secret"
  }

  expect_failures = [var.drift_mlflow_admin_password_name]
}

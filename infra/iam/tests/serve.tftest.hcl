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
  aws_profile               = "example_project_dev"
  environment               = "dev"
  admin_profile             = "example_admin"
  aws_region                = "eu-west-1"
  expected_account_id       = "111111111111"
  data_bucket_name          = "example-project-ci-bucket"
  project_name              = "example_project"
  owner_ip_cidrs            = ["192.0.2.10/32"]
  host_ingress_ports        = [8443]
  job_endpoints_enabled     = true
  training_prepared         = true
  serve_prepared            = true
  model_api_manifest_prefix = "serving/deployments"
}

run "serve_is_off_by_default" {
  command = plan

  variables {
    serve_prepared = false
  }

  assert {
    condition     = length(aws_iam_role.serve_task) == 0 && length(aws_iam_role_policy.serve_task) == 0 && output.serve_roles == {}
    error_message = "Without serve_prepared the IAM root adds no role and no grant for the serve task."
  }
}

run "serve_role_is_narrow" {
  command = plan

  assert {
    condition = (
      aws_iam_role.serve_task[0].name == "example_project_job_serve_task_dev" &&
      toset(keys(aws_iam_role_policy.serve_task)) == toset(["stage_read", "deployments"]) &&
      jsondecode(aws_iam_role.serve_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
    )
    error_message = "The serve role is assumed by ECS tasks only and carries stage reads and the deployment records."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.serve_task["deployments"].policy).Statement[0].Action == ["s3:GetObject", "s3:PutObject"] &&
      jsondecode(aws_iam_role_policy.serve_task["deployments"].policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/serving/deployments/*" &&
      jsondecode(aws_iam_role_policy.serve_task["deployments"].policy).Statement[1].Action == "s3:ListBucket" &&
      jsondecode(aws_iam_role_policy.serve_task["deployments"].policy).Statement[1].Condition.StringLike["s3:prefix"] == ["serving/deployments/*"] &&
      length(jsondecode(aws_iam_role_policy.serve_task["deployments"].policy).Statement) == 2
    )
    error_message = "The serve task reads and writes the deployment records under their one prefix and lists that prefix. It deletes nothing."
  }

  assert {
    condition = alltrue([
      for name, policy in aws_iam_role_policy.serve_task : name == "deployments" ||
      !anytrue([for statement in jsondecode(policy.policy).Statement : length(setintersection(toset(flatten([statement.Action])), toset(["s3:PutObject", "s3:DeleteObject"]))) > 0 && statement.Effect == "Allow"])
    ])
    error_message = "Only the deployment records can be written. The stage policy grants no write."
  }

  assert {
    condition     = output.serve_roles.serve_task.name == "example_project_job_serve_task_dev"
    error_message = "The serve role name reaches the jobs root through the serve_roles output."
  }
}

run "serve_needs_the_training_roles" {
  command = plan

  variables {
    training_prepared = false
  }

  expect_failures = [var.serve_prepared]
}

run "serve_needs_a_deployment_prefix" {
  command = plan

  variables {
    model_api_manifest_prefix = ""
  }

  expect_failures = [var.serve_prepared]
}

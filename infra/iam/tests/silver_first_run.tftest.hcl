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
  aws_profile         = "example_project_dev"
  environment         = "dev"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
  owner_ip_cidrs      = ["192.0.2.10/32"]
  host_ingress_ports  = [8443]
}

# The IAM step of the silver first run, from clean defaults: the two layer flags and the endpoints flag, and nothing else of the layer or training flags.
run "silver_first_run_iam_step_gives_the_task_its_image_pull_path" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
    job_endpoints_enabled      = true
  }

  assert {
    condition = (
      output.job_network.endpoints_enabled == true &&
      length(aws_vpc_endpoint.job_s3) == 1 && length(aws_vpc_endpoint.job_ssm) == 1 &&
      toset(keys(aws_vpc_endpoint.job_interface)) == toset(["ecr.api", "ecr.dkr", "logs"]) &&
      contains(keys(aws_vpc_security_group_egress_rule.job_shared_endpoints), "lakehouse") &&
      contains(keys(aws_vpc_security_group_egress_rule.job_s3), "lakehouse")
    )
    error_message = "With the endpoints flag the lakehouse class reaches ECR, the log service, Parameter Store and S3 without a public address."
  }

  assert {
    condition = (
      contains(keys(aws_iam_role.job_write_layer_task), "silver") &&
      contains(keys(output.job_roles), "write_silver_task") && contains(keys(output.job_roles), "execution") &&
      length(aws_iam_role_policy.job_writer_layer_input) == 3 && length(aws_iam_role_policy.job_writer_layer_upload) == 2
    )
    error_message = "The two layer flags create the silver role, its run grants and the execution role that the task definition names."
  }

  assert {
    condition = (
      strcontains(aws_iam_role_policy.job_writer_execution[0].policy, "aws_polaris_silver_client_id") &&
      strcontains(aws_iam_role_policy.job_writer_execution[0].policy, "aws_polaris_silver_client_secret") &&
      output.job_writer_layer_contract.parameter_names.silver == ["/healthcare_hai_prediction/aws_polaris_silver_client_id", "/healthcare_hai_prediction/aws_polaris_silver_client_secret"]
    )
    error_message = "The execution role injects the silver pair, and the contract output names the two parameters for the jobs file."
  }
}

run "the_two_layer_flags_alone_leave_the_task_without_an_image_pull_path" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      output.job_network.endpoints_enabled == false && length(aws_vpc_endpoint.job_interface) == 0 && length(aws_vpc_endpoint.job_s3) == 0 &&
      length(aws_vpc_security_group_egress_rule.job_shared_endpoints) == 0
    )
    error_message = "Without the endpoints flag no private path to ECR, logs or S3 exists, so the first silver task could not pull its image. The runbook sets the flag in the same step."
  }
}

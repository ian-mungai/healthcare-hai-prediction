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
  target          = aws_security_group.training
  override_during = plan
  values          = { id = "sg-00000000000000007" }
}

override_resource {
  target          = aws_iam_role.train_task
  override_during = plan
  values          = { unique_id = "AROEXAMPLETRAINTASK" }
}

override_resource {
  target          = aws_iam_role.mlflow_task
  override_during = plan
  values          = { unique_id = "AROEXAMPLEMLFLOWTASK" }
}

variables {
  aws_profile           = "example_project_dev"
  environment           = "dev"
  admin_profile         = "example_admin"
  aws_region            = "eu-west-1"
  expected_account_id   = "111111111111"
  data_bucket_name      = "example-project-ci-bucket"
  project_name          = "example_project"
  owner_ip_cidrs        = ["192.0.2.10/32"]
  host_ingress_ports    = [8443]
  job_endpoints_enabled = true
  training_prepared     = true
}

run "training_is_off_by_default" {
  command = plan

  variables {
    training_prepared = false
  }

  assert {
    condition = (
      length(aws_iam_role.train_task) == 0 && length(aws_iam_role.mlflow_task) == 0 && length(aws_security_group.training) == 0 &&
      length(aws_iam_role_policy.train_policy) == 0 && length(aws_iam_role_policy.mlflow_artifacts) == 0 &&
      output.training_network == null && output.training_roles == {}
    )
    error_message = "Without training_prepared the IAM root adds no role, no group and no rule for training."
  }
}

run "training_roles_are_narrow" {
  command = plan

  assert {
    condition = (
      aws_iam_role.train_task[0].name == "example_project_job_train_task_dev" &&
      aws_iam_role.mlflow_task[0].name == "example_project_mlflow_task_dev" &&
      aws_iam_role.train_task[0].assume_role_policy == local.ecs_tasks_trust &&
      aws_iam_role.mlflow_task[0].assume_role_policy == local.ecs_tasks_trust &&
      aws_iam_role.train_task[0].max_session_duration == 3600 && aws_iam_role.mlflow_task[0].max_session_duration == 3600 &&
      lookup(coalesce(aws_iam_role.train_task[0].tags, {}), "Project", "") == "example_project" &&
      lookup(coalesce(aws_iam_role.mlflow_task[0].tags, {}), "Project", "") == "example_project"
    )
    error_message = "Both roles carry the project prefix, the ECS task trust, a one hour session cap and the Project tag."
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.train_policy)) == toset(["stage_read", "features_read", "dvc_baseline"]) &&
      aws_iam_role_policy.train_policy["stage_read"].policy == local.job_policy_documents.job_stage_read &&
      aws_iam_role_policy.train_policy["features_read"].policy == local.training_policy_documents.job_features_read &&
      aws_iam_role_policy.train_policy["dvc_baseline"].policy == local.training_policy_documents.job_dvc_baseline &&
      alltrue([for policy in aws_iam_role_policy.train_policy : policy.role == aws_iam_role.train_task[0].name]) &&
      aws_iam_role_policy.mlflow_artifacts[0].role == aws_iam_role.mlflow_task[0].name &&
      aws_iam_role_policy.mlflow_artifacts[0].policy == local.training_policy_documents.mlflow_artifacts
    )
    error_message = "The training role holds the stage read, the feature read and the DVC grant. The MLflow role holds the mlflow/ artifact grant alone. Neither holds a lakehouse write."
  }

  assert {
    condition     = jsondecode(local.training_policy_documents.mlflow_artifacts).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/mlflow/*"
    error_message = "The MLflow grant names the data bucket mlflow/ prefix."
  }

  assert {
    condition = (
      output.training_roles.train_task.arn == aws_iam_role.train_task[0].arn && output.training_roles.mlflow_task.arn == aws_iam_role.mlflow_task[0].arn &&
      output.training_roles.train_task.unique_id == "AROEXAMPLETRAINTASK" && toset(keys(output.training_roles)) == toset(["train_task", "mlflow_task"]) &&
      !contains(keys(output.job_roles), "train_task") && !contains(keys(output.job_roles), "mlflow_task")
    )
    error_message = "The roles are exported apart from job_roles, whose consumers expect the earlier set."
  }
}

run "training_network_is_closed" {
  command = plan

  assert {
    condition = (
      length(aws_security_group.training[0].ingress) == 0 && length(aws_security_group.training[0].egress) == 0 &&
      aws_security_group.training[0].name == "example_project_job_training_dev" &&
      output.training_network.security_group_id == aws_security_group.training[0].id &&
      output.training_network.subnet_id == aws_subnet.public[0].id &&
      !contains(keys(output.job_network.security_group_ids), "training")
    )
    error_message = "The training group has no inline rule. job_network keeps its two classes, so the ECS target renderer is unchanged."
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.training_mlflow[0].security_group_id == aws_security_group.training[0].id &&
      aws_vpc_security_group_egress_rule.training_mlflow[0].referenced_security_group_id == aws_security_group.host.id &&
      aws_vpc_security_group_egress_rule.training_mlflow[0].from_port == 5100 && aws_vpc_security_group_egress_rule.training_mlflow[0].to_port == 5100 &&
      aws_vpc_security_group_egress_rule.training_mlflow[0].ip_protocol == "tcp" &&
      aws_vpc_security_group_ingress_rule.host_from_training_mlflow[0].security_group_id == aws_security_group.host.id &&
      aws_vpc_security_group_ingress_rule.host_from_training_mlflow[0].referenced_security_group_id == aws_security_group.training[0].id &&
      aws_vpc_security_group_ingress_rule.host_from_training_mlflow[0].from_port == 5100 && aws_vpc_security_group_ingress_rule.host_from_training_mlflow[0].to_port == 5100
    )
    error_message = "Port 5100 of the host accepts only the training group, and the training group reaches only that port of the host for MLflow."
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.training_catalog[0].referenced_security_group_id == aws_security_group.host.id &&
      aws_vpc_security_group_egress_rule.training_catalog[0].from_port == 8181 && aws_vpc_security_group_egress_rule.training_catalog[0].to_port == 8181 &&
      aws_vpc_security_group_ingress_rule.host_from_training_catalog[0].referenced_security_group_id == aws_security_group.training[0].id &&
      aws_vpc_security_group_ingress_rule.host_from_training_catalog[0].from_port == 8181 && aws_vpc_security_group_ingress_rule.host_from_training_catalog[0].to_port == 8181
    )
    error_message = "The extract reads the catalog on port 8181 from the training group only."
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.training_s3[0].prefix_list_id == aws_vpc_endpoint.job_s3[0].prefix_list_id &&
      aws_vpc_security_group_egress_rule.training_s3[0].from_port == 443 &&
      aws_vpc_security_group_egress_rule.training_endpoints[0].referenced_security_group_id == aws_security_group.job["endpoints"].id &&
      aws_vpc_security_group_ingress_rule.training_endpoints[0].security_group_id == aws_security_group.job["endpoints"].id &&
      aws_vpc_security_group_ingress_rule.training_endpoints[0].referenced_security_group_id == aws_security_group.training[0].id &&
      aws_vpc_security_group_egress_rule.training_ssm[0].referenced_security_group_id == aws_security_group.job["ssm_endpoint"].id &&
      aws_vpc_security_group_ingress_rule.training_ssm[0].security_group_id == aws_security_group.job["ssm_endpoint"].id &&
      aws_vpc_security_group_ingress_rule.training_ssm[0].referenced_security_group_id == aws_security_group.training[0].id
    )
    error_message = "Training reaches S3 through the gateway, image and log endpoints and Parameter Store, like the lakehouse class."
  }
}

run "training_without_endpoints_needs_no_endpoint_rules" {
  command = plan

  variables {
    job_endpoints_enabled = false
  }

  assert {
    condition = (
      length(aws_vpc_security_group_egress_rule.training_s3) == 0 && length(aws_vpc_security_group_egress_rule.training_endpoints) == 0 &&
      length(aws_vpc_security_group_ingress_rule.training_endpoints) == 0 && length(aws_vpc_security_group_egress_rule.training_ssm) == 0 &&
      length(aws_vpc_security_group_ingress_rule.training_ssm) == 0 && length(aws_vpc_security_group_egress_rule.training_mlflow) == 1
    )
    error_message = "Turning the endpoints off removes their rules. The catalog and MLflow rules need no endpoint."
  }
}

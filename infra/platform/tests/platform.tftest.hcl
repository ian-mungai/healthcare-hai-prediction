mock_provider "aws" {
  override_during = plan

  # The IAM stack owns the network and the host group; the platform stack finds them by name and tag.
  mock_data "aws_vpc" {
    defaults = {
      id         = "vpc-0123456789abcdef0"
      cidr_block = "10.40.0.0/16"
    }
  }

  mock_data "aws_subnet" {
    defaults = {
      id                = "subnet-0123456789abcdef0"
      availability_zone = "eu-west-1a"
    }
  }

  mock_data "aws_security_group" {
    defaults = {
      id = "sg-0000000000000host0"
    }
  }

  mock_data "aws_autoscaling_group" {
    defaults = {
      arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev"
    }
  }

  mock_resource "aws_ebs_volume" {
    defaults = {
      id = "vol-0123456789abcdef0"
    }
  }

  mock_resource "aws_eip" {
    defaults = {
      allocation_id = "eipalloc-0123456789abcdef0"
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
    }
  }

  mock_resource "aws_ecs_cluster" {
    defaults = {
      arn = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev"
    }
  }
}

# The two subnets and the two security groups come from the same data source type, so each lookup is fixed by name.
override_data {
  target          = data.aws_subnet.public["1"]
  override_during = plan
  values = {
    id                = "subnet-0fedcba9876543210"
    availability_zone = "eu-west-1b"
  }
}

override_data {
  target          = data.aws_security_group.task
  override_during = plan
  values = {
    id = "sg-0000000000000task0"
  }
}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  project_name        = "example_project"
}

# The project profile creates only what the approved permissions list allows. The network, the launch template, the host
# group, the roles and the budget belong to the IAM stack: this stack reads them by their Name and Project tags.
run "lookups_from_the_iam_stack" {
  command = plan

  assert {
    condition = (
      toset([for filter in data.aws_vpc.platform.filter : "${filter.name}=${one(filter.values)}"]) == toset(["tag:Name=example_project_vpc_dev", "tag:Project=example_project"]) &&
      toset([for filter in data.aws_subnet.public["0"].filter : "${filter.name}=${one(filter.values)}"]) == toset(["tag:Name=example_project_public_0_dev", "tag:Project=example_project"]) &&
      toset([for filter in data.aws_subnet.public["1"].filter : "${filter.name}=${one(filter.values)}"]) == toset(["tag:Name=example_project_public_1_dev", "tag:Project=example_project"]) &&
      toset([for filter in data.aws_security_group.host.filter : "${filter.name}=${one(filter.values)}"]) == toset(["tag:Name=example_project_host_dev", "tag:Project=example_project"]) &&
      toset([for filter in data.aws_security_group.task.filter : "${filter.name}=${one(filter.values)}"]) == toset(["tag:Name=example_project_task_dev", "tag:Project=example_project"]) &&
      data.aws_autoscaling_group.host.name == "example_project_host_dev"
    )
    error_message = "The platform stack finds the VPC, both subnets, both security groups and the host group by the Name and Project tags the IAM stack sets."
  }

  assert {
    condition = (
      output.vpc_id == "vpc-0123456789abcdef0" &&
      output.public_subnet_ids == ["subnet-0123456789abcdef0", "subnet-0fedcba9876543210"] &&
      output.host_subnet_id == "subnet-0123456789abcdef0" &&
      output.host_security_group_id == "sg-0000000000000host0" &&
      output.task_security_group_id == "sg-0000000000000task0" &&
      output.host_autoscaling_group_name == "example_project_host_dev"
    )
    error_message = "The outputs the control-plane stack reads keep their names and values."
  }
}

run "image_repositories" {
  command = plan

  assert {
    condition = (
      toset(keys(aws_ecr_repository.image)) == toset([
        "airflow", "analytics", "egress_proxy", "grafana", "marquez", "marquez_web", "polaris", "polaris_admin", "postgres", "prometheus", "quality", "redis", "spark", "statsd_exporter"
      ]) &&
      alltrue([
        for name, repository in aws_ecr_repository.image :
        repository.name == "example_project_${name}_dev" && repository.image_tag_mutability == "IMMUTABLE" &&
        one(repository.image_scanning_configuration).scan_on_push && !repository.force_delete
      ])
    )
    error_message = "Each image gets an immutable, scanned repository that teardown cannot delete with images unless allowed."
  }

  assert {
    condition = alltrue([
      for policy in aws_ecr_lifecycle_policy.image :
      jsondecode(policy.policy).rules[0].selection.tagStatus == "untagged" &&
      jsondecode(policy.policy).rules[0].selection.countNumber == 1 &&
      jsondecode(policy.policy).rules[1].selection.tagStatus == "any" &&
      jsondecode(policy.policy).rules[1].selection.countType == "imageCountMoreThan" &&
      jsondecode(policy.policy).rules[1].selection.countNumber == 5 &&
      length(jsondecode(policy.policy).rules) == 2
    ])
    error_message = "Repositories keep the last five images and expire untagged images after a day."
  }
}

run "cluster_capacity" {
  command = plan

  assert {
    condition = (
      aws_ecs_cluster.platform.name == "example_project_cluster_dev" &&
      toset(aws_ecs_cluster_capacity_providers.platform.capacity_providers) == toset(["example_project_host_dev", "FARGATE", "FARGATE_SPOT"]) &&
      one(aws_ecs_cluster_capacity_providers.platform.default_capacity_provider_strategy).capacity_provider == "FARGATE_SPOT" &&
      aws_ecs_capacity_provider.host.name == "example_project_host_dev" &&
      one(aws_ecs_capacity_provider.host.auto_scaling_group_provider).auto_scaling_group_arn == data.aws_autoscaling_group.host.arn
    )
    error_message = "The cluster offers the host (the IAM stack's group), Fargate Spot and on-demand Fargate, with Spot as the default for run tasks."
  }
}

run "data_volume_and_snapshots" {
  command = plan

  assert {
    condition = (
      aws_ebs_volume.data.type == "gp3" && aws_ebs_volume.data.size == 60 && aws_ebs_volume.data.encrypted && !aws_ebs_volume.data.final_snapshot &&
      aws_ebs_volume.data.availability_zone == data.aws_subnet.public["0"].availability_zone &&
      aws_ebs_volume.data.tags["DataClassification"] == "Confidential" && aws_ebs_volume.data.tags["Name"] == "example_project_host_data_dev" &&
      aws_ebs_volume.data.tags["Project"] == "example_project" && aws_eip.host.tags["Name"] == "example_project_host_dev"
    )
    error_message = "The data volume is a 60 GB encrypted gp3 volume in the host's zone with the exact Project tag in its create call and no final snapshot, which the project profile may not create."
  }

  assert {
    condition = (
      aws_dlm_lifecycle_policy.data.state == "ENABLED" &&
      aws_dlm_lifecycle_policy.data.execution_role_arn == "arn:aws:iam::111111111111:role/example_project_snapshots_dev" &&
      one(aws_dlm_lifecycle_policy.data.policy_details).target_tags["Name"] == "example_project_host_data_dev" &&
      length(one(aws_dlm_lifecycle_policy.data.policy_details).target_tags) == 1 &&
      one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).copy_tags &&
      one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).name == "daily" &&
      one(one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).create_rule).interval == 24 &&
      one(one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).create_rule).interval_unit == "HOURS" &&
      one(one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).retain_rule).count == 7
    )
    error_message = "A daily snapshot of exactly the data volume is taken and the last seven are kept."
  }

  # The snapshot role creates a snapshot only with the exact Project request tag, whether or not tag copying works, so
  # the schedule names the tag itself.
  assert {
    condition = (
      length(one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).tags_to_add) == 2 &&
      one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).tags_to_add["SnapshotCreator"] == "DataLifecycleManager" &&
      one(one(aws_dlm_lifecycle_policy.data.policy_details).schedule).tags_to_add["Project"] == output.project_tag &&
      output.project_tag == "example_project"
    )
    error_message = "The snapshot schedule must add the Project tag itself; the snapshot role refuses untagged snapshots."
  }
}

run "logs_and_alert_topic" {
  command = plan

  assert {
    condition = (
      toset(keys(output.log_group_names)) == toset(["postgres", "redis", "polaris", "airflow", "marquez", "monitoring", "jobs"]) &&
      output.log_group_names["polaris"] == "example_project_polaris_dev" &&
      alltrue([for group in aws_cloudwatch_log_group.platform : group.retention_in_days == 7])
    )
    error_message = "Platform log groups keep logs for seven days."
  }

  assert {
    condition     = aws_sns_topic.alerts.name == "example_project_alerts_dev" && output.alerts_topic_arn == aws_sns_topic.alerts.arn
    error_message = "The run-failure topic has the exact name the project profile's sns policy names; the budget's topic is the IAM stack's."
  }
}

run "role_names_match_the_iam_stack" {
  command = plan

  assert {
    condition = output.role_arns == {
      host               = "arn:aws:iam::111111111111:role/example_project_ecs_host_dev"
      task_execution     = "arn:aws:iam::111111111111:role/example_project_task_execution_dev"
      polaris_task       = "arn:aws:iam::111111111111:role/example_project_polaris_task_dev"
      control_plane_task = "arn:aws:iam::111111111111:role/example_project_control_plane_task_dev"
      snapshots          = "arn:aws:iam::111111111111:role/example_project_snapshots_dev"
    }
    error_message = "Role names must match the IAM stack's names exactly."
  }

  assert {
    condition = (
      output.execution_role_arn == "arn:aws:iam::111111111111:role/example_project_task_execution_dev" &&
      output.polaris_task_role_arn == "arn:aws:iam::111111111111:role/example_project_polaris_task_dev" &&
      output.control_plane_task_role_arn == "arn:aws:iam::111111111111:role/example_project_control_plane_task_dev" &&
      output.polaris_task_role_arn != output.control_plane_task_role_arn &&
      output.cluster_name == "example_project_cluster_dev" &&
      output.ec2_capacity_provider_name == "example_project_host_dev" &&
      output.data_mount_path == "/data" &&
      length(setsubtract(toset(["postgres", "redis", "polaris", "airflow", "marquez", "marquez_web", "spark", "analytics", "quality", "polaris_admin", "egress_proxy", "prometheus", "grafana", "statsd_exporter"]), toset(keys(output.ecr_repository_urls)))) == 0
    )
    error_message = "The outputs the control-plane stack reads must keep their names and values."
  }
}

run "reject_administrator_profile" {
  command = plan

  variables {
    aws_profile = "example_admin"
  }

  expect_failures = [var.aws_profile]
}

run "reject_long_log_retention" {
  command = plan

  variables {
    log_retention_days = 90
  }

  expect_failures = [var.log_retention_days]
}

run "reject_malformed_region" {
  command = plan

  variables {
    aws_region = "eu_west_1"
  }

  expect_failures = [var.aws_region]
}

run "reject_unsupported_environment" {
  command = plan

  variables {
    environment = "development"
  }

  expect_failures = [var.environment]
}

run "reject_data_volume_outside_range" {
  command = plan

  variables {
    data_volume_gb = 5000
  }

  expect_failures = [var.data_volume_gb]
}

run "reject_zero_snapshots_kept" {
  command = plan

  variables {
    snapshot_retain_count = 0
  }

  expect_failures = [var.snapshot_retain_count]
}

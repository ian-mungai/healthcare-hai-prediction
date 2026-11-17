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

# The IAM stack creates the host's network interface; this stack finds it by tag and exports its private address.
override_data {
  target          = data.aws_network_interface.host_address
  override_during = plan
  values = {
    id         = "eni-0123456789abcdef0"
    private_ip = "10.40.0.20"
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
      data.aws_autoscaling_group.host.name == "example_project_host_dev"
    )
    error_message = "The platform stack finds the VPC, both subnets, the host security group and the host group by the Name and Project tags the IAM stack sets."
  }

  assert {
    condition = (
      output.vpc_id == "vpc-0123456789abcdef0" &&
      output.public_subnet_ids == ["subnet-0123456789abcdef0", "subnet-0fedcba9876543210"] &&
      output.host_subnet_id == "subnet-0123456789abcdef0" &&
      output.host_security_group_id == "sg-0000000000000host0" &&
      output.host_autoscaling_group_name == "example_project_host_dev"
    )
    error_message = "The outputs the control-plane stack reads keep their names and values."
  }
}

# The platform settings are the live ones with no preparation file: a plain plan of a clean checkout creates the repositories of every image in
# infra/services/images.json, the monitoring and serving repositories included, and the log groups of the live stack.
run "live_settings_are_the_defaults" {
  command = plan

  assert {
    condition = (
      var.monitoring_enabled && var.serving_enabled && var.scratch_volume_gb == 60 && var.data_volume_gb == 60 &&
      var.snapshot_retain_count == 7 && var.snapshot_time_utc == "09:00" && var.ecr_keep_images == 5 && var.log_retention_days == 7 &&
      !var.allow_destructive_teardown && var.log_groups == toset(["airflow", "jobs", "openmetadata", "mlflow", "polaris", "postgres", "redis"])
    )
    error_message = "The committed defaults must equal the live platform settings."
  }

  assert {
    condition = toset(keys(aws_ecr_repository.image)) == toset([
      "airflow", "analytics", "drift", "egress_proxy", "feature_store", "grafana", "mlflow", "model_api", "node_exporter",
      "openmetadata_ingestion", "openmetadata_search", "openmetadata_server", "polaris", "polaris_admin", "postgres", "postgres_exporter",
      "prometheus", "pushgateway", "quality", "redis", "redis_exporter", "spark", "statsd_exporter", "streamlit", "training",
    ])
    error_message = "The default repositories must be the 25 live ones."
  }

  assert {
    condition     = toset(keys(aws_cloudwatch_log_group.platform)) == toset(["airflow", "jobs", "openmetadata", "mlflow", "monitoring", "polaris", "postgres", "redis"])
    error_message = "The default log groups must be the 8 live ones."
  }
}

run "image_repositories" {
  command = plan

  assert {
    condition = (
      toset(keys(aws_ecr_repository.image)) == toset(keys(jsondecode(file("${path.module}/../services/images.json")).images)) &&
      alltrue([
        for name, repository in aws_ecr_repository.image :
        repository.name == "example_project_${name}_dev" && repository.image_tag_mutability == "IMMUTABLE" &&
        one(repository.image_scanning_configuration).scan_on_push && !repository.force_delete
      ])
    )
    error_message = "Each image of images.json gets an immutable, scanned repository that teardown cannot delete with images unless allowed."
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

# A false flag leaves out only the repositories and log group of that part, so the stack as it was before the part existed is still one input away.
run "monitoring_flag_off_leaves_out_its_foundation" {
  command = plan

  variables {
    monitoring_enabled = false
  }

  assert {
    condition = (
      length(setintersection(toset(["prometheus", "grafana", "statsd_exporter", "pushgateway", "redis_exporter", "postgres_exporter", "node_exporter"]), toset(keys(aws_ecr_repository.image)))) == 0 &&
      !contains(keys(output.log_group_names), "monitoring") &&
      length(aws_ecr_repository.image) == 18 && length(aws_cloudwatch_log_group.platform) == 7
    )
    error_message = "The monitoring flag controls seven repositories (Prometheus, Grafana, the StatsD exporter, the Pushgateway and the Redis, Postgres and node exporters) and one log group, and nothing else."
  }
}

run "serving_flag_off_leaves_out_its_repositories" {
  command = plan

  variables {
    serving_enabled = false
  }

  assert {
    condition = (
      length(setintersection(toset(["model_api", "streamlit"]), toset(keys(aws_ecr_repository.image)))) == 0 &&
      length(aws_ecr_repository.image) == 23 && length(aws_cloudwatch_log_group.platform) == 8
    )
    error_message = "The serving flag controls two repositories and nothing else."
  }
}

# The repository groups of images.json decide which flag keeps a repository, whatever the free text of an entry says.
run "repository_groups_decide_the_gates" {
  command = plan

  variables {
    monitoring_enabled = false
    serving_enabled    = true
  }

  assert {
    condition = (
      toset(keys(aws_ecr_repository.image)) == toset([for name, image in jsondecode(file("${path.module}/../services/images.json")).images : name if image.repository_group != "monitoring"]) &&
      toset([for name, image in jsondecode(file("${path.module}/../services/images.json")).images : name if image.repository_group == "monitoring"]) == toset(["prometheus", "grafana", "statsd_exporter"]) &&
      toset([for name, image in jsondecode(file("${path.module}/../services/images.json")).images : name if image.repository_group == "serving"]) == toset(["model_api", "streamlit"]) &&
      alltrue([for name, image in jsondecode(file("${path.module}/../services/images.json")).images : contains(["always", "monitoring", "serving"], image.repository_group)])
    )
    error_message = "The monitoring group is three images, the serving group two, and a false flag removes exactly its own group."
  }
}

run "both_flags_off_keep_the_base_images" {
  command = plan

  variables {
    monitoring_enabled = false
    serving_enabled    = false
  }

  assert {
    condition = (
      length(aws_ecr_repository.image) == 16 && length(aws_cloudwatch_log_group.platform) == 7 &&
      alltrue([for name in ["airflow", "analytics", "drift", "egress_proxy", "feature_store", "mlflow", "openmetadata_ingestion", "openmetadata_search", "openmetadata_server", "polaris", "polaris_admin", "postgres", "quality", "redis", "spark", "training"] : contains(keys(aws_ecr_repository.image), name)])
    )
    error_message = "With both flags off the 16 base images and the 7 base log groups remain."
  }
}

run "cluster_capacity" {
  command = plan

  assert {
    condition = (
      aws_ecs_cluster.platform.name == "example_project_cluster_dev" &&
      toset(aws_ecs_cluster_capacity_providers.platform.capacity_providers) == toset(["example_project_host_dev"]) &&
      one(aws_ecs_cluster_capacity_providers.platform.default_capacity_provider_strategy).capacity_provider == "example_project_host_dev" &&
      aws_ecs_capacity_provider.host.name == "example_project_host_dev" &&
      one(aws_ecs_capacity_provider.host.auto_scaling_group_provider).auto_scaling_group_arn == data.aws_autoscaling_group.host.arn
    )
    error_message = "The cluster offers only the host (the IAM stack's group) as a capacity provider and as the default for run tasks: no Fargate provider remains."
  }
}

run "data_volume_and_snapshots" {
  command = plan

  assert {
    condition = (
      aws_ebs_volume.data.type == "gp3" && aws_ebs_volume.data.size == 60 && aws_ebs_volume.data.encrypted && !aws_ebs_volume.data.final_snapshot &&
      aws_ebs_volume.data.availability_zone == data.aws_subnet.public["0"].availability_zone &&
      aws_ebs_volume.data.tags["DataClassification"] == "Public" && aws_ebs_volume.data.tags["Name"] == "example_project_host_data_dev" &&
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
      toset(keys(output.log_group_names)) == toset(["postgres", "redis", "polaris", "airflow", "openmetadata", "jobs", "mlflow", "monitoring"]) &&
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

# The project user subscribes to this stack's alert topic with an exact ARN. Terraform builds the topic name, so the policy template is rendered with the stack's own values and
# compared with the ARN built from the name. Among the resources of the alert topics statement, those that begin with the topic ARN are the topic and its subscriptions (:*) only, so a renamed topic or a look-alike name fails here.
run "alert_topic_is_named_in_the_project_sns_policy" {
  command = plan

  assert {
    condition = (
      toset([
        for resource in flatten([
          for statement in jsondecode(replace(replace(replace(replace(file("${path.module}/../../infra/iam/policies/healthcare_hai_prediction_sns_policy.json"), "$${AWS_REGION}", var.aws_region), "$${AWS_ACCOUNT_ID}", var.expected_account_id), "$${PROJECT_NAME}", var.project_name), "$${ENVIRONMENT}", var.environment)).Statement : statement.Resource
          if statement.Sid == "ManageAlertTopics"
        ]) : resource
        if startswith(resource, "arn:aws:sns:${var.aws_region}:${var.expected_account_id}:${aws_sns_topic.alerts.name}")
      ]) ==
      toset(["arn:aws:sns:${var.aws_region}:${var.expected_account_id}:${aws_sns_topic.alerts.name}", "arn:aws:sns:${var.aws_region}:${var.expected_account_id}:${aws_sns_topic.alerts.name}:*"])
    )
    error_message = "The alert topics statement of the project user's SNS policy must name this stack's topic by its exact ARN and its subscriptions (:*) only, and no longer name that begins with it."
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
      length(setsubtract(toset(["postgres", "redis", "polaris", "airflow", "spark", "analytics", "quality", "polaris_admin", "egress_proxy"]), toset(keys(output.ecr_repository_urls)))) == 0
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

# Failure modes: the lookup matches the host's primary interface (same Name as the host) or any project interface; the address is
# not exported, so the services and jobs renderers fail or a person types it; the output is sensitive or empty.
run "host_address_found_by_tag_and_exported" {
  command = plan

  assert {
    condition = (
      toset([for filter in data.aws_network_interface.host_address.filter : "${filter.name}=${one(filter.values)}"]) == toset(["tag:Name=example_project_host_address_dev", "tag:Project=example_project"]) &&
      length(data.aws_network_interface.host_address.filter) == 2
    )
    error_message = "The platform stack finds the host's interface by its own Name and the Project tag, never by the host's Name."
  }

  assert {
    condition     = output.host_private_ip == "10.40.0.20"
    error_message = "The private address of the host's interface is exported as host_private_ip for the services and jobs renderers."
  }
}

run "no_marquez_repository_log_group_or_output" {
  command = plan

  assert {
    condition = (
      !contains(keys(aws_ecr_repository.image), "marquez") && !contains(keys(aws_ecr_repository.image), "marquez_web") &&
      !contains(keys(aws_ecr_lifecycle_policy.image), "marquez") && !contains(keys(aws_ecr_lifecycle_policy.image), "marquez_web") &&
      !contains(keys(aws_cloudwatch_log_group.platform), "marquez") && !contains(keys(output.ecr_repository_urls), "marquez") && !contains(keys(output.log_group_names), "marquez") &&
      contains(keys(output.log_group_names), "openmetadata")
    )
    error_message = "OpenMetadata alone holds lineage: the platform has no Marquez repository, lifecycle policy, log group or output, and OpenMetadata has its own log group."
  }
}

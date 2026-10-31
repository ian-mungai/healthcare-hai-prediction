provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  # The monitoring foundation exists only while its flag is true, so a false flag leaves the stack as it was.
  # The serving flag adds the model API and page repositories the same way, and nothing else.
  monitoring_repositories = var.monitoring_enabled ? setunion(var.ecr_repositories, ["prometheus", "grafana", "statsd_exporter"]) : var.ecr_repositories
  ecr_repositories        = var.serving_enabled ? setunion(local.monitoring_repositories, ["model_api", "streamlit"]) : local.monitoring_repositories
  log_groups              = var.monitoring_enabled ? setunion(var.log_groups, ["monitoring"]) : var.log_groups

  # The Project tag holds the project name as it is, the value the data bucket carries too. Every create of the project
  # profile must send it with exactly this key and value, so the cost budget (in the IAM stack) counts the resource.
  project_tag = var.project_name
  tags = {
    Project     = local.project_tag
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
  name = "${var.project_name}_%s_${var.environment}"

  # The IAM stack creates these roles; both stacks' tests pin the same names.
  host_role_name               = format(local.name, "ecs_host")
  task_execution_role_name     = format(local.name, "task_execution")
  polaris_task_role_name       = format(local.name, "polaris_task")
  control_plane_task_role_name = format(local.name, "control_plane_task")
  snapshot_role_name           = format(local.name, "snapshots")
  role_arn_prefix              = "arn:aws:iam::${var.expected_account_id}:role/"

  data_mount_path     = "/data"
  data_classification = { DataClassification = "Confidential" }
}

# The IAM stack, which an administrator applies first, owns the network, the host launch template, the host Auto Scaling
# group, the roles and the cost budget. They are looked up here by name and tag; the plan fails until that stack has been
# applied. The project profile has no permission to create or change any of them.
data "aws_vpc" "platform" {
  filter {
    name   = "tag:Name"
    values = [format(local.name, "vpc")]
  }

  filter {
    name   = "tag:Project"
    values = [local.project_tag]
  }
}

data "aws_subnet" "public" {
  for_each = toset(["0", "1"])

  vpc_id = data.aws_vpc.platform.id

  filter {
    name   = "tag:Name"
    values = [format(local.name, "public_${each.key}")]
  }

  filter {
    name   = "tag:Project"
    values = [local.project_tag]
  }
}

data "aws_security_group" "host" {
  vpc_id = data.aws_vpc.platform.id

  filter {
    name   = "tag:Name"
    values = [format(local.name, "host")]
  }

  filter {
    name   = "tag:Project"
    values = [local.project_tag]
  }
}

data "aws_security_group" "task" {
  vpc_id = data.aws_vpc.platform.id

  filter {
    name   = "tag:Name"
    values = [format(local.name, "task")]
  }

  filter {
    name   = "tag:Project"
    values = [local.project_tag]
  }
}

# The host's group runs the administrator's launch template at a pinned numeric version. It starts with no instance; the
# project profile turns the host on and off with autoscaling:SetDesiredCapacity, which the group's maximum size of 1 limits.
data "aws_autoscaling_group" "host" {
  name = format(local.name, "host")
}

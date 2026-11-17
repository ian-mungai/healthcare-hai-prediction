provider "aws" {
  profile             = var.aws_profile
  region              = var.aws_region
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  # Every image of images.json gets a repository. The repository_group of an image says which switch keeps it: always, or only while the monitoring or the
  # serving switch is true. A false switch leaves the stack as it was before those parts existed.
  images            = jsondecode(file("${path.module}/../services/images.json")).images
  monitoring_images = toset([for name, image in local.images : name if image.repository_group == "monitoring"])
  serving_images    = toset([for name, image in local.images : name if image.repository_group == "serving"])
  ecr_repositories = setsubtract(
    toset(keys(local.images)),
    setunion(var.monitoring_enabled ? toset([]) : local.monitoring_images, var.serving_enabled ? toset([]) : local.serving_images),
  )
  log_groups = var.monitoring_enabled ? setunion(var.log_groups, ["monitoring"]) : var.log_groups

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
  data_classification = { DataClassification = "Public" }
}

# A repository group other than the three would be kept whatever the switches say, so the plan stops on it. scripts/infrastructure/push_images.py checks the same list.
check "repository_groups" {
  assert {
    condition     = alltrue([for name, image in local.images : contains(["always", "monitoring", "serving"], image.repository_group)])
    error_message = "Every image of infra/services/images.json needs a repository_group of always, monitoring or serving."
  }
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

# The host's private address is the address of a network interface that the IAM stack creates and boot attaches. The interface
# carries its own Name, so the host's primary interface (tagged with the host Name) never matches. The lookup fails until the IAM
# stack has been applied, and its address is exported for the services and jobs stacks, so no person types it.
data "aws_network_interface" "host_address" {
  filter {
    name   = "tag:Name"
    values = [format(local.name, "host_address")]
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

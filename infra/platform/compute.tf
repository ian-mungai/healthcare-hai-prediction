# Container Insights publishes paid custom metrics; the budget design keeps CloudWatch within the free allowance.
#trivy:ignore:AWS-0034
resource "aws_ecs_cluster" "platform" {
  #checkov:skip=CKV_AWS_65:Container Insights publishes paid custom metrics; the budget design keeps CloudWatch within the free allowance.
  name = format(local.name, "cluster")

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_eip" "host" {
  #checkov:skip=CKV2_AWS_19:The host associates this address itself at boot, because the Auto Scaling group owns the instance.
  domain = "vpc"

  tags = {
    Name = format(local.name, "host")
  }
}

# The data volume outlives the instance: a resize or a replaced instance attaches the same volume at boot, which finds it
# by its Name and Project tags. It uses the AWS-managed EBS key; a customer-managed key adds cost and key-policy grants for
# the Auto Scaling service role. No final snapshot: the project profile has no permission to create one, so a destroy would
# fail on it. The daily snapshots of the schedule below and the catalog backups in S3 are the recovery path.
#trivy:ignore:AWS-0027
resource "aws_ebs_volume" "data" {
  #checkov:skip=CKV_AWS_189:Encrypted with the AWS-managed EBS key; a customer-managed key adds cost the budget does not cover.
  availability_zone = data.aws_subnet.public["0"].availability_zone
  type              = "gp3"
  size              = var.data_volume_gb
  encrypted         = true
  final_snapshot    = false

  # The tags are explicit, not only default tags, so they appear in the create call itself: the project profile may create
  # a volume only when the request carries the exact key Project with the project value.
  tags = merge(local.tags, local.data_classification, {
    Name = format(local.name, "host_data")
  })
}

resource "aws_ecs_capacity_provider" "host" {
  name = format(local.name, "host")

  auto_scaling_group_provider {
    auto_scaling_group_arn         = data.aws_autoscaling_group.host.arn
    managed_termination_protection = "DISABLED"
    managed_draining               = "DISABLED"

    managed_scaling {
      status = "DISABLED"
    }
  }
}

resource "aws_ecs_cluster_capacity_providers" "platform" {
  cluster_name       = aws_ecs_cluster.platform.name
  capacity_providers = [aws_ecs_capacity_provider.host.name, "FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }
}

resource "aws_dlm_lifecycle_policy" "data" {
  description        = "Daily snapshots of the container host data volume"
  execution_role_arn = "${local.role_arn_prefix}${local.snapshot_role_name}"
  state              = "ENABLED"

  policy_details {
    resource_types = ["VOLUME"]

    target_tags = {
      Name = format(local.name, "host_data")
    }

    # The snapshot role deletes only snapshots that carry this schedule name; both stacks' tests pin it.
    schedule {
      name      = "daily"
      copy_tags = true

      create_rule {
        interval      = 24
        interval_unit = "HOURS"
        times         = [var.snapshot_time_utc]
      }

      retain_rule {
        count = var.snapshot_retain_count
      }

      # The snapshot role creates snapshots only with this Project request tag, so the schedule supplies it itself
      # instead of relying on tag copying.
      tags_to_add = {
        Project         = local.project_tag
        SnapshotCreator = "DataLifecycleManager"
      }
    }
  }
}

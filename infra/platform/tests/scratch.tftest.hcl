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

# Failure modes pinned before the change:
# - the scratch volume is snapshotted, so job churn fills snapshot storage;
# - the scratch volume sits in another zone than the host, so it cannot attach;
# - the create call lacks the exact Project key and value, which the project profile needs;
# - the volume carries the data volume's Name, so the host finds two data volumes and boot stops, or the snapshot schedule takes it;
# - the size cannot move after the dry-run peak, or accepts a value outside the range.

run "scratch_volume_is_separate_encrypted_and_not_snapshotted" {
  command = plan

  assert {
    condition = (
      aws_ebs_volume.scratch.type == "gp3" && aws_ebs_volume.scratch.size == 60 && aws_ebs_volume.scratch.encrypted && !aws_ebs_volume.scratch.final_snapshot &&
      aws_ebs_volume.scratch.availability_zone == data.aws_subnet.public["0"].availability_zone &&
      aws_ebs_volume.scratch.tags["Name"] == "example_project_host_scratch_dev" && aws_ebs_volume.scratch.tags["Project"] == "example_project" &&
      aws_ebs_volume.scratch.tags["DataClassification"] == "Public"
    )
    error_message = "The scratch volume is a 60 GB encrypted gp3 volume in the host's zone with its own Name and the exact Project tag in its create call."
  }

  assert {
    condition = (
      aws_ebs_volume.scratch.tags["Name"] != aws_ebs_volume.data.tags["Name"] &&
      length(one(aws_dlm_lifecycle_policy.data.policy_details).target_tags) == 1 &&
      one(aws_dlm_lifecycle_policy.data.policy_details).target_tags["Name"] == "example_project_host_data_dev" &&
      one(aws_dlm_lifecycle_policy.data.policy_details).target_tags["Name"] != aws_ebs_volume.scratch.tags["Name"]
    )
    error_message = "The snapshot schedule targets only the data volume's Name, so the scratch volume is never snapshotted."
  }

  assert {
    condition     = output.scratch_volume_id == "vol-0123456789abcdef0" && output.scratch_mount_path == "/data/job_scratch"
    error_message = "The scratch volume ID and its mount path are outputs."
  }
}

run "scratch_size_moves_after_the_dry_run_peak" {
  command = plan

  variables {
    scratch_volume_gb = 120
  }

  assert {
    condition     = aws_ebs_volume.scratch.size == 120 && aws_ebs_volume.data.size == 60
    error_message = "The scratch size is its own input and leaves the data volume alone."
  }
}

run "reject_scratch_volume_outside_range" {
  command = plan

  variables {
    scratch_volume_gb = 5000
  }

  expect_failures = [var.scratch_volume_gb]
}

run "reject_empty_scratch_volume" {
  command = plan

  variables {
    scratch_volume_gb = 0
  }

  expect_failures = [var.scratch_volume_gb]
}

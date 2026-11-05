# A scratch size change replaces the volume with a new, empty one instead of growing it in place (ModifyVolume).
# This file applies, so its mock provider fills computed values at apply, not at plan: values filled at plan are drawn
# again when the apply plans its final values, and Terraform refuses the difference. The data sources and IDs match
# scratch.tftest.hcl.

mock_provider "aws" {
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
  target = data.aws_subnet.public["1"]
  values = {
    id                = "subnet-0fedcba9876543210"
    availability_zone = "eu-west-1b"
  }
}

override_data {
  target = data.aws_security_group.task
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

# The two runs share state: the first creates every resource at the default size, and the second plans a size change
# against that state. A replaced volume gets a new ID, which the override supplies; a volume changed in place keeps the ID it
# had in state, so the assertion tells the two plans apart.
run "scratch_volume_created_at_the_default_size" {
  command = apply

  assert {
    condition     = aws_ebs_volume.scratch.size == 60 && aws_ebs_volume.scratch.id == "vol-0123456789abcdef0"
    error_message = "The first run creates the scratch volume at the default size."
  }
}

run "scratch_size_change_replaces_the_volume" {
  command = plan

  variables {
    scratch_volume_gb = 120
  }

  override_resource {
    target          = aws_ebs_volume.scratch
    override_during = plan
    values = {
      id = "vol-0fedcba9876543210"
    }
  }

  assert {
    condition     = aws_ebs_volume.scratch.id == "vol-0fedcba9876543210" && aws_ebs_volume.scratch.size == 120 && aws_ebs_volume.data.size == 60
    error_message = "A scratch size change replaces the volume with a new one of the new size and leaves the data volume alone; it is never grown in place."
  }
}

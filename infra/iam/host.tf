# The container host belongs to the administrator: its launch template, the Auto Scaling group that runs the template at a
# pinned numeric version, and the network it needs. The project user cannot launch an instance, create or change the
# group, change the template or refresh the group. It can only set the group's desired capacity to 0 or 1 (the group's
# maximum size is 1), which turns the host off and on. The platform stack looks up the VPC, the subnets, the security
# groups and the group by tag and name.
locals {
  # Resizing the host is an administrator change: edit these, then plan and apply this stack; the Auto Scaling group below
  # takes the new template version through an instance refresh. Graviton only: the images are arm64.
  host_instance_type  = "r7g.large"
  host_root_volume_gb = 30
  platform_vpc_cidr   = "10.40.0.0/16"
  data_mount_path     = "/data"

  # Every platform resource carries the Project tag that the budget filters on.
  project_tags = { Project = local.platform_project_tag, Environment = var.environment, ManagedBy = "Terraform" }
  host_name    = format(local.platform_name, "host")
  instance_tags = merge(local.project_tags, {
    DataClassification = "Confidential"
    Name               = local.host_name
  })

  # The boot script bounds every wait and joins the cluster only after the data volume is mounted. The data volume and
  # Elastic IP belong to the platform stack, which applies later, so the script finds each by its Name and Project tags.
  data_volume_name = format(local.platform_name, "host_data")
  host_user_data = templatefile("${path.module}/host_boot.sh.tftpl", {
    region           = var.aws_region
    mount_path       = local.data_mount_path
    cluster_name     = local.cluster_name
    data_volume_name = local.data_volume_name
    address_name     = local.host_name
    project_tag      = local.platform_project_tag
  })
}

data "aws_ssm_parameter" "ecs_ami" {
  name = "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id"
}

# Public subnets only, with no NAT gateway: the host and the run tasks reach AWS services and image registries through
# the internet gateway. Flow logs are left out to stay within the CloudWatch free allowance.
#trivy:ignore:AWS-0178
resource "aws_vpc" "platform" {
  #checkov:skip=CKV2_AWS_11:Flow logs are left out to stay within the CloudWatch free allowance (owner-accepted budget design).
  #checkov:skip=CKV2_AWS_12:Nothing uses the default security group; adopting it would need permission to change an untagged group.
  cidr_block           = local.platform_vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.project_tags, {
    Name = format(local.platform_name, "vpc")
  })
}

resource "aws_security_group" "host" {
  name        = local.host_name
  description = "Container host: reachable only from the run tasks on the service ports and the owner addresses."
  vpc_id      = aws_vpc.platform.id

  tags = merge(local.project_tags, {
    Name = local.host_name
  })
}

resource "aws_launch_template" "host" {
  #checkov:skip=CKV_AWS_88:With no NAT gateway the host needs a public IP to reach AWS endpoints; boot replaces it with the Elastic IP.
  name                   = local.host_name
  image_id               = data.aws_ssm_parameter.ecs_ami.insecure_value
  instance_type          = local.host_instance_type
  update_default_version = true
  user_data              = base64encode(local.host_user_data)

  iam_instance_profile {
    arn = aws_iam_instance_profile.ecs_host.arn
  }

  # The instance takes a temporary public IP until boot associates the Elastic IP; without it the host could not reach
  # the AWS endpoints it needs to do so.
  network_interfaces {
    device_index                = 0
    associate_public_ip_address = true
    delete_on_termination       = true
    security_groups             = [aws_security_group.host.id]
  }

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_type           = "gp3"
      volume_size           = local.host_root_volume_gb
      encrypted             = true
      delete_on_termination = true
    }
  }

  # Tokens and a hop limit of 1 keep bridge-network containers off the metadata service. Host-network containers share
  # the host's network, so the boot script's firewall drops their metadata traffic unless they run as root. IPv6
  # metadata stays off, so the IPv4 rule covers the only endpoint. Boot finds the data volume and the Elastic IP by tag
  # through the EC2 API, so the instance tags stay unreadable through the metadata service.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    http_protocol_ipv6          = "disabled"
    instance_metadata_tags      = "disabled"
  }

  tag_specifications {
    resource_type = "instance"
    tags          = local.instance_tags
  }

  tag_specifications {
    resource_type = "volume"
    tags          = local.instance_tags
  }

  tag_specifications {
    resource_type = "network-interface"
    tags          = merge(local.project_tags, { Name = local.host_name })
  }

  tags = local.instance_tags
}

# The host starts off. The data volume and the Elastic IP belong to the platform stack, which applies after this one, so a
# host started now could not find them. After the platform apply the project user turns the host on with
# autoscaling:SetDesiredCapacity, which the group's maximum size of 1 limits to one instance, and off again the same way.
# Terraform ignores the desired capacity afterwards. A new template version (instance type, image or boot script) is a new
# number in this block, applied by the administrator, and replaces a running host through an instance refresh.
resource "aws_autoscaling_group" "host" {
  name                  = local.host_name
  min_size              = 0
  max_size              = 1
  desired_capacity      = 0
  vpc_zone_identifier   = [aws_subnet.public[0].id]
  health_check_type     = "EC2"
  protect_from_scale_in = false

  launch_template {
    id      = aws_launch_template.host.id
    version = tostring(aws_launch_template.host.latest_version)
  }

  instance_refresh {
    strategy = "Rolling"

    preferences {
      min_healthy_percentage = 0
    }
  }

  dynamic "tag" {
    for_each = merge(local.project_tags, { Name = local.host_name })

    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = false
    }
  }

  lifecycle {
    ignore_changes = [desired_capacity]
  }
}

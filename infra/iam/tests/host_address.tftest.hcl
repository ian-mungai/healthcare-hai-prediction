mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = {
      insecure_value = "ami-0123456789abcdef0"
    }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      locations = ["eu-west-1c", "eu-west-1b", "eu-west-1-example-1a", "eu-west-1a"]
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev"
    }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = {
      arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev"
    }
  }
}

# The launch template ID stays unknown in a mocked plan unless it is overridden, and the group pins its version.
override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values = {
    id             = "lt-0123456789abcdef0"
    latest_version = 1
  }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values = {
    arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev"
  }
}

override_resource {
  target          = aws_vpc.platform
  override_during = plan
  values = {
    id = "vpc-0123456789abcdef0"
  }
}

override_resource {
  target          = aws_security_group.host
  override_during = plan
  values = {
    id = "sg-0000000000000host0"
  }
}

override_resource {
  target          = aws_security_group.task
  override_during = plan
  values = {
    id = "sg-0000000000000task0"
  }
}

override_resource {
  target          = aws_internet_gateway.platform
  override_during = plan
  values = {
    id = "igw-0123456789abcdef0"
  }
}

override_resource {
  target          = aws_subnet.public
  override_during = plan
  values = {
    id = "subnet-0123456789abcdef0"
  }
}

override_resource {
  target          = aws_iam_role.task_execution
  override_during = plan
  values          = { id = "example_project_task_execution_dev" }
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

# The container host keeps one private address for its whole life: a network interface of this stack, tagged like the data
# volume and the Elastic IP, that boot attaches before ECS joins. Services and jobs read the address from the platform
# stack, which finds the interface by tag, so no person types the address.
#
# Failure modes this file pins, written before the change:
# - the interface is missing or untagged, so boot finds none and the host would run on its changing address;
# - the interface sits in another subnet than the host, so it cannot attach;
# - the interface carries a group other than the host group, so the run tasks cannot reach the services;
# - the interface has the host's own Name tag, so the host's primary interface also matches the boot search;
# - the address is not exported, so every consumer needs a hand input again;
# - boot attaches the interface after ECS joins, or without a deadline, or treats an attached interface as a failure;
# - the host role can attach or list more than its own tagged interface.

override_resource {
  target          = aws_network_interface.host_address
  override_during = plan
  values = {
    id         = "eni-0123456789abcdef0"
    private_ip = "10.40.0.20"
  }
}

run "the_interface_is_tagged_in_the_host_subnet_with_the_host_group" {
  command = plan

  assert {
    condition = (
      aws_network_interface.host_address.subnet_id == aws_subnet.public[0].id &&
      toset(aws_network_interface.host_address.security_groups) == toset([aws_security_group.host.id]) &&
      aws_network_interface.host_address.source_dest_check == true
    )
    error_message = "The interface is in the host's subnet (the group's only subnet) with the host security group, so the run tasks reach the services at its address."
  }

  assert {
    condition = (
      aws_network_interface.host_address.tags["Name"] == "example_project_host_address_dev" &&
      aws_network_interface.host_address.tags["Project"] == "example_project" &&
      aws_network_interface.host_address.tags["Environment"] == "dev" &&
      aws_network_interface.host_address.tags["DataClassification"] == "Public" &&
      aws_network_interface.host_address.tags["Name"] != local.host_name
    )
    error_message = "The interface carries the Project tag and a Name of its own, so boot finds exactly one and the host's primary interface never matches."
  }
}

run "the_address_is_exported" {
  command = plan

  assert {
    condition     = output.host_private_ip == "10.40.0.20"
    error_message = "The stack exports the interface's private address as host_private_ip."
  }
}

run "boot_finds_the_interface_by_tag_and_attaches_it_before_ecs_joins" {
  command = plan

  assert {
    condition = alltrue([
      for text in [
        "interface_name=\"example_project_host_address_dev\"",
        "current_step=\"Finding the host network interface\"",
        "aws ec2 describe-network-interfaces",
        "\"Name=tag:Name,Values=$interface_name\"",
        "\"Name=tag:Project,Values=$project_tag\"",
        "Exactly one network interface must carry the host address tags",
        "run_within 60 \"Attaching the host network interface\" aws ec2 attach-network-interface",
        "--device-index 1",
        "interface_attach_seconds=600",
        "interface_wait_seconds=120",
        "The host network interface did not attach within $interface_attach_seconds seconds.",
        "Waiting for the host address",
        "The host address did not come up within $interface_wait_seconds seconds."
      ] : strcontains(local.host_user_data, text)
    ])
    error_message = "Boot finds the interface by its Name and Project tags, attaches it as the second interface under a deadline and waits for the operating system to show its address."
  }

  assert {
    condition = (
      length(split("Attaching the host network interface", local.host_user_data)[0]) < length(split("ECS_CLUSTER=", local.host_user_data)[0]) &&
      length(split("Attaching the host network interface", local.host_user_data)[0]) < length(split("Attaching the data volume", local.host_user_data)[0]) &&
      length(split("Waiting for the host address", local.host_user_data)[0]) < length(split("ECS_CLUSTER=", local.host_user_data)[0])
    )
    error_message = "The interface is attached and its address is up before the volumes attach and before ECS_CLUSTER is written."
  }

  assert {
    condition     = strcontains(local.host_user_data, "attached_instance") && strcontains(local.host_user_data, "NetworkInterfaces[0].Attachment.InstanceId")
    error_message = "An interface that is already attached to this instance counts as attached, as the volume handling does."
  }
}

run "the_host_role_finds_and_attaches_only_the_tagged_interface" {
  command = plan

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement if statement.Sid == "FindOwnNetworkInterface"
      ]) == {
      Sid       = "FindOwnNetworkInterface"
      Effect    = "Allow"
      Action    = "ec2:DescribeNetworkInterfaces"
      Resource  = "*"
      Condition = { StringEquals = { "aws:RequestedRegion" = "eu-west-1" } }
    }
    error_message = "The host role lists network interfaces only in the project region (the call has no resource type)."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement if statement.Sid == "AttachHostAddressInterface"
      ]) == {
      Sid       = "AttachHostAddressInterface"
      Effect    = "Allow"
      Action    = "ec2:AttachNetworkInterface"
      Resource  = "arn:aws:ec2:eu-west-1:111111111111:network-interface/*"
      Condition = { StringEquals = { "aws:ResourceTag/Name" = "example_project_host_address_dev", "aws:ResourceTag/Project" = "example_project" } }
    }
    error_message = "The host role attaches only the interface with the exact address Name and Project tags."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement if statement.Sid == "AttachInterfaceToOwnInstance"
      ]) == {
      Sid       = "AttachInterfaceToOwnInstance"
      Effect    = "Allow"
      Action    = "ec2:AttachNetworkInterface"
      Resource  = "arn:aws:ec2:eu-west-1:111111111111:instance/*"
      Condition = { StringEquals = { "aws:ResourceTag/Name" = "example_project_host_dev", "aws:ResourceTag/Project" = "example_project" } }
    }
    error_message = "The interface attaches only to the project host instance."
  }

  assert {
    condition     = !anytrue([for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : contains(flatten([statement.Action]), "ec2:DetachNetworkInterface")])
    error_message = "The host role gets no detach permission: boot only attaches."
  }
}

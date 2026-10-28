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

# The network belongs to this stack, so the project user cannot open a port, add a route or move the host. The platform
# stack finds each piece by its Name and Project tags.
run "public_network_without_nat" {
  command = plan

  assert {
    condition = (
      length(aws_subnet.public) == 2 &&
      [for subnet in aws_subnet.public : subnet.availability_zone] == ["eu-west-1a", "eu-west-1b"] &&
      alltrue([for subnet in aws_subnet.public : !subnet.map_public_ip_on_launch && subnet.vpc_id == aws_vpc.platform.id]) &&
      [for subnet in aws_subnet.public : subnet.tags["Name"]] == ["example_project_public_0_dev", "example_project_public_1_dev"] &&
      alltrue([for subnet in aws_subnet.public : subnet.tags["Project"] == "example_project"])
    )
    error_message = "Two public subnets in two regular availability zones carry the Name and Project tags the platform stack looks them up by; none gives every launch a public IP."
  }

  assert {
    condition = (
      aws_route.internet.destination_cidr_block == "0.0.0.0/0" &&
      aws_route.internet.gateway_id == aws_internet_gateway.platform.id &&
      aws_route.internet.nat_gateway_id == null &&
      length(aws_route_table_association.public) == 2 &&
      aws_internet_gateway.platform.tags["Name"] == "example_project_internet_gateway_dev" &&
      aws_internet_gateway.platform.tags["Project"] == "example_project" &&
      aws_route_table.public.tags["Project"] == "example_project"
    )
    error_message = "The only route out is the internet gateway; there is no NAT gateway; every network resource carries the Project tag."
  }
}

run "security_groups" {
  command = plan

  assert {
    condition = (
      length(aws_vpc_security_group_ingress_rule.host_owner) == 1 &&
      alltrue([
        for rule in aws_vpc_security_group_ingress_rule.host_owner :
        rule.security_group_id == aws_security_group.host.id && rule.cidr_ipv4 == "192.0.2.10/32" && rule.ip_protocol == "tcp" &&
        rule.from_port == 8443 && rule.to_port == 8443
      ])
    )
    error_message = "The host accepts only the owner address on the listed ports; no rule opens the run-task group."
  }

  assert {
    condition = alltrue([
      for rule in [aws_vpc_security_group_egress_rule.host_https, aws_vpc_security_group_egress_rule.task_https] :
      rule.cidr_ipv4 == "0.0.0.0/0" && rule.ip_protocol == "tcp" && rule.from_port == 443 && rule.to_port == 443
    ])
    error_message = "Internet egress is HTTPS only."
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.host_https.security_group_id == aws_security_group.host.id &&
      aws_vpc_security_group_egress_rule.task_https.security_group_id == aws_security_group.task.id &&
      aws_vpc_security_group_egress_rule.task_platform.security_group_id == aws_security_group.task.id &&
      aws_vpc_security_group_egress_rule.task_platform.cidr_ipv4 == "10.40.0.0/16" &&
      aws_security_group.task.name == "example_project_task_dev" && aws_security_group.task.tags["Name"] == "example_project_task_dev" &&
      aws_security_group.task.tags["Project"] == "example_project"
    )
    error_message = "Run tasks reach only HTTPS endpoints and the platform network, and their group carries the tags the platform stack looks it up by."
  }

  assert {
    condition = (
      toset([for rule in aws_vpc_security_group_ingress_rule.host_from_tasks : rule.from_port]) == toset([6379, 8181]) &&
      alltrue([
        for rule in aws_vpc_security_group_ingress_rule.host_from_tasks :
        rule.security_group_id == aws_security_group.host.id && rule.referenced_security_group_id == aws_security_group.task.id &&
        rule.cidr_ipv4 == null && rule.ip_protocol == "tcp" && rule.from_port == rule.to_port
      ])
    )
    error_message = "Only the run-task group reaches the host, and only on the Polaris and Redis ports."
  }
}

run "closed_host_by_default" {
  command = plan

  variables {
    owner_ip_cidrs     = []
    host_ingress_ports = []
  }

  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.host_owner) == 0
    error_message = "Without owner addresses the host accepts no inbound traffic."
  }
}

# The host's group is the administrator's: it starts with no instance (the data volume and Elastic IP do not exist yet),
# holds at most one, runs the template at a version that is a number and is never replaced when the project user turns the
# host on or off.
run "host_auto_scaling_group" {
  command = plan

  assert {
    condition = (
      aws_autoscaling_group.host.name == "example_project_host_dev" &&
      aws_autoscaling_group.host.min_size == 0 && aws_autoscaling_group.host.max_size == 1 && aws_autoscaling_group.host.desired_capacity == 0 &&
      aws_autoscaling_group.host.vpc_zone_identifier == toset([aws_subnet.public[0].id]) &&
      aws_autoscaling_group.host.health_check_type == "EC2"
    )
    error_message = "The host group holds zero or one instance, starts with none and runs in the data volume's subnet."
  }

  assert {
    condition = (
      one(aws_autoscaling_group.host.launch_template).id == "lt-0123456789abcdef0" &&
      one(aws_autoscaling_group.host.launch_template).version == "1" &&
      can(tonumber(one(aws_autoscaling_group.host.launch_template).version)) &&
      one(aws_autoscaling_group.host.launch_template).version != "$Latest" && one(aws_autoscaling_group.host.launch_template).version != "$Default" &&
      one(aws_autoscaling_group.host.instance_refresh).strategy == "Rolling"
    )
    error_message = "The host group runs the approved template at a pinned version number, never $Latest or $Default, and a new version replaces the host through an instance refresh."
  }

  assert {
    condition = (
      toset([for tag in aws_autoscaling_group.host.tag : "${tag.key}=${tag.value}"]) ==
      toset(["Project=example_project", "Environment=dev", "ManagedBy=Terraform", "Name=example_project_host_dev", "AmazonECSManaged="]) &&
      toset([for tag in aws_autoscaling_group.host.tag : tag.key if tag.propagate_at_launch]) == toset(["AmazonECSManaged"]) &&
      output.host_launch_template.version == "1" && output.host_autoscaling_group_name == "example_project_host_dev"
    )
    error_message = "The group carries the project tags and the AmazonECSManaged tag that ECS adds; the launch template tags the instance, and the group propagates only the ECS tag."
  }
}

run "new_template_version_is_pinned_by_number" {
  command = plan

  override_resource {
    target          = aws_launch_template.host
    override_during = plan
    values = {
      id             = "lt-0123456789abcdef0"
      latest_version = 4
    }
  }

  assert {
    condition     = one(aws_autoscaling_group.host.launch_template).version == "4" && output.host_launch_template.version == "4"
    error_message = "A new template version, such as a resize, is pinned by number."
  }
}

# The cost budget belongs to this stack too, with its own alert topic that the project user's sns policy does not name.
run "budget_and_alert_topic" {
  command = plan

  assert {
    condition = (
      aws_sns_topic.budget_alerts.name == "example_project_budget_alerts_dev" &&
      jsondecode(aws_sns_topic_policy.budget_alerts.policy).Statement[0].Principal == { Service = "budgets.amazonaws.com" } &&
      jsondecode(aws_sns_topic_policy.budget_alerts.policy).Statement[0].Condition.StringEquals["aws:SourceAccount"] == "111111111111" &&
      length(jsondecode(aws_sns_topic_policy.budget_alerts.policy).Statement) == 1
    )
    error_message = "Only AWS Budgets for this account may publish to the budget topic, whose name differs from the platform alert topic."
  }

  assert {
    condition = (
      aws_budgets_budget.platform.name == "example_project_platform_dev" &&
      aws_budgets_budget.platform.limit_amount == "150.00" && aws_budgets_budget.platform.limit_unit == "USD" &&
      aws_budgets_budget.platform.time_unit == "QUARTERLY" && aws_budgets_budget.platform.budget_type == "COST"
    )
    error_message = "The budget is a $150 quarterly cost budget."
  }

  # The Project value is the project name, which the data bucket carries too, so the filter holds both conditions: the
  # tag AND the platform's services. Separate filters combine with AND, the values of one with OR. S3 and Secrets Manager
  # stay outside the budget.
  assert {
    condition = (
      length(aws_budgets_budget.platform.cost_filter) == 2 &&
      toset([for filter in aws_budgets_budget.platform.cost_filter : filter.name]) == toset(["TagKeyValue", "Service"]) &&
      toset(one([for filter in aws_budgets_budget.platform.cost_filter : filter.values if filter.name == "TagKeyValue"])) == toset(["user:Project$example_project"]) &&
      toset(one([for filter in aws_budgets_budget.platform.cost_filter : filter.values if filter.name == "Service"])) == toset([
        "Amazon Elastic Compute Cloud - Compute", "EC2 - Other", "Amazon Virtual Private Cloud", "Amazon Elastic Container Service",
        "Amazon Elastic Load Balancing", "AmazonCloudWatch", "Amazon EC2 Container Registry (ECR)"
      ]) &&
      !anytrue([for filter in aws_budgets_budget.platform.cost_filter : anytrue([for value in filter.values : strcontains(lower(value), "s3") || strcontains(lower(value), "simple storage") || strcontains(lower(value), "secrets")])])
    )
    error_message = "The budget filter must hold the Project tag AND the platform's services, and leave S3 and Secrets Manager out."
  }

  assert {
    condition = (
      toset([for notification in aws_budgets_budget.platform.notification : notification.threshold]) == toset([50, 80]) &&
      alltrue([
        for notification in aws_budgets_budget.platform.notification :
        notification.notification_type == "ACTUAL" && notification.threshold_type == "PERCENTAGE" &&
        notification.comparison_operator == "GREATER_THAN" && notification.subscriber_sns_topic_arns == toset([aws_sns_topic.budget_alerts.arn])
      ])
    )
    error_message = "Actual spend above 50% and 80% of the budget alerts the budget topic."
  }

  # The budget only reports: no action stops spending, so nothing else in the stack names the budget or its topic.
  assert {
    condition = (
      length(aws_budgets_budget.platform.notification) == 2 &&
      output.budget == {
        name          = "example_project_platform_dev"
        alerts_topic  = aws_sns_topic.budget_alerts.arn
        project_tag   = "example_project"
        limit_dollars = 150
      } &&
      !contains(keys(output.platform_role_arns), "budget_action")
    )
    error_message = "The $150 budget keeps its 50% and 80% alerts as information only; no budget action or role exists."
  }
}

# The host launch template and the network it needs belong to this stack, so only an administrator can change what an
# instance launches with. The platform stack finds them by name and tag.
run "host_launch_template" {
  command = plan

  assert {
    condition = (
      aws_vpc.platform.cidr_block == "10.40.0.0/16" && aws_vpc.platform.enable_dns_support && aws_vpc.platform.enable_dns_hostnames &&
      aws_vpc.platform.tags["Name"] == "example_project_vpc_dev" && aws_vpc.platform.tags["Project"] == "example_project" &&
      aws_security_group.host.vpc_id == "vpc-0123456789abcdef0" && aws_security_group.host.name == "example_project_host_dev" &&
      aws_security_group.host.tags["Name"] == "example_project_host_dev" && aws_security_group.host.tags["Project"] == "example_project" &&
      length(try(aws_security_group.host.ingress, [])) == 0
    )
    error_message = "The VPC and the host security group carry the dashed Project tag and their Name tags, which the platform stack looks them up by; the security group holds no inline rule."
  }

  assert {
    condition = (
      aws_launch_template.host.name == "example_project_host_dev" &&
      aws_launch_template.host.instance_type == "r7g.large" &&
      can(regex("^[a-z][0-9]+g[a-z]*\\.[a-z0-9]+$", aws_launch_template.host.instance_type)) &&
      aws_launch_template.host.image_id == "ami-0123456789abcdef0" &&
      data.aws_ssm_parameter.ecs_ami.name == "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id" &&
      one(aws_launch_template.host.iam_instance_profile).arn == "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev" &&
      one(aws_launch_template.host.network_interfaces).security_groups == toset(["sg-0000000000000host0"]) &&
      aws_launch_template.host.tags["Project"] == "example_project" && aws_launch_template.host.tags["Name"] == "example_project_host_dev"
    )
    error_message = "The host is an r7g.large on the arm64 ECS-optimized image with the host instance profile and the host security group, and the template carries the dashed Project tag."
  }

  assert {
    condition = (
      one(aws_launch_template.host.metadata_options).http_tokens == "required" &&
      one(aws_launch_template.host.metadata_options).http_put_response_hop_limit == 1 &&
      one(aws_launch_template.host.metadata_options).http_protocol_ipv6 == "disabled" &&
      one(aws_launch_template.host.metadata_options).instance_metadata_tags == "disabled" &&
      alltrue([for device in aws_launch_template.host.block_device_mappings : one(device.ebs).encrypted == "true" && one(device.ebs).volume_size == 30])
    )
    error_message = "The host requires metadata tokens, keeps containers off the metadata service, does not serve instance tags (boot finds its volume and address by tag through the EC2 API) and encrypts its 30 GB root volume."
  }

  assert {
    condition = (
      toset([for spec in aws_launch_template.host.tag_specifications : spec.resource_type]) == toset(["instance", "volume", "network-interface"]) &&
      alltrue([
        for spec in aws_launch_template.host.tag_specifications :
        spec.tags["Project"] == "example_project" && spec.tags["Environment"] == "dev" && spec.tags["ManagedBy"] == "Terraform"
      ]) &&
      alltrue([for spec in aws_launch_template.host.tag_specifications : spec.tags["Name"] == "example_project_host_dev"])
    )
    error_message = "Default tags do not reach launched instances, so the template's tag specifications carry the dashed project tags."
  }

  assert {
    condition = (
      alltrue([
        for text in [
          "vol-0123456789abcdef0", "eipalloc-0123456789abcdef0", "tags/instance/"
        ] : !strcontains(base64decode(aws_launch_template.host.user_data), text)
      ]) &&
      alltrue([
        for text in [
          "ECS_CLUSTER=example_project_cluster_dev", "ECS_ENABLE_TASK_IAM_ROLE=true", "ECS_ENABLE_TASK_IAM_ROLE_NETWORK_HOST=true",
          "ECS_AWSVPC_BLOCK_IMDS=true", "ECS_ENABLE_AWSLOGS_EXECUTIONROLE_OVERRIDE=true", "systemctl enable --now amazon-ssm-agent",
          "data_volume_name=\"example_project_host_data_dev\"", "address_name=\"example_project_host_dev\"", "project_tag=\"example_project\"",
          "volume=$(run_within 60 \"Finding the data volume\" aws ec2 describe-volumes", "allocation=$(run_within 60 \"Finding the Elastic IP\" aws ec2 describe-addresses",
          "\"Name=tag:Project,Values=$project_tag\"", "^vol-[0-9a-f]{8,17}$", "^eipalloc-[0-9a-f]{8,17}$"
        ] : strcontains(base64decode(aws_launch_template.host.user_data), text)
      ]) &&
      length(split("ECS_CLUSTER=", base64decode(aws_launch_template.host.user_data))[0]) > length(split("mountpoint -q", base64decode(aws_launch_template.host.user_data))[0])
    )
    error_message = "Boot finds the data volume and the Elastic IP by their Name and Project tags, attaches them and joins the cluster only after the volume is mounted."
  }

  # Host-network containers share the host's network: only root may reach the metadata service. The rule has its own
  # nftables table, the nftables configuration includes it, Docker and the ECS agent load and check it before every start,
  # and a timer checks it every 10 seconds. The configuration E2E runs these steps against stateful stand-ins.
  assert {
    condition = (
      alltrue([
        for text in [
          "delete table inet platform_imds", "type filter hook output priority -10; policy accept;",
          "ip daddr 169.254.169.254 meta skuid != 0 drop", "include_line=\"include \\\"$nft_rules_file\\\"\"",
          "echo \"$include_line\" >>\"$nftables_config\"", "for unit in docker.service ecs.service; do",
          "setpriv --reuid=65534 --regid=65534 --clear-groups -- curl", "systemctl stop ecs.service docker.service docker.socket",
          "run_within 120 \"Starting the metadata firewall\" systemctl enable --now platform_imds_guard.service",
          "run_within 60 \"Checking the metadata firewall\" \"$guard_script\" check",
          "run_within 60 \"Starting the metadata firewall timer\" systemctl enable --now platform_imds_watch.timer"
        ] : strcontains(base64decode(aws_launch_template.host.user_data), text)
      ]) &&
      !strcontains(base64decode(aws_launch_template.host.user_data), "iptables") &&
      length(split("systemctl enable --now platform_imds_guard.service", base64decode(aws_launch_template.host.user_data))[0]) <
      length(split("latest/api/token", base64decode(aws_launch_template.host.user_data))[0])
    )
    error_message = "Boot must load, check and keep a root-only metadata rule that survives firewall reloads, gates Docker and the ECS agent and is rechecked every 10 seconds, before it reads metadata."
  }

  # Every systemd directive sits in the section systemd reads it from; systemd ignores a misplaced one without an error,
  # so a dependency in the wrong section silently stops stopping Docker and the ECS agent with the firewall. The unit
  # texts are pinned whole, and the E2E checks the sections again and restarts the firewall through a dependency model.
  assert {
    condition = alltrue([
      for text in [
        "[Unit]\nDescription=Drop instance metadata traffic from every user except root\nAfter=nftables.service\nBefore=docker.service ecs.service\nPartOf=nftables.service\nReloadPropagatedFrom=nftables.service\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nTimeoutStartSec=120\nExecStart=$guard_script apply\nExecReload=$guard_script apply\n\n[Install]\nWantedBy=multi-user.target\n",
        "[Unit]\nRequires=platform_imds_guard.service\nAfter=platform_imds_guard.service\n\n[Service]\nExecStartPre=$guard_script apply\n",
        "[Unit]\nDescription=Check the metadata firewall and stop the ECS agent and Docker when it cannot be restored\nAfter=platform_imds_guard.service\n\n[Service]\nType=oneshot\nTimeoutStartSec=400\nExecStart=$guard_script enforce\n",
        "[Unit]\nDescription=Check the metadata firewall every 10 seconds\n\n[Timer]\nOnActiveSec=10s\nOnUnitActiveSec=10s\nAccuracySec=1s\n\n[Install]\nWantedBy=timers.target\n"
      ] : strcontains(base64decode(aws_launch_template.host.user_data), text)
    ])
    error_message = "The systemd units must keep each directive in its section: dependencies in [Unit], commands in [Service], timers in [Timer]."
  }

  # A pipe handler receives core dumps even when a container's core limit is zero, so cores are plain files and
  # set-user-ID programs never dump. Boot stops when the settings do not take effect, before the host joins the cluster.
  assert {
    condition = (
      alltrue([
        for text in [
          "sysctl_file=\"/etc/sysctl.d/90-platform-core-dumps.conf\"", "kernel.core_pattern = core", "fs.suid_dumpable = 0",
          "run_within 30 \"Applying the core dump settings\" sysctl -q -p \"$sysctl_file\"",
          "if [ \"$core_pattern\" != \"core\" ] || [ \"$suid_dumpable\" != \"0\" ]; then"
        ] : strcontains(base64decode(aws_launch_template.host.user_data), text)
      ]) &&
      !can(regex("core_pattern\\s*=\\s*\\|", base64decode(aws_launch_template.host.user_data))) &&
      length(split("Applying the core dump settings", base64decode(aws_launch_template.host.user_data))[0]) <
      length(split("ECS_CLUSTER=", base64decode(aws_launch_template.host.user_data))[0])
    )
    error_message = "Boot must set a plain-file core pattern and disable set-user-ID dumps, persisted in sysctl.d, and check both before joining the cluster."
  }

  # Every line the boot script writes also reaches the serial console with a fixed prefix, so the project user can read it
  # after the instance ends (ec2:GetConsoleOutput). A separate process copies the log, so a console that fails or blocks
  # cannot stop a step, and the exit handler states success or the failing status, line and step. The AWS CLI calls whose
  # JSON would reach the console print nothing. The configuration E2E runs these cases with a broken and a blocked console.
  assert {
    condition = (
      alltrue([
        for text in [
          "console_device=\"/dev/console\"", "console_prefix=\"platform_boot:\"", "console_flush_seconds=4",
          "copy_log_to_console </dev/null >/dev/null 2>&1 &", "trap 'finish $LINENO' EXIT",
          "log \"finished: every step succeeded and ECS_CLUSTER is written.\"",
          "log \"FAILED: exit status $status at line $line during: $current_step\"",
          "log \"starting: $step (limit $seconds seconds)\"", "--allow-reassociation >/dev/null", "--device /dev/xvdf >/dev/null"
        ] : strcontains(base64decode(aws_launch_template.host.user_data), text)
      ]) &&
      length(split("trap 'finish $LINENO' EXIT", base64decode(aws_launch_template.host.user_data))[0]) <
      length(split("Applying the core dump settings", base64decode(aws_launch_template.host.user_data))[0])
    )
    error_message = "Boot must copy every log line to the console with the platform_boot prefix, state success or the failing status at the end and print no AWS CLI output."
  }

  # Each command that can stall runs under a deadline through the run_within helper; none starts a line on its own. GNU
  # timeout kills a command that ignores TERM 10 seconds after the deadline, and the helper reports it.
  assert {
    condition = (
      alltrue([
        for line in split("\n", base64decode(aws_launch_template.host.user_data)) :
        !can(regex("^\\s*(systemctl|mkfs\\.xfs|mount|dnf|aws|blkid|rpm|iptables|mountpoint|nft|sysctl|setpriv) ", line))
      ]) &&
      alltrue([
        for line in split("\n", base64decode(aws_launch_template.host.user_data)) :
        strcontains(line, "timeout --kill-after=") if can(regex("(^|\\s)timeout\\s", line)) && !can(regex("^\\s*#", line))
      ]) &&
      alltrue([
        for text in [
          "run_within 120 \"Starting the Session Manager agent\" systemctl enable --now amazon-ssm-agent",
          "run_within 300 \"Formatting the data volume\" mkfs.xfs",
          "run_within 120 \"Mounting the data volume\" mount",
          "timeout --kill-after=10 \"$seconds\" \"$@\"",
          "did not finish within $seconds seconds and ignored the stop signal, so it was killed 10 seconds later."
        ] : strcontains(base64decode(aws_launch_template.host.user_data), text)
      ])
    )
    error_message = "Service start, formatting, mounting and every other command that can stall must run under a deadline that also kills a command ignoring TERM."
  }
}

run "platform_roles" {
  command = plan

  assert {
    condition = (
      aws_iam_role.ecs_host.name == "example_project_ecs_host_dev" &&
      aws_iam_role.task_execution.name == "example_project_task_execution_dev" &&
      aws_iam_role.polaris_task.name == "example_project_polaris_task_dev" &&
      aws_iam_role.control_plane_task.name == "example_project_control_plane_task_dev" &&
      aws_iam_role.snapshots.name == "example_project_snapshots_dev" &&
      aws_iam_instance_profile.ecs_host.name == "example_project_ecs_host_dev"
    )
    error_message = "Role names must match the names the platform stack builds."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.ecs_host.assume_role_policy).Statement[0].Principal == { Service = "ec2.amazonaws.com" } &&
      jsondecode(aws_iam_role.snapshots.assume_role_policy).Statement[0].Principal == { Service = "dlm.amazonaws.com" } &&
      alltrue([
        for role in [aws_iam_role.task_execution, aws_iam_role.polaris_task, aws_iam_role.control_plane_task] :
        jsondecode(role.assume_role_policy).Statement[0].Principal == { Service = "ecs-tasks.amazonaws.com" } &&
        jsondecode(role.assume_role_policy).Statement[0].Condition.StringEquals["aws:SourceAccount"] == "111111111111" &&
        jsondecode(role.assume_role_policy).Statement[0].Condition.ArnLike["aws:SourceArn"] == "arn:aws:ecs:eu-west-1:111111111111:*"
      ]) &&
      alltrue([for role in [aws_iam_role.ecs_host, aws_iam_role.snapshots, aws_iam_role.task_execution, aws_iam_role.polaris_task, aws_iam_role.control_plane_task] : length(jsondecode(role.assume_role_policy).Statement) == 1])
    )
    error_message = "Each role trusts exactly one service; task roles only from ECS in this account."
  }

  assert {
    condition = (
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.polaris_task.policy).Statement : flatten([statement.Resource]) if startswith(flatten([statement.Action])[0], "s3:")])) ==
      toset(["arn:aws:s3:::example-project-ci-bucket/lakehouse/*", "arn:aws:s3:::example-project-ci-bucket"]) &&
      one([for statement in jsondecode(aws_iam_role_policy.polaris_task.policy).Statement : statement.Condition.StringLike["s3:prefix"] if statement.Action == "s3:ListBucket"]) == ["lakehouse/", "lakehouse/*"] &&
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.polaris_task.policy).Statement : flatten([statement.Action]) if statement.Sid == "ManageLakehouseTables"])) ==
      toset(["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"])
    )
    error_message = "The Polaris task role reaches only lakehouse/ and never deletes object versions."
  }

  assert {
    condition = (
      one([for statement in jsondecode(aws_iam_role_policy.task_execution.policy).Statement : statement if statement.Sid == "InjectNamedParameters"]) == {
        Sid    = "InjectNamedParameters"
        Effect = "Allow"
        Action = "ssm:GetParameters"
        Resource = [
          for name in ["postgres_password", "airflow_db_password", "marquez_db_password", "airflow_fernet_key", "airflow_jwt_secret", "airflow_api_secret_key", "redis_password", "polaris_root_secret"] :
          "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/${name}"
        ]
      } &&
      length(one([for statement in jsondecode(aws_iam_role_policy.task_execution.policy).Statement : statement.Resource if statement.Sid == "InjectNamedParameters"])) == 8 &&
      !anytrue([for statement in jsondecode(aws_iam_role_policy.task_execution.policy).Statement : anytrue([for resource in flatten([statement.Resource]) : strcontains(resource, "parameter") && (strcontains(resource, "*") || strcontains(resource, "?"))])]) &&
      !strcontains(aws_iam_role_policy.task_execution.policy, "secretsmanager") && !strcontains(aws_iam_role_policy.task_execution.policy, "kms:") &&
      !anytrue([for role_policy in [aws_iam_role_policy.ecs_host, aws_iam_role_policy.polaris_task, aws_iam_role_policy.snapshots] : strcontains(role_policy.policy, "ssm:GetParameter") || strcontains(role_policy.policy, "secretsmanager")]) &&
      !anytrue([for statement in jsondecode(aws_iam_role_policy.polaris_task.policy).Statement : anytrue([for action in flatten([statement.Action]) : !startswith(action, "s3:")])])
    )
    error_message = "Only the execution role reads secrets, only as ssm:GetParameters on the eight control-plane parameter ARNs, with no Secrets Manager or KMS grant; the Polaris role has S3 only."
  }

  assert {
    condition = (
      one([for statement in jsondecode(aws_iam_role_policy.task_execution.policy).Statement : statement.Resource if statement.Sid == "PullProjectImages"]) == "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_*" &&
      one([for statement in jsondecode(aws_iam_role_policy.task_execution.policy).Statement : statement.Resource if statement.Sid == "WriteProjectLogs"]) == "arn:aws:logs:eu-west-1:111111111111:log-group:example_project_*:log-stream:*"
    )
    error_message = "Image pulls and log writes go through the execution role and reach only project repositories and log groups."
  }

  # The host role keeps only the ECS agent, Session Manager registration and the boot-time address and volume calls.
  assert {
    condition = toset(flatten([for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : flatten([statement.Action])])) == toset([
      "ecs:DeregisterContainerInstance", "ecs:RegisterContainerInstance", "ecs:SubmitAttachmentStateChanges", "ecs:SubmitContainerStateChange",
      "ecs:SubmitTaskStateChange", "ecs:Poll", "ecs:StartTelemetrySession", "ecs:DiscoverPollEndpoint", "ssm:UpdateInstanceInformation",
      "ssmmessages:CreateControlChannel", "ssmmessages:CreateDataChannel", "ssmmessages:OpenControlChannel", "ssmmessages:OpenDataChannel",
      "ec2:AssociateAddress", "ec2:AttachVolume", "ec2:DescribeAddresses", "ec2:DescribeVolumes"
    ])
    error_message = "The host role holds only the ECS agent, Session Manager and boot-time actions (finding its volume and address by tag, then attaching them); no image pulls, logs, tagging or state changes beyond the agent's."
  }

  assert {
    condition = (
      one([
        for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement
        if contains(flatten([statement.Action]), "ssm:UpdateInstanceInformation")
        ]) == {
        Sid       = "RegisterWithSessionManager"
        Effect    = "Allow"
        Action    = "ssm:UpdateInstanceInformation"
        Resource  = "arn:aws:ec2:eu-west-1:111111111111:instance/*"
        Condition = { StringEquals = { "ssm:resourceTag/Name" = "example_project_host_dev", "ssm:resourceTag/Project" = "example_project" } }
      } &&
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement :
        statement.Condition.StringEquals == (
          anytrue([for resource in flatten([statement.Resource]) : strcontains(resource, ":volume/")]) ?
          { "aws:ResourceTag/Name" = "example_project_host_data_dev", "aws:ResourceTag/Project" = "example_project" } :
          { "aws:ResourceTag/Name" = "example_project_host_dev", "aws:ResourceTag/Project" = "example_project" }
        ) if anytrue([for action in flatten([statement.Action]) : startswith(action, "ec2:")]) && statement.Sid != "FindOwnDataVolumeAndAddress"
      ]) &&
      one([for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement if statement.Sid == "FindOwnDataVolumeAndAddress"]) == {
        Sid       = "FindOwnDataVolumeAndAddress"
        Effect    = "Allow"
        Action    = ["ec2:DescribeAddresses", "ec2:DescribeVolumes"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestedRegion" = "eu-west-1" } }
      }
    )
    error_message = "Session Manager registration and the boot-time calls reach only the project host, its Elastic IP and its data volume, by Name and Project tag."
  }

  assert {
    condition = (
      one([for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement.Resource if statement.Sid == "JoinProjectCluster"]) == "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" &&
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement :
        statement.Condition.StringEquals["aws:ResourceTag/Project"] == "example_project" if contains(flatten([statement.Action]), "ec2:AttachVolume") || contains(flatten([statement.Action]), "ec2:AssociateAddress")
      ])
    )
    error_message = "The host joins only the project cluster and attaches only its own tagged volume and address."
  }

  # Snapshots are tagged only by the call that creates them, only snapshots that the data volume's own schedule made can
  # be deleted (the aws:dlm tags are reserved, so no principal can add them to another snapshot), and every new snapshot
  # must carry the exact key Project with the project value, which two denies enforce besides the allow. Key matching in a
  # request-tag condition ignores case, so the allow and a deny also test aws:TagKeys. The evaluator cases in
  # scripts/infrastructure/run_policy_e2e.py exercise all of it, including a lowercase key.
  assert {
    condition = (
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.snapshots.policy).Statement :
        try(statement.Condition, null) == { StringEquals = { "ec2:CreateAction" = "CreateSnapshot" } }
        if statement.Effect == "Allow" && contains(flatten([statement.Action]), "ec2:CreateTags")
      ]) &&
      one([
        for statement in jsondecode(aws_iam_role_policy.snapshots.policy).Statement :
        statement.Condition if contains(flatten([statement.Action]), "ec2:DeleteSnapshot")
        ]) == {
        StringEquals = {
          "aws:ResourceTag/Name"                            = "example_project_host_data_dev", "aws:ResourceTag/Project" = "example_project",
          "aws:ResourceTag/aws:dlm:lifecycle-schedule-name" = "daily"
        }
      } &&
      one([for statement in jsondecode(aws_iam_role_policy.snapshots.policy).Statement : statement if statement.Sid == "CreateTaggedSnapshots"]) == {
        Sid      = "CreateTaggedSnapshots"
        Effect   = "Allow"
        Action   = "ec2:CreateSnapshot"
        Resource = "arn:aws:ec2:eu-west-1::snapshot/*"
        Condition = {
          StringEquals               = { "aws:RequestTag/Project" = "example_project" }
          "ForAnyValue:StringEquals" = { "aws:TagKeys" = ["Project"] }
        }
      } &&
      one([for statement in jsondecode(aws_iam_role_policy.snapshots.policy).Statement : statement if statement.Sid == "DenySnapshotsWithoutProjectTagValue"]) == {
        Sid       = "DenySnapshotsWithoutProjectTagValue"
        Effect    = "Deny"
        Action    = ["ec2:CopySnapshot", "ec2:CreateSnapshot", "ec2:CreateSnapshots"]
        Resource  = "*"
        Condition = { StringNotEquals = { "aws:RequestTag/Project" = "example_project" } }
      } &&
      one([for statement in jsondecode(aws_iam_role_policy.snapshots.policy).Statement : statement if statement.Sid == "DenySnapshotsWithoutExactProjectKey"]) == {
        Sid       = "DenySnapshotsWithoutExactProjectKey"
        Effect    = "Deny"
        Action    = ["ec2:CopySnapshot", "ec2:CreateSnapshot", "ec2:CreateSnapshots"]
        Resource  = "*"
        Condition = { "ForAllValues:StringNotEquals" = { "aws:TagKeys" = ["Project"] } }
      } &&
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.snapshots.policy).Statement :
        length(flatten([statement.Action])) == 1 if statement.Effect == "Allow" && anytrue([for action in flatten([statement.Action]) : contains(["ec2:CreateTags", "ec2:DeleteSnapshot"], action)])
      ])
    )
    error_message = "Snapshot tags are set only on creation, deletion reaches only the data volume's scheduled snapshots and every new snapshot needs the exact key Project with the project value."
  }

  # A Resource of "*" stays only for actions that the Service Authorization Reference lists without a resource type.
  assert {
    condition = alltrue(flatten([
      for policy in [
        aws_iam_role_policy.ecs_host.policy, aws_iam_role_policy.task_execution.policy, aws_iam_role_policy.polaris_task.policy, aws_iam_role_policy.snapshots.policy
        ] : [
        for statement in jsondecode(policy).Statement : [
          for action in flatten([statement.Action]) : contains([
            "ec2:DescribeAddresses", "ec2:DescribeInstances", "ec2:DescribeSnapshots", "ec2:DescribeVolumes", "ecr:GetAuthorizationToken", "ecs:DiscoverPollEndpoint",
            "ssmmessages:CreateControlChannel", "ssmmessages:CreateDataChannel", "ssmmessages:OpenControlChannel", "ssmmessages:OpenDataChannel"
          ], action) && try(statement.Condition.StringEquals["aws:RequestedRegion"], "") == "eu-west-1"
        ] if statement.Effect == "Allow" && statement.Resource == "*"
      ]
    ]))
    error_message = "A wildcard resource is allowed only for actions without a resource type, and only in the project region."
  }
}

run "lakehouse_roles" {
  command = plan

  assert {
    condition = alltrue([
      for role in [aws_iam_role.lakehouse_reader, aws_iam_role.lakehouse_writer] :
      length(jsondecode(role.assume_role_policy).Statement) == 1
      && jsondecode(role.assume_role_policy).Statement[0].Effect == "Allow"
      && jsondecode(role.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
      && jsondecode(role.assume_role_policy).Statement[0].Principal == { AWS = "arn:aws:iam::111111111111:user/example_project_dev" }
      && !can(jsondecode(role.assume_role_policy).Statement[0].Condition)
    ])
    error_message = "Each lakehouse role trusts only the project's own IAM user."
  }

  assert {
    condition     = aws_iam_role.lakehouse_reader.name == "healthcare_hai_prediction_lakehouse_reader" && aws_iam_role.lakehouse_writer.name == "healthcare_hai_prediction_lakehouse_writer"
    error_message = "The role names default to their real names."
  }

  assert {
    condition     = aws_iam_role.lakehouse_reader.max_session_duration == 7200 && aws_iam_role.lakehouse_writer.max_session_duration == 7200
    error_message = "The longest session is 7200 seconds: the longest pipeline job deadline of 3600 seconds plus the launcher's margin, with room."
  }

  assert {
    condition = toset(flatten([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_reader.policy).Statement : flatten([statement.Action])
    ])) == toset(["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"])
    error_message = "The reader holds exactly reading an object, listing the bucket by prefix and locating the bucket."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_reader.policy).Statement : toset(flatten([statement.Resource]))
      if statement.Sid == "ReadTableFiles"
      ]) == toset([
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*",
    ])
    error_message = "The reader reads only the bronze, silver and gold prefixes."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_reader.policy).Statement : toset(flatten([statement.Condition.StringLike["s3:prefix"]]))
      if statement.Sid == "ListTablePrefixes"
      ]) == toset([
      "lakehouse/bronze", "lakehouse/bronze/*", "lakehouse/silver", "lakehouse/silver/*", "lakehouse/gold", "lakehouse/gold/*",
    ])
    error_message = "The reader lists only the bronze, silver and gold prefixes."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement : toset(flatten([statement.Action]))
      if statement.Sid == "ManageWrittenTableFiles"
    ]) == toset(["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"])
    error_message = "The writer holds exactly the object actions of the project user's ManageLakehouseTables statement (policies/healthcare_hai_prediction_s3_policy.json)."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement : toset(flatten([statement.Resource]))
      if statement.Sid == "ManageWrittenTableFiles"
      ]) == toset([
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver_e2e/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold_e2e/*",
      "arn:aws:s3:::example-project-ci-bucket/lakehouse/catalog_backups/*",
    ])
    error_message = "The writer writes only the silver, silver test, gold, gold test and catalog backup prefixes."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement : toset(flatten([statement.Condition.StringLike["s3:prefix"]]))
      if statement.Sid == "ListWrittenPrefixes"
      ]) == toset([
      "lakehouse/silver", "lakehouse/silver/*", "lakehouse/silver_e2e", "lakehouse/silver_e2e/*", "lakehouse/gold", "lakehouse/gold/*",
      "lakehouse/gold_e2e", "lakehouse/gold_e2e/*", "lakehouse/catalog_backups", "lakehouse/catalog_backups/*",
    ])
    error_message = "The writer lists only its own prefixes."
  }

  assert {
    condition = alltrue(flatten([
      for policy in [aws_iam_role_policy.lakehouse_reader.policy, aws_iam_role_policy.lakehouse_writer.policy] : [
        for statement in jsondecode(policy).Statement : [
          for action in flatten([statement.Action]) : (
            statement.Effect == "Allow"
            && startswith(action, "s3:")
            && !strcontains(action, "*")
            && contains([
              "s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
            ], action)
          )
        ]
      ]
    ]))
    error_message = "Each role action is one of the S3 actions that the project user's s3 policy file grants (scripts/infrastructure/run_policy_e2e.py compares the files), with no wildcard."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.lakehouse_writer.policy).Statement :
      !contains(flatten([statement.Resource]), "arn:aws:s3:::example-project-ci-bucket/lakehouse/*")
    ])
    error_message = "The writer never gets the whole lakehouse folder."
  }
}

# The policy files take their secret and parameter names from the defaults of these variables (scripts/infrastructure/secret_store.py reads
# variables.tf), so these lists are the grouping the files depend on: the shared keys read only, the 15 project parameters read, the 14 managed ones written.
run "project_secret_names" {
  command = plan

  assert {
    condition     = output.project_secret_names.shared_secrets == ["census_api_key", "bls_api_key", "hud_api_key", "jev_api_key"]
    error_message = "The shared API keys are the four Secrets Manager names, in Secrets Manager and read only."
  }

  assert {
    condition = output.project_secret_names.managed_parameters == [
      for name in [
        "postgres_password", "polaris_root_secret", "polaris_reader_client_id", "polaris_reader_client_secret", "polaris_gold_client_id",
        "polaris_gold_client_secret", "polaris_silver_client_id", "polaris_silver_client_secret", "airflow_db_password", "airflow_fernet_key",
        "airflow_jwt_secret", "airflow_api_secret_key", "marquez_db_password", "redis_password",
      ] : "/healthcare_hai_prediction/${name}"
    ]
    error_message = "The managed parameters are exactly the 14 project parameters under /healthcare_hai_prediction/, none of them the Airflow administrator password."
  }

  assert {
    condition     = output.project_secret_names.read_parameters == concat(["/healthcare_hai_prediction/airflow_admin"], output.project_secret_names.managed_parameters)
    error_message = "Parameter read access is the Airflow administrator password and the 14 managed parameters: 15 names."
  }

  assert {
    condition = alltrue([
      for name in concat(output.project_secret_names.shared_secrets, output.project_secret_names.read_parameters) : !strcontains(name, "*") && !strcontains(name, "?")
    ])
    error_message = "No granted name holds a wildcard."
  }

  # The task execution role injects eight of the managed parameters; its policy takes each name from the same variable, not from the project name.
  assert {
    condition = alltrue([
      for resource in flatten([for statement in jsondecode(aws_iam_role_policy.task_execution.policy).Statement : statement.Resource if statement.Sid == "InjectNamedParameters"]) :
      contains([for name in output.project_secret_names.managed_parameters : "arn:aws:ssm:eu-west-1:111111111111:parameter${name}"], resource)
    ])
    error_message = "Every parameter the task execution role injects is one of the managed parameters named by the secret-name variables."
  }
}

run "reject_equal_lakehouse_role_names" {
  command = plan

  variables {
    lakehouse_writer_role_name = "healthcare_hai_prediction_lakehouse_reader"
  }

  expect_failures = [aws_iam_role.lakehouse_writer]
}

run "reject_empty_lakehouse_reader_role_name" {
  command = plan

  variables {
    lakehouse_reader_role_name = ""
  }

  expect_failures = [var.lakehouse_reader_role_name]
}

run "reject_spaced_lakehouse_writer_role_name" {
  command = plan

  variables {
    lakehouse_writer_role_name = "lakehouse writer"
  }

  expect_failures = [var.lakehouse_writer_role_name]
}

run "reject_overlong_lakehouse_writer_role_name" {
  command = plan

  variables {
    lakehouse_writer_role_name = join("", [for index in range(65) : "a"])
  }

  expect_failures = [var.lakehouse_writer_role_name]
}

run "reject_short_lakehouse_session" {
  command = plan

  variables {
    lakehouse_session_max_seconds = 3599
  }

  expect_failures = [var.lakehouse_session_max_seconds]
}

run "reject_long_lakehouse_session" {
  command = plan

  variables {
    lakehouse_session_max_seconds = 43201
  }

  expect_failures = [var.lakehouse_session_max_seconds]
}

run "reject_fractional_lakehouse_session" {
  command = plan

  variables {
    lakehouse_session_max_seconds = 7200.5
  }

  expect_failures = [var.lakehouse_session_max_seconds]
}

run "reject_invalid_bucket_scope" {
  command = plan

  variables {
    data_bucket_name = "example-*"
  }

  expect_failures = [var.data_bucket_name]
}

run "reject_invalid_user" {
  command = plan

  variables {
    aws_profile = "arn:aws:iam::111111111111:user/example_project_dev"
  }

  expect_failures = [var.aws_profile]
}

run "reject_project_profile_for_iam" {
  command = plan

  variables {
    admin_profile = "example_project_dev"
  }

  expect_failures = [var.admin_profile]
}

run "accept_alternate_region" {
  command = plan

  variables {
    aws_region = "ap-southeast-2"
  }

  override_data {
    target = data.aws_ec2_instance_type_offerings.host
    values = {
      locations = ["ap-southeast-2b", "ap-southeast-2a", "eu-west-1a"]
    }
  }

  assert {
    condition     = [for subnet in aws_subnet.public : subnet.availability_zone] == ["ap-southeast-2a", "ap-southeast-2b"]
    error_message = "The subnets use the first two zones of the chosen region that offer the host instance type."
  }
}

run "reject_empty_region" {
  command = plan

  variables {
    aws_region = ""
  }

  expect_failures = [var.aws_region]
}

run "reject_malformed_region" {
  command = plan

  variables {
    aws_region = "ap_southeast_2"
  }

  expect_failures = [var.aws_region]
}

run "reject_padded_region" {
  command = plan

  variables {
    aws_region = " eu-west-1 "
  }

  expect_failures = [var.aws_region]
}

run "reject_null_region" {
  command = plan

  variables {
    aws_region = null
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

run "reject_legacy_user_profile" {
  command = plan

  variables {
    aws_profile = "example_project_user"
  }

  expect_failures = [var.aws_profile]
}

run "reject_empty_secret_name" {
  command = plan

  variables {
    hud_secret_name = ""
  }

  expect_failures = [var.hud_secret_name]
}

run "reject_wildcard_secret_name" {
  command = plan

  variables {
    bls_secret_name = "example*"
  }

  expect_failures = [var.bls_secret_name]
}

run "reject_single_character_wildcard_secret_name" {
  command = plan

  variables {
    jev_secret_name = "example?key"
  }

  expect_failures = [var.jev_secret_name]
}

run "reject_secret_name_with_space" {
  command = plan

  variables {
    census_secret_name = "example key"
  }

  expect_failures = [var.census_secret_name]
}

run "reject_equal_secret_names" {
  command = plan

  variables {
    bls_secret_name = "census_api_key"
  }

  expect_failures = [output.project_secret_names]
}

run "reject_overlong_secret_name" {
  command = plan

  variables {
    hud_secret_name = join("", [for index in range(513) : "a"])
  }

  expect_failures = [var.hud_secret_name]
}

run "reject_empty_postgres_password_name" {
  command = plan

  variables {
    postgres_password_name = ""
  }

  expect_failures = [var.postgres_password_name]
}

run "reject_wildcard_polaris_root_secret_name" {
  command = plan

  variables {
    polaris_root_secret_name = "example*"
  }

  expect_failures = [var.polaris_root_secret_name]
}

run "reject_question_polaris_reader_client_id_name" {
  command = plan

  variables {
    polaris_reader_client_id_name = "example?key"
  }

  expect_failures = [var.polaris_reader_client_id_name]
}

run "reject_space_polaris_reader_client_secret_name" {
  command = plan

  variables {
    polaris_reader_client_secret_name = "example key"
  }

  expect_failures = [var.polaris_reader_client_secret_name]
}

run "reject_overlong_polaris_gold_client_id_name" {
  command = plan

  variables {
    polaris_gold_client_id_name = "/${join("", [for index in range(513) : "a"])}"
  }

  expect_failures = [var.polaris_gold_client_id_name]
}

run "reject_empty_polaris_gold_client_secret_name" {
  command = plan

  variables {
    polaris_gold_client_secret_name = ""
  }

  expect_failures = [var.polaris_gold_client_secret_name]
}

run "reject_wildcard_polaris_silver_client_id_name" {
  command = plan

  variables {
    polaris_silver_client_id_name = "example*"
  }

  expect_failures = [var.polaris_silver_client_id_name]
}

run "reject_question_polaris_silver_client_secret_name" {
  command = plan

  variables {
    polaris_silver_client_secret_name = "example?key"
  }

  expect_failures = [var.polaris_silver_client_secret_name]
}

run "reject_space_airflow_db_password_name" {
  command = plan

  variables {
    airflow_db_password_name = "example key"
  }

  expect_failures = [var.airflow_db_password_name]
}

run "reject_overlong_airflow_fernet_key_name" {
  command = plan

  variables {
    airflow_fernet_key_name = "/${join("", [for index in range(513) : "a"])}"
  }

  expect_failures = [var.airflow_fernet_key_name]
}

run "reject_empty_airflow_jwt_secret_name" {
  command = plan

  variables {
    airflow_jwt_secret_name = ""
  }

  expect_failures = [var.airflow_jwt_secret_name]
}

run "reject_wildcard_airflow_api_secret_key_name" {
  command = plan

  variables {
    airflow_api_secret_key_name = "example*"
  }

  expect_failures = [var.airflow_api_secret_key_name]
}

run "reject_question_marquez_db_password_name" {
  command = plan

  variables {
    marquez_db_password_name = "example?key"
  }

  expect_failures = [var.marquez_db_password_name]
}

run "reject_equal_managed_secret_names" {
  command = plan

  variables {
    airflow_jwt_secret_name = "/healthcare_hai_prediction/airflow_api_secret_key"
  }

  expect_failures = [output.project_secret_names]
}

run "reject_managed_name_equal_to_shared_name" {
  command = plan

  variables {
    postgres_password_name = "/healthcare_hai_prediction/airflow_admin"
  }

  expect_failures = [output.project_secret_names]
}

run "reject_parameter_name_without_leading_slash" {
  command = plan

  variables {
    postgres_password_name = "healthcare_hai_prediction/postgres_password"
  }

  expect_failures = [var.postgres_password_name]
}

run "reject_parameter_name_with_dashes" {
  command = plan

  variables {
    airflow_fernet_key_name = "/healthcare-hai-prediction/airflow-fernet-key"
  }

  expect_failures = [var.airflow_fernet_key_name]
}

run "reject_parameter_name_with_aws_prefix" {
  command = plan

  variables {
    redis_password_name = "/aws/redis_password"
  }

  expect_failures = [var.redis_password_name]
}

run "reject_wildcard_parameter_name" {
  command = plan

  variables {
    marquez_db_password_name = "/healthcare_hai_prediction/*"
  }

  expect_failures = [var.marquez_db_password_name]
}

run "reject_parameter_name_with_trailing_slash" {
  command = plan

  variables {
    airflow_admin_secret_name = "/healthcare_hai_prediction/"
  }

  expect_failures = [var.airflow_admin_secret_name]
}

run "reject_equal_parameter_names" {
  command = plan

  variables {
    redis_password_name = "/healthcare_hai_prediction/postgres_password"
  }

  expect_failures = [output.project_secret_names]
}

run "reject_wide_owner_range" {
  command = plan

  variables {
    owner_ip_cidrs = ["192.0.2.0/24"]
  }

  expect_failures = [var.owner_ip_cidrs]
}

run "reject_open_owner_range" {
  command = plan

  variables {
    owner_ip_cidrs = ["0.0.0.0/0"]
  }

  expect_failures = [var.owner_ip_cidrs]
}

run "reject_ipv6_owner_address" {
  command = plan

  variables {
    owner_ip_cidrs = ["2001:db8::1/128"]
  }

  expect_failures = [var.owner_ip_cidrs]
}

run "reject_budget_above_the_limit" {
  command = plan

  variables {
    budget_limit_usd = 5000
  }

  expect_failures = [var.budget_limit_usd]
}

run "reject_alert_above_one_hundred_percent" {
  command = plan

  variables {
    budget_alert_percentages = [50, 120]
  }

  expect_failures = [var.budget_alert_percentages]
}

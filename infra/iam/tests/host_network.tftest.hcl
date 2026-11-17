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

# The network belongs to this stack, so the project user cannot open a port, add a route or move the host. The platform

# Failure modes this file pins, written before the change:
# - a job container reaches the instance metadata service through the forward path, where the root-only output rule does not look;
# - a job container reaches a host port beyond the four service ports, or the internet beyond port 443 and DNS;
# - the rules block the task credential endpoint, so every job loses its role, or they drop DNS or the service replies;
# - a rule that is not scoped to the container bridge cuts the host's own traffic (ECS agent, Session Manager, Docker);
# - the new table replaces or edits the metadata rule, or a reload loads the table twice or not at all;
# - the host joins the cluster without the scratch volume, so job files fill the data volume;
# - a resized volume keeps its old file system size;
# - the root volume cannot hold the images;
# - the launch template carries more user data than EC2 accepts, so no new template version can start the host.
# - a reply from a host service to a job container leaves through the second interface, because amazon-ec2-net-utils routes everything from the
#   host address out of that interface (policy rule "from <host address> lookup 10001"), so a job cannot reach Polaris, the catalog, Redis or MLflow
#   at the host address (found in the first live run); the fix needs a rule for the bridge subnet ahead of every rule of amazon-ec2-net-utils;
# - the subnet that Docker really uses is never read, so the rule and docker0 can name different subnets, or boot guesses it from unit files and environment files and guesses wrong;
# - docker0 later gets another, a secondary or no address while Docker runs, and the guard that the timer and the agent's pre-start run still accepts it, or the first read of docker0 succeeds after its deadline;
# - the rule is set once, lost when the interface's network setup or a reconfiguration drops it, and never restored, or a host that cannot set it joins the cluster.

run "job_firewall_is_a_second_table_beside_the_unchanged_metadata_rule" {
  command = plan

  assert {
    condition = (
      length(regexall("(?m)^table inet platform_[a-z]+ \\{", local.host_user_data)) == 2 &&
      alltrue([
        for text in [
          "table inet platform_imds\ndelete table inet platform_imds\ntable inet platform_imds {\n  chain output {\n    type filter hook output priority -10; policy accept;\n    ip daddr 169.254.169.254 meta skuid != 0 drop\n  }\n}\n",
          "table inet platform_jobs\ndelete table inet platform_jobs\ntable inet platform_jobs {"
        ] : strcontains(local.host_user_data, text)
      ])
    )
    error_message = "The job firewall is its own table, deleted and added again in one transaction, and the metadata table keeps its exact text."
  }

  # One file, one transaction: the guard, the firewall service include and a reload load both tables together.
  assert {
    condition = (
      strcontains(split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0], "table inet platform_imds {") &&
      strcontains(split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0], "table inet platform_jobs {") &&
      !strcontains(local.host_user_data, "iptables")
    )
    error_message = "Both tables sit in the one rules file that the guard and the firewall configuration load."
  }
}

run "job_firewall_rules_cover_the_bridge_and_nothing_else" {
  command = plan

  assert {
    condition = (
      alltrue([
        for text in [
          "chain input {\n    type filter hook input priority 0; policy accept;",
          "chain forward {\n    type filter hook forward priority 0; policy accept;",
          "iifname \"docker0\" ip daddr 169.254.169.254 drop",
          "iifname \"docker0\" ct state established,related accept",
          "iifname \"docker0\" udp dport 53 accept",
          "iifname \"docker0\" tcp dport 53 accept",
          "iifname \"docker0\" ip daddr 169.254.170.2 tcp dport 80 accept",
          "iifname \"docker0\" ip daddr 127.0.0.1 tcp dport 51679 accept",
          "iifname \"docker0\" fib daddr type local tcp dport { 5100, 6379, 8181, 8585, 9091 } accept",
          "iifname \"docker0\" tcp dport 443 accept",
          "iifname \"docker0\" drop"
        ] : strcontains(split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0], text)
      ])
    )
    error_message = "The bridge may reach DNS, the task credential endpoint, Polaris, Redis, MLflow, the catalog and port 443, and nothing else."
  }

  # Every accept and drop of the job table names the bridge, so the host's own traffic (ECS agent, Session Manager, Docker) keeps its policy.
  assert {
    condition = alltrue([
      for line in split("\n", split("table inet platform_jobs {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1]) :
      strcontains(line, "iifname \"docker0\"") if endswith(trimspace(line), "accept") && !strcontains(line, "policy") || endswith(trimspace(line), "drop") && !strcontains(line, "policy")
    ])
    error_message = "A job firewall rule that does not name the container bridge would reach the host's own traffic."
  }

  assert {
    condition = (
      !strcontains(split("table inet platform_jobs {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1], "policy drop") &&
      length(regexall("iifname \"docker0\" drop", split("table inet platform_jobs {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1])) == 2
    )
    error_message = "Both chains end in a bridge-scoped drop and neither sets a drop policy."
  }

  # The metadata address is dropped in the forward chain before port 443 is accepted, and each chain ends with the drop.
  assert {
    condition = (
      length(split("tcp dport 443 accept", split("chain forward {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1])[0]) >
      length(split("ip daddr 169.254.169.254 drop", split("chain forward {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1])[0]) &&
      endswith(trimspace(split("\n  }", split("chain forward {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1])[0]), "iifname \"docker0\" drop") &&
      endswith(trimspace(split("\n  }", split("chain input {", split("\nRULES\n", split("<<'RULES'\n", local.host_user_data)[1])[0])[1])[0]), "iifname \"docker0\" drop")
    )
    error_message = "The metadata address is dropped before port 443 is accepted, and each chain ends with the bridge drop."
  }
}

run "the_job_firewall_file_is_the_text_boot_writes" {
  command = plan

  assert {
    condition     = strcontains(local.host_user_data, file("${path.module}/host_job_firewall.nft"))
    error_message = "Boot writes the tracked job firewall file as it is, so the syntax check of that file checks what the host loads."
  }
}

# EC2 accepts at most 16,384 raw bytes of user data before base64 encoding. The text is larger, so the template sends it gzip
# compressed, which cloud-init on the Amazon Linux 2023 image expands before it runs the script. Base64 gives 4 characters for
# every 3 raw bytes, so the encoded length bounds the raw size.
run "the_launch_template_sends_the_boot_script_gzip_compressed_within_the_ec2_limit" {
  command = plan

  assert {
    condition     = aws_launch_template.host.user_data == base64gzip(local.host_user_data) && startswith(aws_launch_template.host.user_data, "H4sI")
    error_message = "The launch template sends the boot script as gzip data, which cloud-init expands: ${length(aws_launch_template.host.user_data)} encoded characters."
  }

  assert {
    condition     = length(aws_launch_template.host.user_data) * 3 / 4 <= 14336
    error_message = "The raw user data stays at least 2,048 bytes under the 16,384 bytes EC2 accepts: ${length(aws_launch_template.host.user_data) * 3 / 4} bytes now."
  }
}

run "root_volume_holds_the_images" {
  command = plan

  assert {
    condition     = alltrue([for device in aws_launch_template.host.block_device_mappings : one(device.ebs).encrypted == "true" && one(device.ebs).volume_size == 80 && one(device.ebs).volume_type == "gp3"])
    error_message = "The root volume is an encrypted 80 GB gp3 volume: the service and job images do not fit in 30 GB, and a root volume cannot grow without a new instance."
  }
}

run "boot_mounts_the_scratch_volume_and_grows_both_file_systems_before_joining" {
  command = plan

  assert {
    condition = (
      alltrue([
        for text in [
          "scratch_volume_name=\"example_project_host_scratch_dev\"",
          "scratch_path=\"/data/job_scratch\"",
          "current_step=\"Finding the scratch volume\"\nscratch=$(run_within 60 \"$current_step\" aws ec2 describe-volumes",
          "\"Name=tag:Name,Values=$scratch_volume_name\"",
          "Exactly one scratch volume must carry the project tags",
          "run_within 60 \"Attaching the scratch volume\" aws ec2 attach-volume",
          "--device /dev/xvdg",
          "run_within 300 \"Formatting the scratch volume\" mkfs.xfs",
          "run_within 120 \"Growing the data volume file system\" xfs_growfs \"$mount_path\"",
          "run_within 120 \"Growing the scratch volume file system\" xfs_growfs \"$scratch_path\"",
          "run_within 30 \"Checking the scratch mount\" mountpoint -q \"$scratch_path\""
        ] : strcontains(local.host_user_data, text)
      ]) &&
      alltrue([
        for text in [
          "Mounting the scratch volume", "Growing the data volume file system", "Growing the scratch volume file system", "Checking the scratch mount"
        ] : length(split("ECS_CLUSTER=", local.host_user_data)[0]) > length(split(text, local.host_user_data)[0])
      ]) &&
      length(split("Mounting the data volume", local.host_user_data)[0]) < length(split("Mounting the scratch volume", local.host_user_data)[0])
    )
    error_message = "Boot finds, attaches, formats and mounts the scratch volume under the data mount after the data volume, grows both file systems and checks the scratch mount, all before ECS_CLUSTER is written."
  }
}

run "the_host_role_attaches_the_scratch_volume_by_its_own_name_and_project_tag" {
  command = plan

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement if statement.Sid == "AttachScratchVolume"
      ]) == {
      Sid       = "AttachScratchVolume"
      Effect    = "Allow"
      Action    = "ec2:AttachVolume"
      Resource  = "arn:aws:ec2:eu-west-1:111111111111:volume/*"
      Condition = { StringEquals = { "aws:ResourceTag/Name" = "example_project_host_scratch_dev", "aws:ResourceTag/Project" = "example_project" } }
    }
    error_message = "The host role attaches the scratch volume only with its exact Name and Project tags, in a statement beside the data volume's."
  }

  assert {
    condition = one([
      for statement in jsondecode(aws_iam_role_policy.ecs_host.policy).Statement : statement if statement.Sid == "AttachDataVolume"
      ]) == {
      Sid       = "AttachDataVolume"
      Effect    = "Allow"
      Action    = "ec2:AttachVolume"
      Resource  = "arn:aws:ec2:eu-west-1:111111111111:volume/*"
      Condition = { StringEquals = { "aws:ResourceTag/Name" = "example_project_host_data_dev", "aws:ResourceTag/Project" = "example_project" } }
    }
    error_message = "The data volume statement keeps its exact condition."
  }
}

# The scratch fstab line is nofail, so a later boot without the scratch volume must not start Docker or the ECS agent, and
# the watchdog must notice the job table missing although the metadata table remains. The E2E starts both services with
# the scratch volume absent and deletes the job table alone through stand-ins; this pins the text the host receives.
run "docker_and_the_agent_require_the_scratch_mount_and_the_watchdog_checks_both_tables" {
  command = plan

  assert {
    condition = alltrue([
      for text in [
        "mountpoint_command=$(command -v mountpoint)\nfor unit in docker.service ecs.service; do\n  cat >\"$systemd_directory/$unit.d/platform_scratch_mount.conf\" <<UNIT\n[Unit]\nRequiresMountsFor=$scratch_path\n\n[Service]\nExecStartPre=$mountpoint_command -q $scratch_path\nUNIT\ndone\n",
        "run_within 60 \"Reloading systemd units for the new mounts\" systemctl daemon-reload",
        "nft list table inet platform_imds >/dev/null \\\n    && timeout --kill-after=5 30 nft list table inet platform_jobs >/dev/null \\\n    && probe_blocked \\\n"
      ] : strcontains(local.host_user_data, text)
    ])
    error_message = "Docker and the ECS agent require the scratch mount at every start, systemd reloads the new fstab lines and the watchdog checks the job table beside the metadata table."
  }
}

# The tests above pin the firewall text; this one pins the job bridge routing rule: boot reads the subnet from docker0 after Docker starts, the guard sets and checks the
# rule at a priority below every rule of amazon-ec2-net-utils (10000 and above), and boot sets it again after the host address is up and before the volumes and ECS.
run "the_job_bridge_subnet_is_read_from_docker0_and_routed_ahead_of_the_interface_rules" {
  command = plan

  assert {
    condition = (
      alltrue([
        for text in [
          "docker_bridge_address=\"172.17.0.1/16\"",
          "docker_bridge_subnet=\"172.17.0.0/16\"",
          "docker_bridge_poll_seconds=2",
          "docker_bridge_wait_seconds=60"
        ] : strcontains(local.host_user_data, text)
      ]) &&
      !anytrue([
        for text in [
          "daemon.json",
          "systemctl cat",
          "docker_options_file",
          "settings[\"bip\"]",
          "--bip"
        ] : strcontains(local.host_user_data, text)
      ])
    )
    error_message = "Boot names the one bridge address and its wait, and no longer guesses Docker's bridge address from its daemon file, unit text, options or environment files, or writes the daemon file."
  }

  assert {
    condition = (
      alltrue([
        for text in [
          "run_within 120 \"Starting Docker\" systemctl start docker.service",
          "Docker did not start, so the job bridge subnet cannot be checked and the host does not join the cluster.",
          "ip -o -4 addr show dev docker0",
          "docker0 did not appear within $docker_bridge_wait_seconds seconds",
          "if ! bridge_address_exact \"$shown\" \"$docker_bridge_address\"; then",
          "remaining=$((deadline - SECONDS))",
          "query_seconds=$((remaining - 1))",
          "timeout --kill-after=1 \"$query_seconds\" ip -o -4 addr show dev docker0",
          "docker0 answered after the $docker_bridge_wait_seconds second limit",
          "docker0 holds $bridge_addresses instead of the required $docker_bridge_address ($docker_bridge_subnet)"
        ] : strcontains(local.host_user_data, text)
      ]) &&
      length(split("Reloading systemd units for the new mounts", local.host_user_data)[0]) < length(split("step \"Starting Docker\"", local.host_user_data)[0]) &&
      length(split("step \"Starting Docker\"", local.host_user_data)[0]) < length(split("step \"Waiting for docker0\"", local.host_user_data)[0]) &&
      length(split("step \"Waiting for docker0\"", local.host_user_data)[0]) < length(split("step \"Checking the job bridge subnet\"", local.host_user_data)[0]) &&
      length(split("step \"Checking the job bridge subnet\"", local.host_user_data)[0]) < length(split("step \"Writing the ECS agent configuration\"", local.host_user_data)[0])
    )
    error_message = "After the scratch mount, boot starts Docker, waits for docker0 under a deadline, refuses any address but the one the routing rule names, and does all of it before the ECS configuration is written."
  }

  assert {
    condition = (
      length(regexall("(?m)^bridge_rule_priority=[0-9]+$", local.host_user_data)) == 1 &&
      tonumber(regex("(?m)^bridge_rule_priority=([0-9]+)$", local.host_user_data)[0]) > 0 &&
      tonumber(regex("(?m)^bridge_rule_priority=([0-9]+)$", local.host_user_data)[0]) < 10000
    )
    error_message = "The bridge rule priority is below 10000, where amazon-ec2-net-utils starts its rules (the second interface's rule is at 10001)."
  }

  assert {
    condition = alltrue([
      for text in [
        "bridge_priority=%q",
        "'NF == 7 && $1 == priority && $2 == \"from\" && $3 == \"all\" && $4 == \"to\" && $5 == subnet && $6 == \"lookup\" && $7 == \"main\"",
        "ip -4 rule add to \"$bridge_subnet\" lookup main priority \"$bridge_priority\"",
        "Could not set the routing rule for the job bridge.",
        "The job bridge routing rule is not ahead of every table rule.",
        "docker0 does not hold the required address",
        "docker_running() {",
        "systemctl is-active --quiet docker.service",
        "bridge_address_exact \"$shown\" \"$bridge_address\"",
        "  declare -f bridge_address_exact\n",
        "alone (it has another or a secondary address, or is missing while Docker runs).",
        "  route) ensure_bridge_rule ;;",
        "&& bridge_rule_present \\\n    && bridge_rule_first\n}",
        "  ensure_bridge_rule || return 1\n"
      ] : strcontains(local.host_user_data, text)
    ])
    error_message = "The guard sets the bridge rule with apply and route, checks it with check (so the timer restores it and stops Docker when it cannot), and names each failure. It holds docker0 to the same single exact address as boot, and a missing docker0 passes only while Docker is not running."
  }

  assert {
    condition = (
      length(split("is up on $interface_id", local.host_user_data)[0]) < length(split("run_within 60 \"Setting the routing rule for the job bridge\" \"$guard_script\" route", local.host_user_data)[0]) &&
      length(split("run_within 60 \"Setting the routing rule for the job bridge\" \"$guard_script\" route", local.host_user_data)[0]) < length(split("\"Attaching the data volume\"", local.host_user_data)[0]) &&
      length(split("Setting the routing rule for the job bridge", local.host_user_data)[0]) < length(split("ECS_CLUSTER=", local.host_user_data)[0])
    )
    error_message = "Boot sets the bridge rule after the host address is up and before the volumes attach and ECS_CLUSTER is written, and fails there when the rule cannot be set."
  }
}

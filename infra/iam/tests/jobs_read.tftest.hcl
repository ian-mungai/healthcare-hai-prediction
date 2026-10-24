mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["eu-west-1a", "eu-west-1b"] }
  }

  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev" }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = { arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev" }
  }

  mock_resource "aws_vpc_endpoint" {
    defaults = { prefix_list_id = "pl-0123456789abcdef0" }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values          = { id = "lt-0123456789abcdef0", latest_version = 1 }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values          = { arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev" }
}

override_resource {
  target          = aws_vpc.platform
  override_during = plan
  values          = { id = "vpc-0123456789abcdef0" }
}

override_resource {
  target          = aws_security_group.host
  override_during = plan
  values          = { id = "sg-00000000000000001" }
}

override_resource {
  target          = aws_security_group.job["offline"]
  override_during = plan
  values          = { id = "sg-00000000000000002" }
}

override_resource {
  target          = aws_security_group.job["lakehouse"]
  override_during = plan
  values          = { id = "sg-00000000000000003" }
}

override_resource {
  target          = aws_security_group.job["endpoints"]
  override_during = plan
  values          = { id = "sg-00000000000000004" }
}

override_resource {
  target          = aws_security_group.job["ssm_endpoint"]
  override_during = plan
  values          = { id = "sg-00000000000000005" }
}

override_resource {
  target          = aws_subnet.public
  override_during = plan
  values          = { id = "subnet-0123456789abcdef0" }
}

override_resource {
  target          = aws_iam_role.job["execution"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLEEXECUTION" }
}

override_resource {
  target          = aws_iam_role.job["offline_task"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLEOFFLINETASK" }
}

override_resource {
  target          = aws_iam_role.job["seal_task"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLESEALTASK" }
}

override_resource {
  target          = aws_iam_role.job["read_task"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLEREADTASK" }
}

override_resource {
  target          = aws_iam_role.job["try_writer"]
  override_during = plan
  values          = { unique_id = "AROEXAMPLETRYWRITER" }
}

override_resource {
  target          = aws_route_table.public
  override_during = plan
  values          = { id = "rtb-0123456789abcdef0" }
}

override_resource {
  target          = aws_security_group.task
  override_during = plan
  values          = { id = "sg-00000000000000006" }
}

variables {
  aws_profile           = "example_project_dev"
  environment           = "dev"
  admin_profile         = "example_admin"
  aws_region            = "eu-west-1"
  expected_account_id   = "111111111111"
  data_bucket_name      = "example-project-ci-bucket"
  project_name          = "example_project"
  owner_ip_cidrs        = ["192.0.2.10/32"]
  host_ingress_ports    = [8443]
  job_endpoints_enabled = true
}

run "read_roles_without_writers" {
  command = plan

  assert {
    condition = (
      toset(keys(aws_iam_role.job)) == toset(["execution", "offline_task", "seal_task", "read_task", "try_writer"]) &&
      alltrue([for key, role in aws_iam_role.job : role.name == "example_project_job_${key}_dev"]) &&
      aws_iam_role.job["try_writer"].max_session_duration == 43200 &&
      alltrue([for key, role in aws_iam_role.job : role.max_session_duration == 3600 if key != "try_writer"])
    )
    error_message = "The early read slice creates exactly five roles, with no lakehouse writer role."
  }

  assert {
    condition     = aws_iam_role.job["try_writer"].description == "Write-only upper bound for staged try outputs."
    error_message = "The try output role description must identify its write-only bound."
  }

  assert {
    condition = (
      alltrue([
        lookup(coalesce(aws_iam_role.job["execution"].tags, {}), "Project", "") == "example_project",
        lookup(coalesce(aws_iam_role.job["offline_task"].tags, {}), "Project", "") == "example_project",
        lookup(coalesce(aws_iam_role.job["seal_task"].tags, {}), "Project", "") == "example_project",
        lookup(coalesce(aws_iam_role.job["read_task"].tags, {}), "Project", "") == "example_project",
        lookup(coalesce(aws_iam_role.job["try_writer"].tags, {}), "Project", "") == "example_project"
      ]) &&
      alltrue([for group in aws_security_group.job : lookup(coalesce(group.tags, {}), "Project", "") == "example_project"]) &&
      alltrue([for endpoint in aws_vpc_endpoint.job_interface : lookup(coalesce(endpoint.tags, {}), "Project", "") == "example_project"]) &&
      lookup(coalesce(aws_vpc_endpoint.job_s3[0].tags, {}), "Project", "") == "example_project" &&
      lookup(coalesce(aws_vpc_endpoint.job_ssm[0].tags, {}), "Project", "") == "example_project"
    )
    error_message = "Jobs resources must carry the Project tag used by foundation discovery."
  }

  assert {
    condition = (
      alltrue([for key, role in aws_iam_role.job : jsondecode(role.assume_role_policy) == jsondecode(local.ecs_tasks_trust) if key != "try_writer"]) &&
      jsondecode(aws_iam_role.job["try_writer"].assume_role_policy).Statement[0].Principal.AWS == "arn:aws:iam::111111111111:user/example_project_dev" &&
      jsondecode(aws_iam_role.job["try_writer"].assume_role_policy).Statement[0].Condition.StringLike["sts:RoleSessionName"] == "hai_try_*"
    )
    error_message = "Task roles use the foundation's account-scoped ECS trust; only the project user assumes the output role."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.job_source_read.policy).Statement[0].Action) == toset(["s3:GetObject", "s3:GetObjectVersion"]) &&
      toset(jsondecode(aws_iam_role_policy.job_source_read.policy).Statement[0].Resource) == toset([
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/bronze/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/silver/*",
        "arn:aws:s3:::example-project-ci-bucket/lakehouse/gold/*"
      ]) &&
      jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/grants/$${aws:userid}.json" &&
      jsondecode(aws_iam_role_policy.job_stage_read["read_task"].policy).Statement[1].Action == "s3:GetObject"
    )
    error_message = "Read scope is versioned source reads and the session's own current grant, with no qualification or writer prefix."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.job_execution.policy).Statement[3].Resource) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_reader_client_id",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/aws_polaris_reader_client_secret"
      ]) &&
      jsondecode(aws_iam_role_policy.job_execution.policy).Statement[3].Action == "ssm:GetParameters" &&
      length(jsondecode(aws_iam_role_policy.job_execution.policy).Statement[1].Resource) == 3 &&
      aws_iam_role_policy.job_execution.role == aws_iam_role.job["execution"].name &&
      aws_iam_role_policy.job_source_read.role == aws_iam_role.job["read_task"].name
    )
    error_message = "The separate jobs execution role pulls all three job images and injects the reader pair only."
  }

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.job_try_writer.policy).Statement[0].Action) == toset(["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]) &&
      jsondecode(aws_iam_role_policy.job_try_writer.policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/tries/*" &&
      jsondecode(aws_iam_role_policy.job_seal_deny.policy).Statement[0].Effect == "Deny" &&
      jsondecode(aws_iam_role_policy.job_seal_deny.policy).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/lakehouse/run_stage/*/inputs/trees/*"
    )
    error_message = "The output upper bound is write-only under tries, and seal tasks cannot read an input tree."
  }

  assert {
    condition = (
      toset(keys(output.job_roles)) == toset(keys(aws_iam_role.job)) &&
      alltrue([for key, role in output.job_roles : role.unique_id == aws_iam_role.job[key].unique_id]) &&
      length(output.job_project_policy_delta.Statement[0].Resource) == 4 &&
      output.job_project_policy_delta.Statement[0].Condition.StringEquals["iam:PassedToService"] == "ecs-tasks.amazonaws.com"
    )
    error_message = "Role IDs are exported instead of queried at runtime; the unattached project-policy proposal passes exactly four ECS roles."
  }
}

run "closed_read_endpoints" {
  command = plan

  assert {
    condition = (
      length(aws_vpc_endpoint.job_s3) == 1 &&
      aws_vpc_endpoint.job_s3[0].vpc_endpoint_type == "Gateway" &&
      aws_vpc_endpoint.job_s3[0].route_table_ids == toset([aws_route_table.public.id]) &&
      toset(keys(aws_vpc_endpoint.job_interface)) == toset(["ecr.api", "ecr.dkr", "logs"]) &&
      alltrue([for endpoint in aws_vpc_endpoint.job_interface : endpoint.private_dns_enabled && endpoint.vpc_endpoint_type == "Interface" && endpoint.subnet_ids == toset([aws_subnet.public[0].id])]) &&
      aws_vpc_endpoint.job_ssm[0].private_dns_enabled &&
      aws_vpc_endpoint.job_ssm[0].security_group_ids == toset([aws_security_group.job["ssm_endpoint"].id])
    )
    error_message = "S3 uses the public route table; four interface endpoints use private DNS in one job subnet, with SSM isolated."
  }

  assert {
    condition = (
      toset(keys(aws_vpc_security_group_egress_rule.job_s3)) == toset(["offline", "lakehouse"]) &&
      alltrue([for rule in aws_vpc_security_group_egress_rule.job_s3 : rule.ip_protocol == "tcp" && rule.from_port == 443 && rule.to_port == 443 && rule.prefix_list_id == aws_vpc_endpoint.job_s3[0].prefix_list_id && rule.cidr_ipv4 == null && rule.cidr_ipv6 == null]) &&
      alltrue([for rule in aws_vpc_security_group_egress_rule.job_shared_endpoints : rule.referenced_security_group_id == aws_security_group.job["endpoints"].id && rule.from_port == 443 && rule.to_port == 443 && rule.cidr_ipv4 == null && rule.cidr_ipv6 == null]) &&
      aws_vpc_security_group_egress_rule.job_ssm[0].security_group_id == aws_security_group.job["lakehouse"].id &&
      aws_vpc_security_group_ingress_rule.job_ssm["lakehouse"].referenced_security_group_id == aws_security_group.job["lakehouse"].id &&
      aws_vpc_security_group_ingress_rule.job_ssm["lakehouse"].security_group_id == aws_security_group.job["ssm_endpoint"].id
    )
    error_message = "Offline jobs reach only S3 and image/log endpoints. Only the lakehouse group reaches SSM."
  }

  assert {
    condition = (
      toset([for rule in aws_vpc_security_group_ingress_rule.job_shared_endpoints : rule.referenced_security_group_id]) == toset([
        aws_security_group.job["offline"].id, aws_security_group.job["lakehouse"].id, aws_security_group.host.id, aws_security_group.task.id
      ]) &&
      alltrue([for rule in aws_vpc_security_group_ingress_rule.job_shared_endpoints : rule.security_group_id == aws_security_group.job["endpoints"].id && rule.ip_protocol == "tcp" && rule.from_port == 443 && rule.to_port == 443 && rule.cidr_ipv4 == null && rule.cidr_ipv6 == null]) &&
      toset([for rule in aws_vpc_security_group_ingress_rule.job_ssm : rule.referenced_security_group_id]) == toset([
        aws_security_group.job["lakehouse"].id, aws_security_group.host.id, aws_security_group.task.id
      ]) &&
      alltrue([for rule in aws_vpc_security_group_ingress_rule.job_ssm : rule.security_group_id == aws_security_group.job["ssm_endpoint"].id && rule.ip_protocol == "tcp" && rule.from_port == 443 && rule.to_port == 443 && rule.cidr_ipv4 == null && rule.cidr_ipv6 == null])
    )
    error_message = "Private DNS must preserve host and foundation task HTTPS access. Only lakehouse jobs, the host and foundation tasks reach SSM."
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.job_catalog.security_group_id == aws_security_group.job["lakehouse"].id &&
      aws_vpc_security_group_egress_rule.job_catalog.referenced_security_group_id == aws_security_group.host.id &&
      aws_vpc_security_group_egress_rule.job_catalog.from_port == 8181 && aws_vpc_security_group_egress_rule.job_catalog.to_port == 8181 &&
      aws_vpc_security_group_ingress_rule.host_from_job_catalog.security_group_id == aws_security_group.host.id &&
      aws_vpc_security_group_ingress_rule.host_from_job_catalog.referenced_security_group_id == aws_security_group.job["lakehouse"].id &&
      aws_vpc_security_group_ingress_rule.host_from_job_catalog.from_port == 8181 && aws_vpc_security_group_ingress_rule.host_from_job_catalog.to_port == 8181 &&
      alltrue([for group in aws_security_group.job : length(group.ingress) == 0 && length(group.egress) == 0])
    )
    error_message = "The reciprocal catalog rules permit only lakehouse jobs on TCP 8181. No default or public group rule is added."
  }
}

run "endpoints_disabled" {
  command = plan

  variables {
    job_endpoints_enabled = false
  }

  assert {
    condition = (
      length(aws_vpc_endpoint.job_s3) == 0 && length(aws_vpc_endpoint.job_interface) == 0 && length(aws_vpc_endpoint.job_ssm) == 0 &&
      length(aws_vpc_security_group_egress_rule.job_s3) == 0 && length(aws_vpc_security_group_egress_rule.job_shared_endpoints) == 0 &&
      length(aws_vpc_security_group_ingress_rule.job_shared_endpoints) == 0 && length(aws_vpc_security_group_egress_rule.job_ssm) == 0 &&
      length(aws_vpc_security_group_ingress_rule.job_ssm) == 0 && length(aws_iam_role.job) == 5
    )
    error_message = "Disabling the optional endpoints removes their resources and routes while retaining the five role definitions."
  }
}

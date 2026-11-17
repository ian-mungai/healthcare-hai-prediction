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
  target          = aws_iam_role.train_task
  override_during = plan
  values          = { unique_id = "AROEXAMPLETRAINTASK" }
}

override_resource {
  target          = aws_iam_role.mlflow_task
  override_during = plan
  values          = { unique_id = "AROEXAMPLEMLFLOWTASK" }
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
  training_prepared     = true
}

run "training_is_off_by_default" {
  command = plan

  variables {
    training_prepared = false
  }

  assert {
    condition = (
      length(aws_iam_role.train_task) == 0 && length(aws_iam_role.mlflow_task) == 0 &&
      length(aws_iam_role_policy.train_policy) == 0 && length(aws_iam_role_policy.mlflow_artifacts) == 0 &&
      length(aws_iam_role_policy.train_image_pull) == 0 && length(aws_iam_role_policy.mlflow_parameter) == 0 && length(aws_iam_role_policy.train_parameter) == 0 &&
      output.training_roles == {}
    )
    error_message = "Without training_prepared the IAM root adds no role, no group and no rule for training."
  }
}

run "training_roles_are_narrow" {
  command = plan

  assert {
    condition = (
      aws_iam_role.train_task[0].name == "example_project_job_train_task_dev" &&
      aws_iam_role.mlflow_task[0].name == "example_project_mlflow_task_dev" &&
      aws_iam_role.train_task[0].assume_role_policy == local.ecs_tasks_trust &&
      aws_iam_role.mlflow_task[0].assume_role_policy == local.ecs_tasks_trust &&
      aws_iam_role.train_task[0].max_session_duration == 3600 && aws_iam_role.mlflow_task[0].max_session_duration == 3600 &&
      lookup(coalesce(aws_iam_role.train_task[0].tags, {}), "Project", "") == "example_project" &&
      lookup(coalesce(aws_iam_role.mlflow_task[0].tags, {}), "Project", "") == "example_project"
    )
    error_message = "Both roles carry the project prefix, the ECS task trust, a one hour session cap and the Project tag."
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.train_policy)) == toset(["stage_read", "features_read", "dvc_baseline", "promote_receipts", "research_records"]) &&
      aws_iam_role_policy.train_policy["stage_read"].policy == local.job_policy_documents.job_stage_read &&
      aws_iam_role_policy.train_policy["features_read"].policy == local.training_policy_documents.job_features_read &&
      aws_iam_role_policy.train_policy["dvc_baseline"].policy == local.training_policy_documents.job_dvc_baseline &&
      aws_iam_role_policy.train_policy["promote_receipts"].policy == local.training_policy_documents.promote_receipts &&
      alltrue([for policy in aws_iam_role_policy.train_policy : policy.role == aws_iam_role.train_task[0].name]) &&
      aws_iam_role_policy.mlflow_artifacts[0].role == aws_iam_role.mlflow_task[0].name &&
      aws_iam_role_policy.mlflow_artifacts[0].policy == local.training_policy_documents.mlflow_artifacts
    )
    error_message = "The training role holds the stage read, the feature read, the DVC grant and the promotion receipt grant. The MLflow role holds the mlflow/ artifact grant alone. Neither holds a lakehouse write."
  }

  assert {
    condition     = jsondecode(local.training_policy_documents.mlflow_artifacts).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/mlflow/*"
    error_message = "The MLflow grant names the data bucket mlflow/ prefix."
  }

  assert {
    condition = (
      jsondecode(local.training_policy_documents.promote_receipts).Statement[0].Action == "s3:PutObject" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[0].Resource == "arn:aws:s3:::example-project-ci-bucket/promote/*" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[0].Condition.StringEquals["s3:if-none-match"] == "*" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[1].Action == "s3:GetObject" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[1].Resource == "arn:aws:s3:::example-project-ci-bucket/promote/*" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[2].Action == "s3:ListBucket" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[2].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      jsondecode(local.training_policy_documents.promote_receipts).Statement[2].Condition.StringLike["s3:prefix"] == ["promote/*"] &&
      length(jsondecode(local.training_policy_documents.promote_receipts).Statement) == 3
    )
    error_message = "The promotion record grant creates objects under promote/ only when none exists (receipts, intents, the per-model lease records under promote/models/ and the Production record of each infection type under promote/production/), reads them back, lists the promote/ prefix alone so the run and the model lease are found by an explicit listing, and allows nothing else."
  }

  assert {
    condition = (
      output.training_roles.train_task.arn == aws_iam_role.train_task[0].arn && output.training_roles.mlflow_task.arn == aws_iam_role.mlflow_task[0].arn &&
      output.training_roles.train_task.unique_id == "AROEXAMPLETRAINTASK" && toset(keys(output.training_roles)) == toset(["train_task", "mlflow_task"]) &&
      !contains(keys(output.job_roles), "train_task") && !contains(keys(output.job_roles), "mlflow_task")
    )
    error_message = "The roles are exported apart from job_roles, whose consumers expect the earlier set."
  }
}

run "execution_roles_get_exactly_what_the_new_tasks_inject" {
  command = plan

  assert {
    condition = (
      aws_iam_role_policy.train_image_pull[0].role == aws_iam_role.job["execution"].name &&
      aws_iam_role_policy.mlflow_parameter[0].role == aws_iam_role.task_execution.name &&
      jsondecode(aws_iam_role_policy.train_image_pull[0].policy).Statement[0].Resource == [
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_training_dev",
        "arn:aws:ecr:eu-west-1:111111111111:repository/example_project_feature_store_dev",
      ] &&
      jsondecode(aws_iam_role_policy.mlflow_parameter[0].policy).Statement[0].Resource == [
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/mlflow_db_password",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/mlflow_admin_password",
      ] &&
      aws_iam_role_policy.train_parameter[0].role == aws_iam_role.job["execution"].name &&
      jsondecode(aws_iam_role_policy.train_parameter[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/mlflow_admin_password"] &&
      jsondecode(aws_iam_role_policy.train_parameter[0].policy).Statement[0].Action == "ssm:GetParameters" &&
      jsondecode(aws_iam_role_policy.mlflow_parameter[0].policy).Statement[0].Action == "ssm:GetParameters"
    )
    error_message = "The job execution role may pull the two training images and no other new repository, and inject the one MLflow administrator password. The host execution role may inject the MLflow database password and the administrator password."
  }
}

run "training_adds_no_awsvpc_network" {
  command = plan

  assert {
    condition = (
      length(aws_iam_role.train_task) == 1 &&
      toset(keys(aws_security_group.job)) == toset(["endpoints", "ssm_endpoint"]) &&
      toset(keys(output.job_network)) == toset(["subnet_id", "endpoints_enabled"])
    )
    error_message = "Preparing training adds no security group and no rule: every job runs on the host, so the only job groups are the two endpoint groups and job_network names no class group."
  }
}

run "the_train_role_stores_and_reads_the_research_records_only_under_research" {
  command = plan

  assert {
    condition = (
      aws_iam_role_policy.train_policy["research_records"].role == aws_iam_role.train_task[0].name &&
      jsondecode(aws_iam_role_policy.train_policy["research_records"].policy).Statement == [
        {
          Sid       = "CreateResearchRecords"
          Effect    = "Allow"
          Action    = "s3:PutObject"
          Resource  = "arn:aws:s3:::example-project-ci-bucket/research/*"
          Condition = { StringEquals = { "s3:if-none-match" = "*" } }
        },
        {
          Sid      = "ReadResearchRecords"
          Effect   = "Allow"
          Action   = "s3:GetObject"
          Resource = "arn:aws:s3:::example-project-ci-bucket/research/*"
        },
        {
          Sid       = "ListResearchRecords"
          Effect    = "Allow"
          Action    = "s3:ListBucket"
          Resource  = "arn:aws:s3:::example-project-ci-bucket"
          Condition = { StringLike = { "s3:prefix" = ["research/*"] } }
        },
      ]
    )
    error_message = "The training role creates objects under research/ only when none exists (the research records, the canary results and the notice receipts), reads them back and lists the research/ prefix alone, so a missing record answers 404. It overwrites nothing and reaches no other prefix."
  }

  assert {
    condition     = !contains(keys(aws_iam_role_policy.train_policy), "owner_notice")
    error_message = "Without a topic the training role may publish nothing."
  }
}

run "the_train_role_publishes_to_the_one_alerts_topic" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
  }

  assert {
    condition = (
      aws_iam_role_policy.train_policy["owner_notice"].role == aws_iam_role.train_task[0].name &&
      jsondecode(aws_iam_role_policy.train_policy["owner_notice"].policy).Statement == [
        {
          Sid      = "PublishOwnerNotices"
          Effect   = "Allow"
          Action   = "sns:Publish"
          Resource = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
        }
      ]
    )
    error_message = "The training role gains one action, sns:Publish, on the one alerts topic. It cannot subscribe, create, change or read a topic, and it reaches no other topic."
  }
}

run "the_alerts_topic_must_be_an_sns_topic_arn" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:*"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "grafana_publishes_to_the_one_alerts_topic_only_when_told_the_topic" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.grafana_alerts) == 0
    error_message = "Without a topic the control plane role gains no alert grant."
  }
}

run "the_control_plane_role_gets_two_sns_actions_on_the_one_alerts_topic" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
  }

  assert {
    condition = (
      aws_iam_role_policy.grafana_alerts[0].role == aws_iam_role.control_plane_task.name &&
      jsondecode(aws_iam_role_policy.grafana_alerts[0].policy).Statement == [
        {
          Sid      = "PublishGrafanaAlerts"
          Effect   = "Allow"
          Action   = ["sns:Publish", "sns:GetTopicAttributes"]
          Resource = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
        }
      ]
    )
    error_message = "Grafana's contact point publishes with the control plane role: sns:Publish and sns:GetTopicAttributes on the one alerts topic, no subscribe, create or change action and no other topic."
  }
}

run "the_host_state_exporter_reads_the_cluster_and_nothing_else_when_the_alerts_topic_is_set" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"
  }

  assert {
    condition = (
      aws_iam_role_policy.host_state_read[0].role == aws_iam_role.control_plane_task.name &&
      jsondecode(aws_iam_role_policy.host_state_read[0].policy).Statement == [
        {
          Sid       = "ListClusterTasks"
          Effect    = "Allow"
          Action    = "ecs:ListTasks"
          Resource  = "*"
          Condition = { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } }
        },
        {
          Sid      = "ListClusterContainerInstances"
          Effect   = "Allow"
          Action   = "ecs:ListContainerInstances"
          Resource = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev"
        },
        {
          Sid       = "DescribeClusterContainerInstances"
          Effect    = "Allow"
          Action    = "ecs:DescribeContainerInstances"
          Resource  = "arn:aws:ecs:eu-west-1:111111111111:container-instance/example_project_cluster_dev/*"
          Condition = { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } }
        },
        {
          Sid      = "ListClusterServices"
          Effect   = "Allow"
          Action   = "ecs:ListServices"
          Resource = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev"
        },
        {
          Sid       = "DescribeClusterServices"
          Effect    = "Allow"
          Action    = "ecs:DescribeServices"
          Resource  = "arn:aws:ecs:eu-west-1:111111111111:service/example_project_cluster_dev/*"
          Condition = { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } }
        },
        {
          Sid       = "DescribeClusterTasks"
          Effect    = "Allow"
          Action    = "ecs:DescribeTasks"
          Resource  = "arn:aws:ecs:eu-west-1:111111111111:task/example_project_cluster_dev/*"
          Condition = { ArnEquals = { "ecs:cluster" = "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev" } }
        },
      ]
    )
    error_message = "The host state exporter lists and describes the tasks, services and container instances of the one cluster. It cannot run, stop or change a task or a service."
  }
}

run "without_the_alerts_topic_the_control_plane_role_gets_no_host_read" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.host_state_read) == 0
    error_message = "The host read follows the same switch as the alert grant."
  }
}

locals {
  lakehouse_read_prefixes  = ["lakehouse/bronze/", "lakehouse/bronze_dictionary/", "lakehouse/silver/", "lakehouse/gold/"]
  lakehouse_write_prefixes = ["lakehouse/silver/", "lakehouse/silver_e2e/", "lakehouse/gold/", "lakehouse/gold_e2e/", "lakehouse/catalog_backups/"]
  role_policy_inputs = {
    AWS_ACCOUNT_ID   = var.expected_account_id
    PROJECT_USER     = var.aws_profile
    DATA_BUCKET_NAME = var.data_bucket_name
    READ_PREFIXES    = local.lakehouse_read_prefixes
    WRITE_PREFIXES   = local.lakehouse_write_prefixes
  }

  # The policies in owner_policies/ are JSON files rendered with these values. The task execution policy names the nine
  # control-plane parameters the task definitions inject (Systems Manager Parameter Store SecureString parameters); each name is
  # the variable of that secret, the same default the project user's policy files and the secret store read. The Airflow
  # administrator password is not injected (the Simple auth manager runs with all users as administrators), so it has no
  # grant. The stack holds no parameter value. The same files are evaluated by scripts/infrastructure/run_policy_e2e.py.
  owner_policy_values = {
    AWS_REGION                  = var.aws_region
    AWS_ACCOUNT_ID              = var.expected_account_id
    PROJECT_NAME                = var.project_name
    ENVIRONMENT                 = var.environment
    POSTGRES_PASSWORD_NAME      = var.postgres_password_name
    AIRFLOW_DB_PASSWORD_NAME    = var.airflow_db_password_name
    MARQUEZ_DB_PASSWORD_NAME    = var.marquez_db_password_name
    AIRFLOW_FERNET_KEY_NAME     = var.airflow_fernet_key_name
    AIRFLOW_JWT_SECRET_NAME     = var.airflow_jwt_secret_name
    AIRFLOW_API_SECRET_KEY_NAME = var.airflow_api_secret_key_name
    REDIS_PASSWORD_NAME         = var.redis_password_name
    POLARIS_ROOT_SECRET_NAME    = var.polaris_root_secret_name
    GRAFANA_ADMIN_PASSWORD_NAME = var.grafana_admin_password_name
  }

  # The platform stack builds these names and the Project tag the same way; both stacks' tests pin them.
  platform_project_tag = var.project_name
  platform_name        = "${var.project_name}_%s_${var.environment}"
  cluster_name         = format(local.platform_name, "cluster")

  ecs_arn = "arn:aws:ecs:${var.aws_region}:${var.expected_account_id}"
  ec2_arn = "arn:aws:ec2:${var.aws_region}:${var.expected_account_id}"

  ecs_tasks_trust = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = var.expected_account_id }
        ArnLike      = { "aws:SourceArn" = "${local.ecs_arn}:*" }
      }
    }]
  })

  host_tags = { "aws:ResourceTag/Name" = format(local.platform_name, "host"), "aws:ResourceTag/Project" = local.platform_project_tag }
  data_volume_tags = {
    "aws:ResourceTag/Name" = format(local.platform_name, "host_data"), "aws:ResourceTag/Project" = local.platform_project_tag
  }

  # The services the platform adds, as the Cost Explorer SERVICE dimension names them. S3 and Secrets Manager stay out.
  budget_services = [
    "Amazon Elastic Compute Cloud - Compute", "EC2 - Other", "Amazon Virtual Private Cloud", "Amazon Elastic Container Service",
    "Amazon Elastic Load Balancing", "AmazonCloudWatch", "Amazon EC2 Container Registry (ECR)"
  ]
}

resource "aws_iam_role" "lakehouse_reader" {
  name                 = var.lakehouse_reader_role_name
  description          = "Read-only access to the lakehouse tables for pipeline read jobs; assumed for one job by the project user."
  max_session_duration = var.lakehouse_session_max_seconds
  assume_role_policy   = templatefile("${path.module}/role_policies/trust.json", local.role_policy_inputs)

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role_policy" "lakehouse_reader" {
  name   = "${var.lakehouse_reader_role_name}_access"
  role   = aws_iam_role.lakehouse_reader.name
  policy = templatefile("${path.module}/role_policies/reader.json", local.role_policy_inputs)
}

resource "aws_iam_role" "lakehouse_writer" {
  name                 = var.lakehouse_writer_role_name
  description          = "Write access to the silver, gold and catalog backup prefixes for pipeline write jobs; assumed for one job by the project user."
  max_session_duration = var.lakehouse_session_max_seconds
  assume_role_policy   = templatefile("${path.module}/role_policies/trust.json", local.role_policy_inputs)

  lifecycle {
    prevent_destroy = true

    precondition {
      condition     = var.lakehouse_reader_role_name != var.lakehouse_writer_role_name
      error_message = "The reader and writer roles must have different names."
    }
  }
}

resource "aws_iam_role_policy" "lakehouse_writer" {
  name   = "${var.lakehouse_writer_role_name}_access"
  role   = aws_iam_role.lakehouse_writer.name
  policy = templatefile("${path.module}/role_policies/writer.json", local.role_policy_inputs)
}

# The task execution role injects the control plane's parameters with ssm:GetParameters on exactly those parameter ARNs.
# The ECS documentation requires kms:Decrypt only for a customer-managed key; these SecureString parameters use the
# AWS-managed key aws/ssm, so the role holds no KMS permission and no Secrets Manager permission.
# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html#task-execution-secrets
# The host role holds only what the ECS agent and the boot script call. Every task definition names the execution role,
# so image pulls and awslogs use that role instead (the boot script turns on the agent's awslogs override). A Resource of
# "*" stays only where the Service Authorization Reference lists no resource type for the action.
resource "aws_iam_role" "ecs_host" {
  name        = format(local.platform_name, "ecs_host")
  description = "Container host: ECS agent for the project cluster, Session Manager and boot-time attachment of its data volume and Elastic IP."
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_instance_profile" "ecs_host" {
  name = aws_iam_role.ecs_host.name
  role = aws_iam_role.ecs_host.name
}

resource "aws_iam_role_policy" "ecs_host" {
  name = "container_host"
  role = aws_iam_role.ecs_host.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "JoinProjectCluster"
        Effect = "Allow"
        Action = [
          "ecs:DeregisterContainerInstance", "ecs:RegisterContainerInstance", "ecs:SubmitAttachmentStateChanges",
          "ecs:SubmitContainerStateChange", "ecs:SubmitTaskStateChange"
        ]
        Resource = "${local.ecs_arn}:cluster/${local.cluster_name}"
      },
      {
        Sid      = "AgentSessions"
        Effect   = "Allow"
        Action   = ["ecs:Poll", "ecs:StartTelemetrySession"]
        Resource = "${local.ecs_arn}:container-instance/${local.cluster_name}/*"
      },
      {
        Sid       = "FindPollEndpoint"
        Effect    = "Allow"
        Action    = "ecs:DiscoverPollEndpoint"
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestedRegion" = var.aws_region } }
      },
      {
        Sid       = "RegisterWithSessionManager"
        Effect    = "Allow"
        Action    = "ssm:UpdateInstanceInformation"
        Resource  = "${local.ec2_arn}:instance/*"
        Condition = { StringEquals = { "ssm:resourceTag/Name" = format(local.platform_name, "host"), "ssm:resourceTag/Project" = local.platform_project_tag } }
      },
      {
        Sid       = "SessionManagerChannels"
        Effect    = "Allow"
        Action    = ["ssmmessages:CreateControlChannel", "ssmmessages:CreateDataChannel", "ssmmessages:OpenControlChannel", "ssmmessages:OpenDataChannel"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestedRegion" = var.aws_region } }
      },
      {
        Sid       = "FindOwnDataVolumeAndAddress"
        Effect    = "Allow"
        Action    = ["ec2:DescribeAddresses", "ec2:DescribeVolumes"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestedRegion" = var.aws_region } }
      },
      {
        Sid       = "AssociateOwnElasticIp"
        Effect    = "Allow"
        Action    = "ec2:AssociateAddress"
        Resource  = ["${local.ec2_arn}:elastic-ip/*", "${local.ec2_arn}:instance/*", "${local.ec2_arn}:network-interface/*"]
        Condition = { StringEquals = local.host_tags }
      },
      {
        Sid       = "AttachDataVolume"
        Effect    = "Allow"
        Action    = "ec2:AttachVolume"
        Resource  = "${local.ec2_arn}:volume/*"
        Condition = { StringEquals = local.data_volume_tags }
      },
      {
        Sid       = "AttachToOwnInstance"
        Effect    = "Allow"
        Action    = "ec2:AttachVolume"
        Resource  = "${local.ec2_arn}:instance/*"
        Condition = { StringEquals = local.host_tags }
      }
    ]
  })
}

resource "aws_iam_role" "task_execution" {
  name               = format(local.platform_name, "task_execution")
  description        = "ECS task execution: pulls project images, writes project logs and injects the control plane's named parameters."
  assume_role_policy = local.ecs_tasks_trust
}

resource "aws_iam_role_policy" "task_execution" {
  name   = "task_execution"
  role   = aws_iam_role.task_execution.id
  policy = templatefile("${path.module}/owner_policies/task_execution.json", local.owner_policy_values)
}

# The Grafana password is injected only while monitoring is on, so the IAM stack is unchanged while it is off.
resource "aws_iam_role_policy" "task_execution_monitoring" {
  count = var.monitoring_enabled ? 1 : 0

  name   = "task_execution_monitoring"
  role   = aws_iam_role.task_execution.id
  policy = templatefile("${path.module}/owner_policies/task_execution_monitoring.json", local.owner_policy_values)
}

# The model API and the page run on the host and read six parameters through the execution role: the key hashes, the serving writer pair, the MLflow pair and the page
# key. The base policy stays as it was, and the grant exists only while the model API is prepared.
resource "aws_iam_role_policy" "task_execution_model_api" {
  count = var.model_api_prepared ? 1 : 0

  name = "task_execution_model_api"
  role = aws_iam_role.task_execution.id
  policy = templatefile("${path.module}/owner_policies/task_execution_model_api.json", merge(local.owner_policy_values, {
    MODEL_API_KEY_HASHES_NAME              = var.model_api_key_hashes_name
    AWS_POLARIS_SERVING_CLIENT_ID_NAME     = var.aws_polaris_serving_client_id_name
    AWS_POLARIS_SERVING_CLIENT_SECRET_NAME = var.aws_polaris_serving_client_secret_name
    MODEL_API_MLFLOW_USER_NAME             = var.model_api_mlflow_user_name
    MODEL_API_MLFLOW_CREDENTIAL_NAME       = var.model_api_mlflow_credential_name
    STREAMLIT_MODEL_API_KEY_NAME           = var.streamlit_model_api_key_name
  }))
}

resource "aws_iam_role" "polaris_task" {
  name               = format(local.platform_name, "polaris_task")
  description        = "Polaris catalog service: Iceberg files under lakehouse/ only."
  assume_role_policy = local.ecs_tasks_trust
}

resource "aws_iam_role_policy" "polaris_task" {
  name = "polaris_task"
  role = aws_iam_role.polaris_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ManageLakehouseTables"
        Effect   = "Allow"
        Action   = ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"]
        Resource = "arn:aws:s3:::${var.data_bucket_name}/lakehouse/*"
      },
      {
        Sid      = "ListLakehouseFolder"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::${var.data_bucket_name}"
        Condition = {
          StringLike = { "s3:prefix" = ["lakehouse/", "lakehouse/*"] }
        }
      }
    ]
  })
}

# Postgres, Redis, Airflow and Marquez need no AWS access of their own: the role has no policy.
resource "aws_iam_role" "control_plane_task" {
  name               = format(local.platform_name, "control_plane_task")
  description        = "Control-plane services other than Polaris: no AWS permissions."
  assume_role_policy = local.ecs_tasks_trust
}

resource "aws_iam_role" "snapshots" {
  name        = format(local.platform_name, "snapshots")
  description = "Data Lifecycle Manager: daily snapshots of the container host data volume."
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "dlm.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Data Lifecycle Manager snapshots the data volume, tags each snapshot only in the creating call and deletes only
# snapshots that its own schedule made: the aws:dlm tags are reserved, so no principal can add them to another snapshot.
# Every new snapshot must carry the exact key Project with the project value in the request, whatever the schedule's tag
# copying says, so a snapshot never escapes the budget filter: the schedule adds the tag itself, and an untagged or
# differently spelled create fails instead of costing money unseen. Key matching in a request-tag condition ignores
# case, so the policy also requires the exact key in aws:TagKeys. Three denies hold even if a later edit weakens an allow.
resource "aws_iam_role_policy" "snapshots" {
  name   = "snapshots"
  role   = aws_iam_role.snapshots.id
  policy = templatefile("${path.module}/owner_policies/snapshots.json", local.owner_policy_values)
}

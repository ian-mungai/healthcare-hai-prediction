variable "training_prepared" {
  description = "Prepare the training roles, the training security group and the MLflow rules (decision 56). The roles hold no lakehouse write. The MLflow role reads and writes the mlflow/ prefix only."
  type        = bool
  default     = false
  nullable    = false
}

locals {
  training_policy_documents = {
    for name in ["job_dvc_baseline", "job_features_read", "mlflow_artifacts"] :
    name => templatefile("${path.module}/owner_policies/${name}.json", local.job_policy_inputs)
  }
  training_policies = var.training_prepared ? {
    stage_read    = local.job_policy_documents.job_stage_read
    features_read = local.training_policy_documents.job_features_read
    dvc_baseline  = local.training_policy_documents.job_dvc_baseline
  } : {}
  training_endpoints = var.training_prepared && var.job_endpoints_enabled
  mlflow_parameter_values = merge(local.owner_policy_values, {
    MLFLOW_DB_PASSWORD_NAME    = var.mlflow_db_password_name
    MLFLOW_ADMIN_PASSWORD_NAME = var.mlflow_admin_password_name
  })
}

resource "aws_iam_role" "train_task" {
  count = var.training_prepared ? 1 : 0

  name                 = format(local.platform_name, "job_train_task")
  description          = "Training job: stage reads, the feature tables, the Feast registry and the DVC remote. No lakehouse write and no mlflow/ access."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "train_policy" {
  for_each = local.training_policies

  name   = each.key
  role   = aws_iam_role.train_task[0].name
  policy = each.value
}

resource "aws_iam_role" "mlflow_task" {
  count = var.training_prepared ? 1 : 0

  name                 = format(local.platform_name, "mlflow_task")
  description          = "MLflow tracking service: read and write on the mlflow/ prefix of the data bucket only."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "mlflow_artifacts" {
  count = var.training_prepared ? 1 : 0

  name   = "mlflow_artifacts"
  role   = aws_iam_role.mlflow_task[0].name
  policy = local.training_policy_documents.mlflow_artifacts
}

# The shared job execution policy names three image repositories and the host execution policy names eight parameters. Both stay unchanged. The new tasks
# add exactly their own grant here, and only while the flag is true.
resource "aws_iam_role_policy" "train_image_pull" {
  count = var.training_prepared ? 1 : 0

  name   = "train_images"
  role   = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/job_train_images.json", local.job_policy_inputs)
}

resource "aws_iam_role_policy" "train_parameter" {
  count = var.training_prepared ? 1 : 0

  name   = "train_parameter"
  role   = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/job_train_parameter.json", local.mlflow_parameter_values)
}

resource "aws_iam_role_policy" "mlflow_parameter" {
  count = var.training_prepared ? 1 : 0

  name   = "mlflow_parameter"
  role   = aws_iam_role.task_execution.name
  policy = templatefile("${path.module}/owner_policies/mlflow_parameter.json", local.mlflow_parameter_values)
}

resource "aws_security_group" "training" {
  #checkov:skip=CKV2_AWS_5:The training run task attaches this group when it starts.
  count = var.training_prepared ? 1 : 0

  name        = format(local.platform_name, "job_training")
  description = "Closed private training jobs: S3, image and log endpoints, Parameter Store, the catalog and MLflow."
  vpc_id      = aws_vpc.platform.id

  tags = merge(local.project_tags, { Name = format(local.platform_name, "job_training") })
}

resource "aws_vpc_security_group_egress_rule" "training_s3" {
  count = local.training_endpoints ? 1 : 0

  security_group_id = aws_security_group.training[0].id
  description       = "Training HTTPS to S3 through its gateway endpoint"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  prefix_list_id    = aws_vpc_endpoint.job_s3[0].prefix_list_id
}

resource "aws_vpc_security_group_egress_rule" "training_endpoints" {
  count = local.training_endpoints ? 1 : 0

  security_group_id            = aws_security_group.training[0].id
  description                  = "Training HTTPS to ECR and Logs endpoints"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.job["endpoints"].id
}

resource "aws_vpc_security_group_ingress_rule" "training_endpoints" {
  count = local.training_endpoints ? 1 : 0

  security_group_id            = aws_security_group.job["endpoints"].id
  description                  = "ECR and Logs HTTPS from training jobs"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.training[0].id
}

resource "aws_vpc_security_group_egress_rule" "training_ssm" {
  count = local.training_endpoints ? 1 : 0

  security_group_id            = aws_security_group.training[0].id
  description                  = "Training HTTPS to Parameter Store"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.job["ssm_endpoint"].id
}

resource "aws_vpc_security_group_ingress_rule" "training_ssm" {
  count = local.training_endpoints ? 1 : 0

  security_group_id            = aws_security_group.job["ssm_endpoint"].id
  description                  = "SSM HTTPS from training jobs"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.training[0].id
}

resource "aws_vpc_security_group_egress_rule" "training_catalog" {
  count = var.training_prepared ? 1 : 0

  security_group_id            = aws_security_group.training[0].id
  description                  = "Training jobs to the private catalog listener"
  ip_protocol                  = "tcp"
  from_port                    = 8181
  to_port                      = 8181
  referenced_security_group_id = aws_security_group.host.id
}

resource "aws_vpc_security_group_ingress_rule" "host_from_training_catalog" {
  count = var.training_prepared ? 1 : 0

  security_group_id            = aws_security_group.host.id
  description                  = "Private catalog listener from training jobs"
  ip_protocol                  = "tcp"
  from_port                    = 8181
  to_port                      = 8181
  referenced_security_group_id = aws_security_group.training[0].id
}

resource "aws_vpc_security_group_egress_rule" "training_mlflow" {
  count = var.training_prepared ? 1 : 0

  security_group_id            = aws_security_group.training[0].id
  description                  = "Training jobs to the MLflow listener"
  ip_protocol                  = "tcp"
  from_port                    = 5100
  to_port                      = 5100
  referenced_security_group_id = aws_security_group.host.id
}

resource "aws_vpc_security_group_ingress_rule" "host_from_training_mlflow" {
  count = var.training_prepared ? 1 : 0

  security_group_id            = aws_security_group.host.id
  description                  = "MLflow listener from training jobs only"
  ip_protocol                  = "tcp"
  from_port                    = 5100
  to_port                      = 5100
  referenced_security_group_id = aws_security_group.training[0].id
}

output "training_roles" {
  description = "Prepared training role names, ARNs and unique IDs. Empty until training_prepared is true. The MLflow service takes mlflow_task as mlflow_task_role_arn."
  value = merge(
    { for role in aws_iam_role.train_task : "train_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } },
    { for role in aws_iam_role.mlflow_task : "mlflow_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
  )
}

output "training_network" {
  description = "The training job subnet and security group, or null until training_prepared is true. Application definitions must disable public IPs."
  value = var.training_prepared ? {
    subnet_id         = aws_subnet.public[0].id
    security_group_id = aws_security_group.training[0].id
    endpoints_enabled = var.job_endpoints_enabled
  } : null
}

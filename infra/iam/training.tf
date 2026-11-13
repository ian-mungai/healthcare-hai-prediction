locals {
  training_policy_documents = {
    for name in ["job_dvc_baseline", "job_features_read", "mlflow_artifacts", "promote_receipts", "research_records"] :
    name => templatefile("${path.module}/owner_policies/${name}.json", local.job_policy_inputs)
  }
  # The promote task also publishes the owner notice to the one alerts topic, once the stack is told the topic.
  training_owner_notice = var.alerts_topic_arn == null ? {} : { owner_notice = templatefile("${path.module}/owner_policies/owner_notice.json", { ALERTS_TOPIC_ARN = var.alerts_topic_arn }) }
  training_policies = var.training_prepared ? merge({
    stage_read       = local.job_policy_documents.job_stage_read
    features_read    = local.training_policy_documents.job_features_read
    dvc_baseline     = local.training_policy_documents.job_dvc_baseline
    promote_receipts = local.training_policy_documents.promote_receipts
    research_records = local.training_policy_documents.research_records
  }, local.training_owner_notice) : {}
  mlflow_parameter_values = merge(local.owner_policy_values, {
    MLFLOW_DB_PASSWORD_NAME    = var.mlflow_db_password_name
    MLFLOW_ADMIN_PASSWORD_NAME = var.mlflow_admin_password_name
  })
}

resource "aws_iam_role" "train_task" {
  count = var.training_prepared ? 1 : 0

  name                 = format(local.platform_name, "job_train_task")
  description          = "Training and promotion jobs: stage reads, the feature tables, the Feast registry, the DVC remote, the create-only promotion receipts under promote/ and research records under research/, and publishing to the one alerts topic. No lakehouse write and no mlflow/ access."
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

output "training_roles" {
  description = "Prepared training role names, ARNs and unique IDs. Empty until training_prepared is true. The MLflow service takes mlflow_task as mlflow_task_role_arn."
  value = merge(
    { for role in aws_iam_role.train_task : "train_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } },
    { for role in aws_iam_role.mlflow_task : "mlflow_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
  )
}

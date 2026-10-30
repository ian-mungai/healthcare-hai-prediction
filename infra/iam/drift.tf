variable "drift_prepared" {
  description = "Prepare the drift task role and the grants it needs (decision 21). Off by default. It reuses the training network and the endpoints of the jobs, so apply it with training_prepared and job_endpoints_enabled."
  type        = bool
  default     = false
  nullable    = false
}

variable "drift_mlflow_admin_password_name" {
  description = "Parameter Store SecureString name of the MLflow administrator password that the drift baseline container logs in with. The execution role may read exactly this parameter."
  type        = string
  default     = "/healthcare_hai_prediction/mlflow_admin_password"
  nullable    = false

  validation {
    condition     = !var.drift_prepared || var.drift_mlflow_admin_password_name == "/${var.project_name}/mlflow_admin_password"
    error_message = "Grant only the project MLflow administrator password parameter."
  }
}

locals {
  drift_policy_documents = {
    for name in ["job_dvc_baseline_read", "job_features_read", "drift_records", "drift_records_read", "job_drift_images"] :
    name => templatefile("${path.module}/owner_policies/${name}.json", local.job_policy_inputs)
  }
  drift_policies = var.drift_prepared ? {
    stage_read        = local.job_policy_documents.job_stage_read
    features_read     = local.drift_policy_documents.job_features_read
    dvc_baseline_read = local.drift_policy_documents.job_dvc_baseline_read
    drift_records     = local.drift_policy_documents.drift_records
  } : {}
}

resource "aws_iam_role" "drift_task" {
  count = var.drift_prepared ? 1 : 0

  name                 = format(local.platform_name, "job_drift_task")
  description          = "Drift job: stage reads, the feature tables, the Feast registry and a read of the DVC remote. It creates objects under drift/ and has no lakehouse write."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "drift_task" {
  for_each = local.drift_policies

  name   = each.key
  role   = aws_iam_role.drift_task[0].name
  policy = each.value
}

# The shared job execution policy names three image repositories and no MLflow parameter. It stays unchanged. The drift task adds exactly its own grants here,
# and only while the flag is true.
resource "aws_iam_role_policy" "drift_image_pull" {
  count = var.drift_prepared ? 1 : 0

  name   = "drift_images"
  role   = aws_iam_role.job["execution"].name
  policy = local.drift_policy_documents.job_drift_images
}

resource "aws_iam_role_policy" "drift_parameter" {
  count = var.drift_prepared ? 1 : 0

  name   = "drift_parameter"
  role   = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/job_drift_parameter.json", merge(local.job_policy_inputs, { DRIFT_MLFLOW_PASSWORD_NAME = var.drift_mlflow_admin_password_name }))
}

# Airflow runs as the control plane role, which has no AWS permissions. The drift DAG reads the records that the drift task published under drift/.
resource "aws_iam_role_policy" "drift_records_read" {
  count = var.drift_prepared ? 1 : 0

  name   = "drift_records_read"
  role   = aws_iam_role.control_plane_task.name
  policy = local.drift_policy_documents.drift_records_read
}

output "drift_roles" {
  description = "The prepared drift role name, ARN and unique ID. Empty until drift_prepared is true. The jobs root takes drift_task as the drift_roles entry of the same name."
  value       = { for role in aws_iam_role.drift_task : "drift_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
}

# The dedicated execution role of the serving task definitions (owner decision D18: GitHub registers the serving revisions). ECS pulls the image, writes
# the log and injects the parameters with this role, not with the shared task execution role, which reads the PostgreSQL and Polaris root secrets. So a
# revision that GitHub registers can name this role and reach none of the control-plane secrets. Only ECS tasks of this account can assume it. It exists
# while the model API is prepared, like the serving task roles that the deploy role may pass beside it.
resource "aws_iam_role" "serving_execution" {
  count = var.model_api_prepared ? 1 : 0

  name                 = format(local.platform_name, "serving_execution")
  description          = "ECS task execution for the model API and the dashboard: pulls their two images, writes their log streams and injects their seven parameters. Reads no control-plane secret."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "serving_execution" {
  count = var.model_api_prepared ? 1 : 0

  name = "serving_execution"
  role = aws_iam_role.serving_execution[0].id
  policy = templatefile("${path.module}/owner_policies/serving_execution.json", merge(local.owner_policy_values, {
    MODEL_API_KEY_HASHES_NAME              = var.model_api_key_hashes_name
    AWS_POLARIS_SERVING_CLIENT_ID_NAME     = var.aws_polaris_serving_client_id_name
    AWS_POLARIS_SERVING_CLIENT_SECRET_NAME = var.aws_polaris_serving_client_secret_name
    MODEL_API_MLFLOW_USER_NAME             = var.model_api_mlflow_user_name
    MODEL_API_MLFLOW_CREDENTIAL_NAME       = var.model_api_mlflow_credential_name
    MODEL_API_REDIS_PASSWORD_NAME          = var.model_api_redis_password_name
    STREAMLIT_MODEL_API_KEY_NAME           = var.streamlit_model_api_key_name
  }))
}

output "serving_execution_role" {
  description = "The serving execution role name and ARN. Empty until model_api_prepared is true. The services stack names it as the execution role of the model API and dashboard task definitions."
  value       = one([for role in aws_iam_role.serving_execution : { name = role.name, arn = role.arn }])
}

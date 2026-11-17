variable "mlflow_user_task_role_arn" {
  description = "IAM output restore_after_teardown_roles.mlflow_user: the task role of the MLflow user one-shot. It may only read the MLflow administrator password and the model API MLflow login. Null plans no one-shot. Needs mlflow_enabled."
  type        = string
  default     = null

  validation {
    condition     = var.mlflow_user_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/[a-z][a-z0-9_]+_mlflow_user_task_(dev|prod)$", coalesce(var.mlflow_user_task_role_arn, "none")))
    error_message = "Supply the mlflow_user_task IAM role ARN, or leave mlflow_user_task_role_arn null."
  }

  validation {
    condition     = var.mlflow_user_task_role_arn == null || var.mlflow_enabled
    error_message = "The MLflow user one-shot needs mlflow_enabled."
  }
}

locals {
  mlflow_user_on = var.mlflow_user_task_role_arn != null && var.mlflow_enabled

  # host_session start runs this task once the services are stable. It reads its three parameters with the task role (no secret is injected), logs in to the MLflow server
  # on the host as the administrator and makes sure the model API's user exists with its stored login and READ on the served models. It runs the reviewed script
  # scripts/mlflow_user.py as the command.
  mlflow_user_task = {
    network_mode  = "host"
    task_role_arn = var.mlflow_user_task_role_arn
    volumes       = {}
    containers = [
      merge(local.container_defaults, {
        name                   = "mlflow_user"
        image                  = lookup(local.image, "mlflow", "")
        user                   = "10005:10005"
        readonlyRootFilesystem = true
        memory                 = 256
        memoryReservation      = 64
        stopTimeout            = 30
        entryPoint             = ["python", "-I", "-c"]
        command                = [file("${path.module}/scripts/mlflow_user.py")]
        environment = [
          { name = "ADMIN_PASSWORD_PARAMETER", value = var.mlflow_admin_password_name },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "AWS_REGION", value = var.aws_region },
          { name = "CREDENTIAL_PARAMETER", value = var.model_api_mlflow_credential_name },
          { name = "HOME", value = "/tmp" },
          { name = "MLFLOW_ADMIN_USERNAME", value = "admin" },
          { name = "MLFLOW_URL", value = "http://127.0.0.1:${local.mlflow_port}" },
          { name = "MODEL_API_HAI_TYPES", value = var.model_api_hai_types },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "PYTHONUNBUFFERED", value = "1" },
          { name = "USER_PARAMETER", value = var.model_api_mlflow_user_name },
        ]
        linuxParameters = {
          capabilities = { add = [], drop = ["ALL"] }
          tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
        }
        restartPolicy    = { enabled = false }
        logConfiguration = lookup(local.log_configuration, "mlflow_user", null)
      }),
    ]
  }
}

variable "restore_after_teardown_prepared" {
  description = "Prepare the three task roles of the restore after a teardown: the Postgres backup service (creates objects under pg_backups/ once), the postgres task's restore (lists and reads pg_backups/) and the MLflow user step (reads three parameters). Off by default."
  type        = bool
  default     = false
  nullable    = false
}

variable "code_run_record_parameter_name" {
  description = "Parameter Store name of the code run record: the run ID and digest of the code that host_session placed on the host, kept outside the data volume. The project user reads and writes it (it is under the project prefix, so the secret store policy names it)."
  type        = string
  default     = "/healthcare_hai_prediction/code_run_record"
  nullable    = false

  validation {
    condition     = can(regex("^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$", var.code_run_record_parameter_name)) && !can(regex("^/(?i:aws|ssm)", var.code_run_record_parameter_name))
    error_message = "Use a parameter path that starts with a slash and has letters, digits and underscores only."
  }
}

locals {
  restore_after_teardown_statements = {
    # The backup service creates each dump and each marker once: the condition makes S3 refuse a key that exists. It has no read, no listing and no delete.
    postgres_backup = {
      description = "Postgres backup service: creates objects under pg_backups/ once per key and does nothing else."
      statements = [{
        Sid       = "CreateBackupObjectsOnce"
        Effect    = "Allow"
        Action    = ["s3:PutObject"]
        Resource  = ["arn:aws:s3:::${var.data_bucket_name}/pg_backups/*"]
        Condition = { StringEquals = { "s3:if-none-match" = "*" } }
      }]
    }
    # The postgres task reads the newest completed set at its start. It has no write.
    postgres_restore = {
      description = "Postgres task: lists and reads pg_backups/ and nothing else."
      statements = [
        {
          Sid      = "ReadBackupObjects"
          Effect   = "Allow"
          Action   = ["s3:GetObject"]
          Resource = ["arn:aws:s3:::${var.data_bucket_name}/pg_backups/*"]
        },
        {
          Sid       = "ListBackupObjects"
          Effect    = "Allow"
          Action    = ["s3:ListBucket"]
          Resource  = ["arn:aws:s3:::${var.data_bucket_name}"]
          Condition = { StringLike = { "s3:prefix" = ["pg_backups/*"] } }
        },
      ]
    }
    # The MLflow user step logs in as the administrator and creates or checks the model API's user. It reads exactly these three parameters.
    mlflow_user = {
      description = "MLflow user step: reads the MLflow administrator password and the model API MLflow login and nothing else."
      statements = [{
        Sid    = "ReadMlflowUserParameters"
        Effect = "Allow"
        Action = ["ssm:GetParameter"]
        Resource = [
          "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.mlflow_admin_password_name}",
          "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.model_api_mlflow_user_name}",
          "arn:aws:ssm:${var.aws_region}:${var.expected_account_id}:parameter${var.model_api_mlflow_credential_name}",
        ]
      }]
    }
  }
  restore_after_teardown_roles = { for key, value in local.restore_after_teardown_statements : key => value if var.restore_after_teardown_prepared }
}

resource "aws_iam_role" "restore_after_teardown" {
  for_each = local.restore_after_teardown_roles

  name                 = format(local.platform_name, "${each.key}_task")
  description          = each.value.description
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "restore_after_teardown" {
  for_each = local.restore_after_teardown_roles

  name = each.key
  role = aws_iam_role.restore_after_teardown[each.key].name
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = each.value.statements
  })
}

output "restore_after_teardown_roles" {
  description = "The task role name and ARN of postgres_backup, postgres_restore and mlflow_user. Empty until restore_after_teardown_prepared is true. The services root takes the ARNs as postgres_backup_task_role_arn, postgres_restore_task_role_arn and mlflow_user_task_role_arn."
  value       = { for key, role in aws_iam_role.restore_after_teardown : key => { name = role.name, arn = role.arn } }
}

# The project user's policy files read this name from the variable default (scripts/infrastructure/secret_store.py), so no resource here uses it. The check reads it so
# an unused declaration is not removed by mistake.
check "code_run_record_parameter_is_under_the_project_prefix" {
  assert {
    condition     = startswith(var.code_run_record_parameter_name, "/healthcare_hai_prediction/") || startswith(var.code_run_record_parameter_name, "/${var.project_name}/")
    error_message = "The code run record parameter belongs under the project prefix, where the project user's policy names it."
  }
}

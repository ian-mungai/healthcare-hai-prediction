variable "catalog_setup_task_role_arn" {
  description = "IAM output host_catalog_setup_role: the task role of the catalog setup one-shot. It reads and writes only the fourteen AWS Polaris pair parameters, the setup's own S3 records and the services apply lock, and lists the project's job tasks. Null plans no catalog setup one-shot, and host_session catalog-setup then refuses."
  type        = string
  default     = null

  validation {
    condition     = var.catalog_setup_task_role_arn == null || can(regex("^arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@-]{1,64}$", coalesce(var.catalog_setup_task_role_arn, "none")))
    error_message = "Supply an IAM role ARN for the catalog setup one-shot, or leave catalog_setup_task_role_arn null."
  }

  validation {
    condition     = var.catalog_setup_task_role_arn == null || var.code_bucket_name != null
    error_message = "The catalog setup one-shot keeps its S3 records in the data bucket: it needs code_bucket_name."
  }

  validation {
    condition = var.catalog_setup_task_role_arn == null || !contains(
      [var.control_plane_task_role_arn, var.polaris_task_role_arn, var.mlflow_task_role_arn, var.openmetadata_task_role_arn, var.code_fetch_task_role_arn], var.catalog_setup_task_role_arn
    )
    error_message = "The catalog setup one-shot must have its own task role, so no service holds its pair grants and it holds none of theirs."
  }
}

locals {
  catalog_setup_on    = var.catalog_setup_task_role_arn != null
  catalog_setup_mount = "/opt/hai/repo"

  # The catalog setup runs the tracked module scripts.lakehouse.aws_catalog_setup from the checkout that host_session places as /data/repo, in the Airflow image
  # (boto3 and the Python standard library are all it imports; a Terraform-independent E2E case checks that). It uses host networking, so Polaris is its own
  # 127.0.0.1:8181. The execution role injects the Polaris root secret as POLARIS_ROOT_SECRET, as for polaris_bootstrap, so the task role holds no grant on it.
  # The registered command is the read-only check. host_session catalog-setup overrides the command with the step and its arguments for each launch.
  catalog_setup_task = {
    network_mode  = "host"
    task_role_arn = var.catalog_setup_task_role_arn
    volumes       = { code_repo = local.code_path }
    containers = [
      merge(local.container_defaults, {
        name                   = "catalog_setup"
        image                  = local.image["airflow"]
        user                   = "50000:0"
        readonlyRootFilesystem = true
        memory                 = 512
        memoryReservation      = 256
        stopTimeout            = 30
        workingDirectory       = local.catalog_setup_mount
        entryPoint             = ["python", "-m", "scripts.lakehouse.aws_catalog_setup"]
        command                = ["check"]
        environment = [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AWS_DEFAULT_REGION", value = var.aws_region },
          { name = "CATALOG_SETUP_CLUSTER", value = var.cluster_name },
          { name = "CATALOG_SETUP_FAMILY_PREFIX", value = "${local.name_prefix}_job_" },
          { name = "CATALOG_SETUP_LOCK_PARAMETER", value = var.services_apply_lock_parameter_name },
          { name = "CATALOG_SETUP_ROLE_ARN", value = coalesce(var.catalog_setup_task_role_arn, "unset") },
          { name = "DATA_BUCKET_NAME", value = coalesce(var.code_bucket_name, "unset") },
          { name = "HOME", value = "/tmp" },
          { name = "PYTHONDONTWRITEBYTECODE", value = "1" },
          { name = "PYTHONPATH", value = local.catalog_setup_mount },
          { name = "PYTHONUNBUFFERED", value = "1" },
        ]
        secrets     = [{ name = "POLARIS_ROOT_SECRET", valueFrom = local.secret_arn["polaris_root_secret"] }]
        mountPoints = [{ sourceVolume = "code_repo", containerPath = local.catalog_setup_mount, readOnly = true }]
        linuxParameters = {
          capabilities = { add = [], drop = ["ALL"] }
          tmpfs        = [{ containerPath = "/tmp", size = 64, mountOptions = ["nosuid"] }]
        }
        restartPolicy    = { enabled = false }
        logConfiguration = lookup(local.log_configuration, "catalog_setup", null)
      }),
    ]
  }
}

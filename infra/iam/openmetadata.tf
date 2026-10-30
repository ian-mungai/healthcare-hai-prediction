variable "openmetadata_prepared" {
  description = "Prepare the role, grants and private ports of the on-demand OpenMetadata catalog task. Off by default. It reuses the job network, so apply it with job_endpoints_enabled."
  type        = bool
  default     = false
  nullable    = false
}

variable "openmetadata_marquez_reachable" {
  description = "Open port 5000 between the lakehouse job group and the container host, so the catalog task can read Marquez. Off by default. Marquez itself must also listen on the host's private address (a change in the services stack)."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition     = !var.openmetadata_marquez_reachable || var.openmetadata_prepared
    error_message = "Opening the Marquez port needs openmetadata_prepared."
  }
}

variable "openmetadata_admin_password_name" {
  description = "Parameter Store SecureString name of the catalog administrator password. The execution role may inject exactly this parameter. The owner sets the value outside Terraform."
  type        = string
  default     = "/healthcare_hai_prediction/openmetadata_admin_password"
  nullable    = false

  validation {
    condition     = !var.openmetadata_prepared || var.openmetadata_admin_password_name == "/${var.project_name}/openmetadata_admin_password"
    error_message = "Grant only the project catalog administrator password parameter."
  }
}

locals {
  openmetadata_policy_inputs = merge(local.job_policy_inputs, { OPENMETADATA_ADMIN_PASSWORD_NAME = var.openmetadata_admin_password_name })
  openmetadata_policies      = var.openmetadata_prepared ? toset(["catalog_read"]) : toset([])
}

resource "aws_iam_role" "openmetadata_task" {
  count = var.openmetadata_prepared ? 1 : 0

  name                 = format(local.platform_name, "job_openmetadata_task")
  description          = "Catalog task: reads Iceberg metadata files and dbt artifacts only. It has no data file read and no write."
  max_session_duration = 3600
  assume_role_policy   = local.ecs_tasks_trust
  tags                 = local.project_tags
}

resource "aws_iam_role_policy" "openmetadata_task" {
  for_each = local.openmetadata_policies

  name   = each.key
  role   = aws_iam_role.openmetadata_task[0].name
  policy = templatefile("${path.module}/owner_policies/openmetadata_catalog_read.json", local.openmetadata_policy_inputs)
}

# The shared job execution policy names three image repositories and no catalog parameter. It stays unchanged. The catalog task adds exactly its own grants here,
# and only while the flag is true.
resource "aws_iam_role_policy" "openmetadata_images" {
  count = var.openmetadata_prepared ? 1 : 0

  name   = "openmetadata_images"
  role   = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/openmetadata_images.json", local.openmetadata_policy_inputs)
}

resource "aws_iam_role_policy" "openmetadata_parameter" {
  count = var.openmetadata_prepared ? 1 : 0

  name   = "openmetadata_parameter"
  role   = aws_iam_role.job["execution"].name
  policy = templatefile("${path.module}/owner_policies/openmetadata_parameter.json", local.openmetadata_policy_inputs)
}

# The owner reaches the catalog UI through a Session Manager port forward on the container host to the task's private address. Only the host may open that port.
resource "aws_vpc_security_group_ingress_rule" "openmetadata_ui" {
  count = var.openmetadata_prepared ? 1 : 0

  security_group_id            = aws_security_group.job["lakehouse"].id
  description                  = "Catalog UI from the container host only"
  ip_protocol                  = "tcp"
  from_port                    = 8585
  to_port                      = 8585
  referenced_security_group_id = aws_security_group.host.id
}

resource "aws_vpc_security_group_egress_rule" "host_to_openmetadata_ui" {
  count = var.openmetadata_prepared ? 1 : 0

  security_group_id            = aws_security_group.host.id
  description                  = "Port forward from the host to the catalog UI"
  ip_protocol                  = "tcp"
  from_port                    = 8585
  to_port                      = 8585
  referenced_security_group_id = aws_security_group.job["lakehouse"].id
}

resource "aws_vpc_security_group_egress_rule" "openmetadata_marquez" {
  count = var.openmetadata_prepared && var.openmetadata_marquez_reachable ? 1 : 0

  security_group_id            = aws_security_group.job["lakehouse"].id
  description                  = "Lakehouse jobs to Marquez on the host"
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
  referenced_security_group_id = aws_security_group.host.id
}

resource "aws_vpc_security_group_ingress_rule" "host_from_openmetadata_marquez" {
  count = var.openmetadata_prepared && var.openmetadata_marquez_reachable ? 1 : 0

  security_group_id            = aws_security_group.host.id
  description                  = "Marquez from the lakehouse jobs only"
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
  referenced_security_group_id = aws_security_group.job["lakehouse"].id
}

output "openmetadata_roles" {
  description = "The prepared catalog task role name, ARN and unique ID. Empty until openmetadata_prepared is true. The jobs root takes openmetadata_task as the openmetadata_roles entry of the same name."
  value       = { for role in aws_iam_role.openmetadata_task : "openmetadata_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
}

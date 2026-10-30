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

# The owner forwards a local port to the catalog task through this project session document and no other. The port is fixed in the document and the caller chooses
# only the destination address, which must lie in the platform network. The document exists only while the catalog is prepared, and the project user's policy names
# it by ARN, so the grant does nothing while the flag is false. The general AWS remote-host document is not granted.
resource "aws_ssm_document" "catalog_forward" {
  count = var.openmetadata_prepared ? 1 : 0

  name          = format(local.platform_name, "catalog_forward")
  document_type = "Session"
  tags          = local.project_tags
  content = jsonencode({
    schemaVersion = "1.0"
    description   = "Forward a local port to the catalog UI of the on-demand catalog task. The destination port is fixed at 8585 and the destination lies in the platform network."
    sessionType   = "Port"
    parameters = {
      host = {
        type           = "String"
        description    = "Private address of the catalog task."
        allowedPattern = "^${join("\\.", slice(split(".", split("/", aws_vpc.platform.cidr_block)[0]), 0, 2))}\\.[0-9]{1,3}\\.[0-9]{1,3}$"
      }
      localPortNumber = {
        type           = "String"
        description    = "Local port to listen on."
        allowedPattern = "^[0-9]{1,5}$"
        default        = "8585"
      }
    }
    properties = {
      portNumber      = "8585"
      type            = "LocalPortForwarding"
      host            = "{{ host }}"
      localPortNumber = "{{ localPortNumber }}"
    }
  })
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

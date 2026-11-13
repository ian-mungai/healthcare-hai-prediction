locals {
  job_shared_endpoint_clients = {
    host = aws_security_group.host.id
  }
  job_ssm_endpoint_clients = {
    host = aws_security_group.host.id
  }
}

resource "aws_security_group" "job" {
  for_each = toset(["endpoints", "ssm_endpoint"])

  name        = format(local.platform_name, "job_${each.key}")
  description = "Closed private jobs: ${each.key}."
  vpc_id      = aws_vpc.platform.id

  tags = merge(local.project_tags, { Name = format(local.platform_name, "job_${each.key}") })
}

resource "aws_vpc_endpoint" "job_s3" {
  count = var.job_endpoints_enabled ? 1 : 0

  vpc_id            = aws_vpc.platform.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = merge(local.project_tags, { Name = format(local.platform_name, "job_s3") })
}

resource "aws_vpc_endpoint" "job_interface" {
  for_each = var.job_endpoints_enabled ? toset(["ecr.api", "ecr.dkr", "logs"]) : toset([])

  vpc_id              = aws_vpc.platform.id
  service_name        = "com.amazonaws.${var.aws_region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.public[0].id]
  security_group_ids  = [aws_security_group.job["endpoints"].id]

  tags = merge(local.project_tags, { Name = format(local.platform_name, "job_${replace(each.key, ".", "_")}") })
}

resource "aws_vpc_endpoint" "job_ssm" {
  count = var.job_endpoints_enabled ? 1 : 0

  vpc_id              = aws_vpc.platform.id
  service_name        = "com.amazonaws.${var.aws_region}.ssm"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.public[0].id]
  security_group_ids  = [aws_security_group.job["ssm_endpoint"].id]

  tags = merge(local.project_tags, { Name = format(local.platform_name, "job_ssm") })
}

resource "aws_vpc_security_group_ingress_rule" "job_shared_endpoints" {
  for_each = var.job_endpoints_enabled ? local.job_shared_endpoint_clients : {}

  security_group_id            = aws_security_group.job["endpoints"].id
  description                  = "ECR and Logs HTTPS from private jobs and foundation clients"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = each.value
}

resource "aws_vpc_security_group_ingress_rule" "job_ssm" {
  for_each = var.job_endpoints_enabled ? local.job_ssm_endpoint_clients : {}

  security_group_id            = aws_security_group.job["ssm_endpoint"].id
  description                  = "SSM HTTPS from lakehouse jobs and the container host"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = each.value
}


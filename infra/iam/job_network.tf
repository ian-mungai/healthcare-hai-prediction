locals {
  job_classes = toset(["offline", "lakehouse"])
  job_shared_endpoint_clients = {
    offline   = aws_security_group.job["offline"].id
    lakehouse = aws_security_group.job["lakehouse"].id
    host      = aws_security_group.host.id
    task      = aws_security_group.task.id
  }
  job_ssm_endpoint_clients = {
    lakehouse = aws_security_group.job["lakehouse"].id
    host      = aws_security_group.host.id
    task      = aws_security_group.task.id
  }
}

resource "aws_security_group" "job" {
  #checkov:skip=CKV2_AWS_5:Job run tasks attach the offline and lakehouse groups when they start; the endpoint groups attach to the private endpoints.
  for_each = toset(["offline", "lakehouse", "endpoints", "ssm_endpoint"])

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

resource "aws_vpc_security_group_egress_rule" "job_s3" {
  for_each = var.job_endpoints_enabled ? local.job_classes : toset([])

  security_group_id = aws_security_group.job[each.key].id
  description       = "Job HTTPS to S3 through its gateway endpoint"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  prefix_list_id    = aws_vpc_endpoint.job_s3[0].prefix_list_id
}

resource "aws_vpc_security_group_egress_rule" "job_shared_endpoints" {
  for_each = var.job_endpoints_enabled ? local.job_classes : toset([])

  security_group_id            = aws_security_group.job[each.key].id
  description                  = "Job HTTPS to ECR and Logs endpoints"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.job["endpoints"].id
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

resource "aws_vpc_security_group_egress_rule" "job_ssm" {
  count = var.job_endpoints_enabled ? 1 : 0

  security_group_id            = aws_security_group.job["lakehouse"].id
  description                  = "Lakehouse HTTPS to Parameter Store"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.job["ssm_endpoint"].id
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

resource "aws_vpc_security_group_egress_rule" "job_catalog" {
  security_group_id            = aws_security_group.job["lakehouse"].id
  description                  = "Lakehouse jobs to the private catalog listener"
  ip_protocol                  = "tcp"
  from_port                    = 8181
  to_port                      = 8181
  referenced_security_group_id = aws_security_group.host.id
}

resource "aws_vpc_security_group_ingress_rule" "host_from_job_catalog" {
  security_group_id            = aws_security_group.host.id
  description                  = "Private catalog listener from lakehouse jobs only"
  ip_protocol                  = "tcp"
  from_port                    = 8181
  to_port                      = 8181
  referenced_security_group_id = aws_security_group.job["lakehouse"].id
}

resource "aws_vpc_security_group_egress_rule" "job_redis" {
  security_group_id            = aws_security_group.job["lakehouse"].id
  description                  = "Lakehouse jobs to the host Redis that the features job applies Feast to"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = aws_security_group.host.id
}

resource "aws_vpc_security_group_ingress_rule" "host_from_job_redis" {
  security_group_id            = aws_security_group.host.id
  description                  = "Host Redis from lakehouse jobs only"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = aws_security_group.job["lakehouse"].id
}

# The network belongs to the administrator: the project user has no network permission, so it cannot open a port, add a
# route or move the host. Public subnets only, with no NAT gateway: the host and the run tasks reach AWS services and image
# registries through the internet gateway. The platform stack finds the VPC, the subnets and both security groups by their
# Name and Project tags.
data "aws_ec2_instance_type_offerings" "host" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [local.host_instance_type]
  }
}

locals {
  host_availability_zones = slice(sort([for zone in distinct(data.aws_ec2_instance_type_offerings.host.locations) : zone if can(regex("^${var.aws_region}[a-z]$", zone))]), 0, 2)
}

resource "aws_internet_gateway" "platform" {
  vpc_id = aws_vpc.platform.id

  tags = merge(local.project_tags, {
    Name = format(local.platform_name, "internet_gateway")
  })
}

resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.platform.id
  availability_zone       = local.host_availability_zones[count.index]
  cidr_block              = cidrsubnet(aws_vpc.platform.cidr_block, 24 - tonumber(split("/", aws_vpc.platform.cidr_block)[1]), count.index)
  map_public_ip_on_launch = false

  tags = merge(local.project_tags, {
    Name = format(local.platform_name, "public_${count.index}")
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.platform.id

  tags = merge(local.project_tags, {
    Name = format(local.platform_name, "public")
  })
}

resource "aws_route" "internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.platform.id
}

resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_vpc_security_group_ingress_rule" "host_owner" {
  for_each = { for pair in setproduct(var.owner_ip_cidrs, var.host_ingress_ports) : "${pair[0]}_${pair[1]}" => { cidr = pair[0], port = pair[1] } }

  security_group_id = aws_security_group.host.id
  description       = "Owner access"
  ip_protocol       = "tcp"
  cidr_ipv4         = each.value.cidr
  from_port         = each.value.port
  to_port           = each.value.port
}

resource "aws_vpc_security_group_ingress_rule" "host_from_tasks" {
  for_each = toset([for port in var.host_task_ports : tostring(port)])

  security_group_id            = aws_security_group.host.id
  description                  = "Run tasks to host services"
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.task.id
  from_port                    = tonumber(each.key)
  to_port                      = tonumber(each.key)
}

# The host has no NAT gateway, so it reaches ECR, S3, Secrets Manager, ECS and image registries over public HTTPS.
#trivy:ignore:AWS-0104
resource "aws_vpc_security_group_egress_rule" "host_https" {
  security_group_id = aws_security_group.host.id
  description       = "HTTPS to AWS service endpoints and image registries"
  ip_protocol       = "tcp"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
}

resource "aws_security_group" "task" {
  #checkov:skip=CKV2_AWS_5:Run tasks attach this group when they start; no resource in this stack uses it.
  name        = format(local.platform_name, "task")
  description = "Run tasks: no inbound traffic; outbound to HTTPS and to the platform network."
  vpc_id      = aws_vpc.platform.id

  tags = merge(local.project_tags, {
    Name = format(local.platform_name, "task")
  })
}

# Run tasks have no NAT gateway either; they reach S3, Secrets Manager and ECR over public HTTPS.
#trivy:ignore:AWS-0104
resource "aws_vpc_security_group_egress_rule" "task_https" {
  security_group_id = aws_security_group.task.id
  description       = "HTTPS to AWS service endpoints"
  ip_protocol       = "tcp"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "task_platform" {
  security_group_id = aws_security_group.task.id
  description       = "Services on the container host"
  ip_protocol       = "tcp"
  cidr_ipv4         = aws_vpc.platform.cidr_block
  from_port         = 1
  to_port           = 65535
}

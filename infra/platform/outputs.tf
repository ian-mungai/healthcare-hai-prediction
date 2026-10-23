output "vpc_id" {
  description = "Platform network, created by the IAM stack."
  value       = data.aws_vpc.platform.id
}

output "public_subnet_ids" {
  description = "The two public subnets, in different availability zones; run tasks use both with a public IP and no NAT gateway."
  value       = [for index in sort(keys(data.aws_subnet.public)) : data.aws_subnet.public[index].id]
}

output "host_subnet_id" {
  description = "The subnet of the container host and its data volume."
  value       = data.aws_subnet.public["0"].id
}

output "host_security_group_id" {
  description = "Container host group: inbound only from the run-task group on the service ports and from the owner addresses."
  value       = data.aws_security_group.host.id
}

output "task_security_group_id" {
  description = "Run-task group: no inbound rules; outbound HTTPS and TCP inside the platform network."
  value       = data.aws_security_group.task.id
}

output "ecr_repository_urls" {
  description = "Repository URL for each image name; push images by digest."
  value       = { for name, repository in aws_ecr_repository.image : name => repository.repository_url }
}

output "cluster_name" {
  description = "ECS cluster for the host services and the run tasks."
  value       = aws_ecs_cluster.platform.name
}

output "cluster_arn" {
  description = "ECS cluster ARN."
  value       = aws_ecs_cluster.platform.arn
}

output "ec2_capacity_provider_name" {
  description = "EC2 capacity provider of the single container host; host services use it in their capacity provider strategy."
  value       = aws_ecs_capacity_provider.host.name
}

output "fargate_capacity_provider_names" {
  description = "Fargate capacity providers for run tasks: Spot first, on-demand as the fallback. The cluster default is Spot."
  value       = ["FARGATE_SPOT", "FARGATE"]
}

output "host_autoscaling_group_name" {
  description = "Auto Scaling group of the container host, created by the IAM stack: 0 or 1 instances; set the desired capacity to turn the host on or off."
  value       = data.aws_autoscaling_group.host.name
}

output "host_public_ip" {
  description = "Elastic IP the host associates at boot."
  value       = aws_eip.host.public_ip
}

output "data_volume_id" {
  description = "The host's gp3 data volume, snapshotted daily."
  value       = aws_ebs_volume.data.id
}

output "data_mount_path" {
  description = "Where the data volume is mounted on the host; host services keep their state under it."
  value       = local.data_mount_path
}

output "log_group_names" {
  description = "CloudWatch log group for each log name."
  value       = { for name, group in aws_cloudwatch_log_group.platform : name => group.name }
}

output "alerts_topic_arn" {
  description = "SNS topic for run-failure alerts; it has no subscriber until the owner adds one."
  value       = aws_sns_topic.alerts.arn
}

output "project_tag" {
  description = "The Project tag value every platform resource carries and the cost budget filters on."
  value       = local.project_tag
}

output "role_arns" {
  description = "Roles the IAM stack creates for the platform; the project profile only passes them."
  value = {
    host               = "${local.role_arn_prefix}${local.host_role_name}"
    task_execution     = "${local.role_arn_prefix}${local.task_execution_role_name}"
    polaris_task       = "${local.role_arn_prefix}${local.polaris_task_role_name}"
    control_plane_task = "${local.role_arn_prefix}${local.control_plane_task_role_name}"
    snapshots          = "${local.role_arn_prefix}${local.snapshot_role_name}"
  }
}

output "execution_role_arn" {
  description = "Task execution role: pulls project images, writes project logs and injects the control plane's named parameters."
  value       = "${local.role_arn_prefix}${local.task_execution_role_name}"
}

output "polaris_task_role_arn" {
  description = "Task role for Polaris only: read and write under lakehouse/ in the data bucket."
  value       = "${local.role_arn_prefix}${local.polaris_task_role_name}"
}

output "control_plane_task_role_arn" {
  description = "Task role for Postgres, Redis, Airflow and Marquez: no AWS permissions."
  value       = "${local.role_arn_prefix}${local.control_plane_task_role_name}"
}

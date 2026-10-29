output "job_roles" {
  description = "Prepared job role names, ARNs and unique IDs for the validated target file."
  value = merge(
    { for key, role in aws_iam_role.job : key => { name = role.name, arn = role.arn, unique_id = role.unique_id } },
    { for role in aws_iam_role.job_write_e2e_task : "write_e2e_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } },
    { for layer, role in aws_iam_role.job_write_layer_task : "write_${layer}_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } },
    { for role in aws_iam_role.bronze_writer : "write_bronze_task" => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
  )
}

output "job_network" {
  description = "Private class groups and the single endpoint subnet. Application definitions must disable public IPs."
  value = {
    subnet_id          = aws_subnet.public[0].id
    security_group_ids = { for key in local.job_classes : key => aws_security_group.job[key].id }
    endpoints_enabled  = var.job_endpoints_enabled
  }
}

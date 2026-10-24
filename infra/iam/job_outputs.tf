output "job_roles" {
  description = "Read-increment role names, ARNs and unique IDs for the validated target file."
  value       = { for key, role in aws_iam_role.job : key => { name = role.name, arn = role.arn, unique_id = role.unique_id } }
}

output "job_network" {
  description = "Private class groups and the single endpoint subnet. Application definitions must disable public IPs."
  value = {
    subnet_id          = aws_subnet.public[0].id
    security_group_ids = { for key in local.job_classes : key => aws_security_group.job[key].id }
    endpoints_enabled  = var.job_endpoints_enabled
  }
}

output "job_project_policy_delta" {
  description = "Unattached proposal for the lead to integrate into the existing IAM, STS, S3 and SSM service policies before the owner applies them."
  value       = jsondecode(local.job_policy_documents.job_project_delta)
}

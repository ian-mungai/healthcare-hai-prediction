output "policy_arns" {
  description = "Only these customer-managed policies are attached to the existing project user."
  value       = { for policy in aws_iam_policy.service : policy.name => policy.arn }
}

output "lakehouse_role_arns" {
  description = "The two roles whose short sessions the launcher gives to pipeline jobs."
  value = {
    reader = aws_iam_role.lakehouse_reader.arn
    writer = aws_iam_role.lakehouse_writer.arn
  }
}

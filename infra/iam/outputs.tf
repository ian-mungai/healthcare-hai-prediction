output "policy_arns" {
  description = "Only these customer-managed policies are attached to the existing project user."
  value       = { for policy in aws_iam_policy.service : policy.name => policy.arn }
}

output "subscribed_topic_arns" {
  description = "The alert topics that carry a subscription of the alert address, by name. Empty when no address is set. It holds no address."
  value       = local.topics
}

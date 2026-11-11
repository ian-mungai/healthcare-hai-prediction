variable "training_prepared" {
  description = "Prepare the training roles, the training security group and the MLflow rules. The roles hold no lakehouse write. The MLflow role reads and writes the mlflow/ prefix only."
  type        = bool
  default     = false
  nullable    = false
}

variable "alerts_topic_arn" {
  description = "ARN of the platform alerts topic (the alerts_topic_arn output of infra/platform). The training task role, which the promote family runs with, may publish to this one topic, for the owner notice of the promotion chain. Null grants no publish."
  type        = string
  default     = null

  validation {
    condition     = var.alerts_topic_arn == null || can(regex("^arn:aws:sns:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]{1,256}$", var.alerts_topic_arn))
    error_message = "Supply the ARN of one SNS topic: arn:aws:sns:<region>:<account>:<name>, with no wildcard."
  }
}

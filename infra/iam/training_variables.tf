variable "training_prepared" {
  description = "Prepare the training roles, the training security group and the MLflow rules (decision 56). The roles hold no lakehouse write. The MLflow role reads and writes the mlflow/ prefix only."
  type        = bool
  default     = false
  nullable    = false
}

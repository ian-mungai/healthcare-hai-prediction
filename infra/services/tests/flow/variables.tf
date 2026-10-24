variable "definitions" {
  description = "Container definitions JSON of each task definition, keyed by task name."
  type        = map(string)
}

variable "tasks" {
  description = "Task settings of each task definition, keyed by task name (the task_definition_settings output of the services stack): every attribute except container_definitions and those AWS assigns at apply."
  type        = any
}

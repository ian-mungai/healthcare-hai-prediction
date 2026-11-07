variable "host_cpu_units" {
  description = "CPU units of the container host (r7g.xlarge: 4 vCPU, 4096 units)."
  type        = number
  default     = 4096
  nullable    = false

  validation {
    condition     = var.host_cpu_units >= 1024 && var.host_cpu_units == floor(var.host_cpu_units)
    error_message = "Use a whole number of CPU units, at least 1024."
  }
}

variable "job_cpu_units" {
  description = "CPU units the largest job reserves at task level on the host (the jobs stack caps a job at 3072). The services keep the rest of the host."
  type        = number
  default     = 3072
  nullable    = false

  validation {
    condition     = var.job_cpu_units >= 256 && var.job_cpu_units == floor(var.job_cpu_units)
    error_message = "Use a whole number of CPU units, at least 256."
  }
}

variable "host_scratch_folder" {
  description = "Name of the folder below the data mount where the scratch volume is mounted. Each job family gets work and tmp folders in it."
  type        = string
  default     = "job_scratch"
  nullable    = false

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]*$", var.host_scratch_folder))
    error_message = "Use a single folder name of lowercase letters, digits and underscores, with no slash or parent step."
  }
}

variable "host_job_families" {
  description = "The job families that get scratch folders on the host. They match the families of the jobs stack."
  type        = list(string)
  default     = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]
  nullable    = false

  validation {
    condition     = alltrue([for family in var.host_job_families : can(regex("^[a-z][a-z0-9_]*$", family))]) && length(var.host_job_families) == length(distinct(var.host_job_families))
    error_message = "Use distinct family names of lowercase letters, digits and underscores."
  }
}

locals {
  # The host has 4096 CPU units. A job reserves 3072 at task level and the services share the remaining 1024: the scheduler, Polaris
  # and Postgres 256 each, the API server 128 and the other long-lived containers 116 together. A container the table does not name
  # reserves nothing, so a service added later gets a plan failure only if it is named here and the total grows past the limit.
  service_cpu_units = {
    postgres              = 256
    polaris               = 256
    airflow_scheduler     = 256
    airflow_apiserver     = 128
    redis                 = 16
    airflow_dag_processor = 16
    airflow_triggerer     = 8
    mlflow                = 8
    model_api             = 8
    streamlit             = 8
    openmetadata_server   = 16
    opensearch            = 16
    prometheus            = 8
    grafana               = 8
    statsd_exporter       = 4
  }

  service_containers = {
    for key, service in local.services : key => [
      for container in service.containers : merge(container, { for key, units in { cpu = lookup(local.service_cpu_units, container.name, 0) } : key => units if units > 0 })
    ]
  }

  reserved_cpu_units = sum([for containers in values(local.service_containers) : sum([for container in containers : lookup(container, "cpu", 0)])])

  host_scratch_directories = flatten([for family in var.host_job_families : ["/host/${var.host_scratch_folder}/${family}/work", "/host/${var.host_scratch_folder}/${family}/tmp"]])
}

output "reserved_cpu_units" {
  description = "CPU units the planned service containers reserve together. The precondition keeps it within the host's units minus the largest job's."
  value       = local.reserved_cpu_units
}

output "service_cpu_units" {
  description = "The CPU units each named service container reserves."
  value       = local.service_cpu_units
}

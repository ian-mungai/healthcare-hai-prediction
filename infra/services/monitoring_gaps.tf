# The switch of the Evidently UI, declared here for the monitoring gaps work. When the branch that adds the UI (feat/evidently-live-check, infra/services/evidently_ui.tf)
# merges, it declares the same variable: delete this declaration then and keep the locals below.
variable "evidently_ui_enabled" {
  description = "Run the Evidently UI on the host. Off by default. The host state exporter probes the UI on 8600 only while this is true and reports it disabled otherwise."
  type        = bool
  default     = false
  nullable    = false
}

locals {
  # The settings of the optional sources of the host state exporter (monitoring/host_state_exporter.py). OpenSearch exists only with OpenMetadata, so its
  # health address is empty without it and the exporter then has no OpenSearch source. The one-shot task names are the task definition families without the
  # name prefix: the exporter reads the latest stopped task of each (ecs:ListTasks and ecs:DescribeTasks, scoped to the cluster by host_state_read.json).
  monitoring_gap_environment = [
    { name = "EVIDENTLY_UI_ENABLED", value = tostring(var.evidently_ui_enabled) },
    { name = "OPENSEARCH_HEALTH_URL", value = local.om_on ? "http://127.0.0.1:9200/_cluster/health" : "" },
    { name = "BOOTSTRAP_TASKS", value = join(",", concat(local.code_fetch_on ? ["code_fetch"] : [], ["polaris_bootstrap"], local.om_on ? ["openmetadata_bootstrap"] : [], local.catalog_setup_on ? ["catalog_setup"] : [])) },
  ]
}

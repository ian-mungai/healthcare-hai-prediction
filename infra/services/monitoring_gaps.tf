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

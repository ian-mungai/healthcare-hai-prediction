# The owner reaches a web page on the host through one project Session Manager document per page and no other. Each document fixes the host port and takes only the
# local port as a parameter, so a port without a web page cannot be forwarded. The persistent OpenMetadata document (openmetadata_persistent.tf) is a further one.
# A document exists only while its service is prepared. Airflow always runs on the host, so its document always exists. The project user's policy names each
# document by ARN, and the generic AWS port forward document is not granted.
locals {
  web_ui_forwards = {
    airflow      = { port = 8080, page = "the Airflow web interface", present = true }
    mlflow       = { port = 5100, page = "the MLflow web interface", present = var.training_prepared }
    streamlit    = { port = 8501, page = "the Streamlit page", present = var.model_api_prepared }
    grafana      = { port = 3001, page = "the Grafana web interface", present = var.monitoring_enabled }
    prometheus   = { port = 9090, page = "the Prometheus web interface", present = var.monitoring_enabled }
    evidently_ui = { port = 8600, page = "the Evidently web interface", present = var.evidently_ui_prepared }
  }
}

resource "aws_ssm_document" "web_ui_forward" {
  for_each = { for key, forward in local.web_ui_forwards : key => forward if forward.present }

  name          = format(local.platform_name, "${each.key}_forward")
  document_type = "Session"
  tags          = local.project_tags
  content = jsonencode({
    schemaVersion = "1.0"
    description   = "Forward a local port to ${each.value.page} on the container host. The destination port is fixed at ${each.value.port}."
    sessionType   = "Port"
    parameters = {
      localPortNumber = {
        type           = "String"
        description    = "Local port to listen on."
        allowedPattern = "^[0-9]{1,5}$"
        default        = tostring(each.value.port)
      }
    }
    properties = {
      portNumber      = tostring(each.value.port)
      type            = "LocalPortForwarding"
      localPortNumber = "{{ localPortNumber }}"
    }
  })
}

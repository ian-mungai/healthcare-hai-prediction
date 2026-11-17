mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["eu-west-1a", "eu-west-1b"] }
  }

  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev" }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = { arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev" }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values          = { id = "lt-0123456789abcdef0", latest_version = 1 }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values          = { arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev" }
}

variables {
  aws_profile                      = "example_project_dev"
  environment                      = "dev"
  admin_profile                    = "example_admin"
  aws_region                       = "eu-west-1"
  expected_account_id              = "111111111111"
  data_bucket_name                 = "example-project-ci-bucket"
  project_name                     = "example_project"
  owner_ip_cidrs                   = []
  openmetadata_admin_password_name = "/example_project/openmetadata_admin_password"
  openmetadata_db_password_name    = "/example_project/openmetadata_db_password"
  openmetadata_bot_token_names = {
    lineage    = "/example_project/openmetadata_bot_lineage"
    ingestion  = "/example_project/openmetadata_bot_ingestion"
    comparison = "/example_project/openmetadata_bot_comparison"
  }
}

# The owner reaches a web page on the host through a project Session Manager document and no other way. Each document fixes the host port, takes only the local
# port as a parameter and exists only while its service is prepared. No document exists for a port without a web page. Every case reads the plan only.
run "default_holds_only_the_airflow_document" {
  command = plan

  assert {
    condition = (
      length(aws_ssm_document.web_ui_forward) == 1
      && contains(keys(aws_ssm_document.web_ui_forward), "airflow")
      && aws_ssm_document.web_ui_forward["airflow"].name == "example_project_airflow_forward_dev"
      && jsondecode(aws_ssm_document.web_ui_forward["airflow"].content).properties.portNumber == "8080"
      && length(aws_ssm_document.openmetadata_ui_forward) == 0
    )
    error_message = "With every switch off, the stack holds the Airflow document only: Airflow always runs and no other web page does."
  }
}

run "each_prepared_service_adds_exactly_its_document" {
  command = plan

  variables {
    training_prepared         = true
    model_api_prepared        = true
    model_api_registry_key    = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix = "serving/deployments"
  }

  assert {
    condition     = toset(keys(aws_ssm_document.web_ui_forward)) == toset(["airflow", "mlflow", "streamlit"])
    error_message = "Training adds the MLflow document and the model API switch adds the Streamlit one. Neither adds a monitoring document."
  }

  assert {
    condition     = jsondecode(aws_ssm_document.web_ui_forward["mlflow"].content).properties.portNumber == "5100" && jsondecode(aws_ssm_document.web_ui_forward["streamlit"].content).properties.portNumber == "8501"
    error_message = "MLflow forwards 5100 and Streamlit forwards 8501."
  }
}

run "monitoring_adds_grafana_and_prometheus" {
  command = plan

  variables {
    monitoring_enabled = true
  }

  assert {
    condition = (
      toset(keys(aws_ssm_document.web_ui_forward)) == toset(["airflow", "grafana", "prometheus"])
      && jsondecode(aws_ssm_document.web_ui_forward["grafana"].content).properties.portNumber == "3001"
      && jsondecode(aws_ssm_document.web_ui_forward["prometheus"].content).properties.portNumber == "9090"
    )
    error_message = "Monitoring adds the Grafana document at 3001 and the Prometheus document at 9090 and nothing else."
  }
}

run "every_document_fixes_its_port_and_takes_only_the_local_port" {
  command = plan

  variables {
    training_prepared                = true
    model_api_prepared               = true
    model_api_registry_key           = "lakehouse/feast/registry.pb"
    model_api_manifest_prefix        = "serving/deployments"
    monitoring_enabled               = true
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = alltrue([
      for key, document in aws_ssm_document.web_ui_forward : (
        document.document_type == "Session"
        && document.name == "example_project_${key}_forward_dev"
        && jsondecode(document.content).sessionType == "Port"
        && jsondecode(document.content).properties.type == "LocalPortForwarding"
        && jsondecode(document.content).properties.localPortNumber == "{{ localPortNumber }}"
        && keys(jsondecode(document.content).parameters) == ["localPortNumber"]
        && !can(jsondecode(document.content).properties.host)
      )
    ])
    error_message = "Each web page document is a Session document that forwards to the loopback of the host, fixes the host port and takes only the local port as a parameter."
  }

  assert {
    condition = (
      toset([for document in aws_ssm_document.web_ui_forward : jsondecode(document.content).properties.portNumber]) == toset(["8080", "5100", "8501", "3001", "9090"])
      && jsondecode(aws_ssm_document.openmetadata_ui_forward[0].content).properties.portNumber == "8585"
    )
    error_message = "The documents forward exactly the web page ports 8080, 8585, 5100, 8501, 3001 and 9090."
  }

  assert {
    condition = length(setintersection(
      toset([for document in aws_ssm_document.web_ui_forward : jsondecode(document.content).properties.portNumber]),
      toset(["5432", "6379", "8181", "8182", "8000", "8586", "9200", "8974", "9102", "9125", "9091", "3000"]),
    )) == 0
    error_message = "No document forwards a port that has no web page: the databases, the catalog, the model API, the admin and search ports, the scheduler health and the metrics ports."
  }
}

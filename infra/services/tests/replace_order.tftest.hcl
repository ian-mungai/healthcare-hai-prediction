mock_provider "aws" {
  override_during = plan
}

# A services apply that replaces a task definition must register the new revision before it retires the old one, so a registration that ECS refuses leaves the old
# revision in the state and the services keep a definition. On Nov 16 2026 ECS refused seven new jobs definitions after Terraform had already destroyed the old ones in the
# state; the services stack has the same replace order. A plan does not run the ECS limits, so the proof is the order of the actions: a replacement that creates first is
# planned as ["create", "delete"], one that destroys first as ["delete", "create"]. scripts/infrastructure/run_services_replace_order_e2e.py reads the plan of
# replace_every_definition and refuses any task definition that is replaced in the old order or that is not replaced.
# The fixture turns every optional service on, so the four task definition families of the live stack (service, catalog, serving and one-shot) are all in the plan.
variables {

  # TEST FIXTURE ONLY, not a measured value: the host job cap. The default is the room the live stack leaves, which this fixture does not have.
  host_job_cap_mib                = 4096
  aws_profile                     = "example_project_dev"
  environment                     = "dev"
  project_name                    = "example_project"
  project_tag                     = "example_project"
  aws_region                      = "us-west-2"
  expected_account_id             = "111111111111"
  cluster_name                    = "example-project-dev"
  ec2_capacity_provider_name      = "example-project-dev-host"
  execution_role_arn              = "arn:aws:iam::111111111111:role/example-execution"
  serving_execution_role_arn      = "arn:aws:iam::111111111111:role/example-serving-execution"
  polaris_task_role_arn           = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn     = "arn:aws:iam::111111111111:role/example-control"
  mlflow_task_role_arn            = "arn:aws:iam::111111111111:role/example-mlflow"
  model_api_task_role_arn         = "arn:aws:iam::111111111111:role/example-model-api"
  streamlit_task_role_arn         = "arn:aws:iam::111111111111:role/example-streamlit"
  openmetadata_task_role_arn      = "arn:aws:iam::111111111111:role/example-openmetadata"
  airflow_launch_task_role_arn    = "arn:aws:iam::111111111111:role/example-airflow-launch"
  code_fetch_task_role_arn        = "arn:aws:iam::111111111111:role/example-code-fetch"
  data_mount_path                 = "/mnt/example-data"
  host_private_ip                 = "10.0.1.20"
  mlflow_artifact_bucket          = "example-project-data-dev"
  airflow_launch_bucket_name      = "example-project-data-dev"
  code_bucket_name                = "example-project-data-dev"
  mlflow_enabled                  = true
  monitoring_enabled              = true
  openmetadata_persistent_enabled = true
  model_api_enabled               = true
  streamlit_enabled               = true
  model_api_hai_types             = "HAI_1"
  streamlit_hai_types             = "HAI_1"
  model_api_manifest_uri          = "s3://example-project-data-dev/serving/deployments"
  model_api_registry_uri          = "s3://example-project-data-dev/lakehouse/feast/registry.pb"
  model_api_mlflow_uri            = "http://127.0.0.1:5100"

  log_group_names = {
    airflow    = "/example/airflow"
    marquez    = "/example/marquez"
    mlflow     = "/example/mlflow"
    monitoring = "/example/monitoring"
    polaris    = "/example/polaris"
    postgres   = "/example/postgres"
    redis      = "/example/redis"
  }

  ecr_repository_urls = {
    airflow                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    mlflow                 = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/mlflow"
    model_api              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/model_api"
    openmetadata_ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_ingestion"
    openmetadata_search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_search"
    openmetadata_server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_server"
    polaris                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    postgres               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    prometheus             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
    grafana                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
    statsd_exporter        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
    redis                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    streamlit              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/streamlit"
    training               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training"
  }

  image_digests = {
    airflow                = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    mlflow                 = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    model_api              = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    openmetadata_ingestion = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    openmetadata_search    = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    openmetadata_server    = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    polaris                = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    polaris_admin          = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    postgres               = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    prometheus             = "sha256:5555555555555555555555555555555555555555555555555555555555555555"
    grafana                = "sha256:6666666666666666666666666666666666666666666666666666666666666666"
    statsd_exporter        = "sha256:7777777777777777777777777777777777777777777777777777777777777777"
    redis                  = "sha256:8888888888888888888888888888888888888888888888888888888888888888"
    streamlit              = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    training               = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
}

run "register_every_definition" {
  command = apply

  assert {
    condition     = length(aws_ecs_task_definition.service) == 12 && length(aws_ecs_task_definition.one_shot) == 4
    error_message = "The state holds the twelve service definitions and the four one-shot definitions before the replacement."
  }

  assert {
    condition     = length(aws_ecs_service.service) == 9 && length(aws_ecs_service.catalog) == 1 && length(aws_ecs_service.serving) == 2
    error_message = "The state holds the nine fixed services, the catalog service and the two serving services, so every service resource kind reads a definition."
  }
}

# The mock provider cannot decide which change forces a new registration (the real provider does), so the run replaces the sixteen definitions by address. A task
# definition that this list does not name stays unchanged in the plan, and the E2E refuses a plan that holds any task definition outside the replaced ones.
run "replace_every_definition" {
  command = plan

  plan_options {
    replace = [
      aws_ecs_task_definition.one_shot["code_fetch"],
      aws_ecs_task_definition.one_shot["host_prepare"],
      aws_ecs_task_definition.one_shot["openmetadata_bootstrap"],
      aws_ecs_task_definition.one_shot["polaris_bootstrap"],
      aws_ecs_task_definition.service["airflow_apiserver"],
      aws_ecs_task_definition.service["airflow_dag_processor"],
      aws_ecs_task_definition.service["airflow_scheduler"],
      aws_ecs_task_definition.service["airflow_triggerer"],
      aws_ecs_task_definition.service["mlflow"],
      aws_ecs_task_definition.service["model_api"],
      aws_ecs_task_definition.service["monitoring"],
      aws_ecs_task_definition.service["openmetadata"],
      aws_ecs_task_definition.service["polaris"],
      aws_ecs_task_definition.service["postgres"],
      aws_ecs_task_definition.service["redis"],
      aws_ecs_task_definition.service["streamlit"]
    ]
  }

  assert {
    condition     = length(aws_ecs_task_definition.service) == 12 && length(aws_ecs_task_definition.one_shot) == 4
    error_message = "The replacement plan still declares the twelve service definitions and the four one-shot definitions."
  }
}

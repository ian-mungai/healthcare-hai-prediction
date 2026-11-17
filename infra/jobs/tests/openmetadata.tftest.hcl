mock_provider "aws" {}

# The catalog inputs of the metadata host job: the ingestion image and the two catalog roles. The job is a client of the persistent catalog.
variables {
  # The committed defaults of the jobs stack are the live ones (every job family on, host_repo_path and serve_manifest_prefix set). Every case here that does
  # not set a switch tests it off, so the fixture sets each one explicitly.
  drift_enabled                         = false
  promote_enabled                       = false
  layer_jobs_enabled                    = false
  serve_enabled                         = false
  train_enabled                         = false
  publisher_job_enabled                 = false
  publisher_keys_enabled                = false
  serve_manifest_prefix                 = ""
  aws_profile                           = "example_project_dev"
  aws_region                            = "us-west-2"
  expected_account_id                   = "111111111111"
  project_name                          = "example_project"
  environment                           = "dev"
  data_bucket_name                      = "example-project-data-dev"
  log_group_name                        = "example_project_jobs_dev"
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  host_job_memory_mib                   = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding                = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  host_jobs_enabled                    = true
  host_jobs                            = ["metadata"]
  host_private_ip                      = "10.0.1.20"
  host_repo_path                       = "/data/repo"
  openmetadata_ingestion_token_name    = "/example_project/openmetadata_bot_ingestion"
  mlflow_metadata_reader_password_name = "/example_project/mlflow_metadata_reader_password"
  openmetadata_images = {
    ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
  }
  openmetadata_roles = {
    execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
  }
}

run "the_metadata_job_is_one_host_task_with_the_catalog_roles" {
  command = plan

  assert {
    condition = (
      aws_ecs_task_definition.host["metadata"].family == "example_project_dev_job_metadata_host" &&
      aws_ecs_task_definition.host["metadata"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev" &&
      aws_ecs_task_definition.host["metadata"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.host["metadata"].requires_compatibilities) == toset(["EC2"]) && aws_ecs_task_definition.host["metadata"].network_mode == "bridge" &&
      aws_ecs_task_definition.host["metadata"].runtime_platform[0].cpu_architecture == "ARM64" &&
      aws_ecs_task_definition.host["metadata"].cpu == "1024" && aws_ecs_task_definition.host["metadata"].memory == "2048"
    )
    error_message = "The metadata job is one ARM64 host definition with the execution role, the catalog task role and its reservation of the host."
  }
}

run "refuses_a_tag_instead_of_a_digest" {
  command = plan

  variables {
    openmetadata_images = {
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev:latest"
    }
  }

  expect_failures = [var.openmetadata_images]
}

run "refuses_a_writer_role" {
  command = plan

  variables {
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    }
  }

  expect_failures = [var.openmetadata_roles]
}

run "refuses_a_plain_name_other_than_the_project_token" {
  command = plan

  variables {
    openmetadata_ingestion_token_name = "/example_project/openmetadata_admin_password"
  }

  expect_failures = [var.openmetadata_ingestion_token_name]
}

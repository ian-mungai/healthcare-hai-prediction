mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    offline_task        = "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev"
    seal_task           = "arn:aws:iam::111111111111:role/example_project_job_seal_task_dev"
    read_task           = "arn:aws:iam::111111111111:role/example_project_job_read_task_dev"
    write_e2e_task      = "arn:aws:iam::111111111111:role/example_project_job_write_e2e_task_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    write_bronze_task   = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  }
  writer_parameter_names = {
    write_e2e_task      = ["/example_project/aws_polaris_e2e_client_id", "/example_project/aws_polaris_e2e_client_secret"]
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
    write_bronze_task   = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"]
  }
  images = {
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }
  task_resources = {
    spark_write_e2e      = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_silver   = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_gold     = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_features = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_bronze   = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    analytics_offline    = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    quality_offline      = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    seal                 = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    analytics_read       = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    spark_read           = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  log_group_name                        = "example_project_jobs_dev"
  cloud_limits                          = { max_tasks = 3, max_vcpu = 16, max_memory_gib = 120 }
}

run "default_adds_no_raw_read_variable_to_any_container" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_bronze", "spark_write_e2e", "spark_write_silver"]
  }
  assert {
    condition     = alltrue([for name, definition in output.container_definitions : !strcontains(definition, "HAI_READ_RAW_ROOT")])
    error_message = "With no read environment the writer containers must be exactly as they were."
  }
}

run "the_bronze_and_qualification_writers_get_the_raw_read_environment_and_no_other_writer_does" {
  command = plan
  variables {
    jobs_enabled            = true
    enabled_definitions     = ["spark_write_bronze", "spark_write_e2e", "spark_write_silver"]
    bronze_read_environment = { HAI_READ_RAW_ROOT = "true" }
  }
  assert {
    condition = (
      contains(jsondecode(output.container_definitions["spark_write_bronze"])[0].environment, { name = "HAI_READ_RAW_ROOT", value = "true" }) &&
      contains(jsondecode(output.container_definitions["spark_write_e2e"])[0].environment, { name = "HAI_READ_RAW_ROOT", value = "true" }) &&
      !strcontains(output.container_definitions["spark_write_silver"], "HAI_READ_RAW_ROOT")
    )
    error_message = "Only the writers that read source collections may receive the raw read variable."
  }
}

run "only_the_one_raw_read_variable_with_the_value_true_is_accepted" {
  command = plan
  variables {
    jobs_enabled            = true
    enabled_definitions     = ["spark_write_bronze"]
    bronze_read_environment = { HAI_READ_RAW_ROOT = "yes" }
  }
  expect_failures = [var.bronze_read_environment]
}

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
    publisher_task      = "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev"
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
    publisher_fetch      = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  log_group_name                        = "example_project_jobs_dev"
  cloud_limits                          = { max_tasks = 3, max_vcpu = 16, max_memory_gib = 120 }
}

run "default_registers_no_publisher_class" {
  command = plan
  variables {
    images                 = {}
    job_role_arns          = {}
    writer_parameter_names = {}
    task_resources         = {}
    log_group_name         = null
    cloud_limits           = null
  }
  assert {
    condition     = output.container_definitions == {} && output.publisher_dispatch_contracts == {}
    error_message = "Disabled defaults must register no publisher definition and export no publisher contract."
  }
}

run "publisher_definition_has_its_own_role_no_secrets_and_the_raw_root" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["publisher_fetch"]
    writer_parameter_names = {}
  }
  assert {
    condition = (
      output.task_definition_settings["publisher_fetch"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev" &&
      output.task_definition_settings["publisher_fetch"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      jsondecode(output.container_definitions["publisher_fetch"])[0].secrets == [] &&
      contains(jsondecode(output.container_definitions["publisher_fetch"])[0].environment, { name = "HAI_CAPTURE_ROOT", value = "raw/" }) &&
      jsondecode(output.container_definitions["publisher_fetch"])[0].logConfiguration.options["awslogs-group"] == "example_project_jobs_dev" &&
      jsondecode(output.container_definitions["publisher_fetch"])[0].user == "10001:10001" &&
      jsondecode(output.container_definitions["publisher_fetch"])[0].readonlyRootFilesystem
    )
    error_message = "The publisher definition must use its own task role, hold no secret, write under raw, log to the jobs group and keep the hardened container settings."
  }
}

run "publisher_contract_states_public_ip_and_capacity" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["publisher_fetch"]
    writer_parameter_names = {}
  }
  assert {
    condition = output.publisher_dispatch_contracts == {
      publisher_fetch = {
        job_class          = "publisher"
        task_role_key      = "publisher_task"
        capacity_providers = ["FARGATE"]
        network            = "publisher"
        assign_public_ip   = "ENABLED"
        capture_root       = "raw/"
      }
    }
    error_message = "The publisher contract must name its class, role, on-demand capacity, own network group, the public IP the no-NAT network needs and the raw root."
  }
}

run "publisher_definition_needs_its_task_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["publisher_fetch"]
    writer_parameter_names = {}
    job_role_arns = {
      execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

mock_provider "aws" {}

variables {
  aws_profile                           = "example_project_dev"
  aws_region                            = "us-west-2"
  expected_account_id                   = "111111111111"
  project_name                          = "example_project"
  environment                           = "dev"
  data_bucket_name                      = "example-project-data-dev"
  log_group_name                        = "example_project_jobs_dev"
  openmetadata_admin_password_name      = "/example_project/openmetadata_admin_password"
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
}

run "default_registers_no_catalog_task" {
  command = plan

  assert {
    condition     = !var.openmetadata_enabled && length(aws_ecs_task_definition.openmetadata) == 0
    error_message = "The catalog task must default to off and register nothing."
  }

  assert {
    condition     = output.openmetadata_run_task == null && output.openmetadata_container_definitions == null
    error_message = "A disabled catalog task reports no run settings and no containers."
  }
}

run "enabled_plan_registers_one_bounded_private_task" {
  command = plan

  variables {
    openmetadata_enabled = true
    openmetadata_images = {
      server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_server_dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
      search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_search_dev@sha256:3333333333333333333333333333333333333333333333333333333333333333"
      database  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_database_dev@sha256:4444444444444444444444444444444444444444444444444444444444444444"
    }
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    }
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.openmetadata) == 1 &&
      aws_ecs_task_definition.openmetadata[0].family == "example_project_dev_job_metadata" &&
      aws_ecs_task_definition.openmetadata[0].requires_compatibilities == toset(["FARGATE"]) &&
      aws_ecs_task_definition.openmetadata[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.openmetadata[0].cpu == "4096" &&
      aws_ecs_task_definition.openmetadata[0].memory == "24576" &&
      aws_ecs_task_definition.openmetadata[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev" &&
      aws_ecs_task_definition.openmetadata[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    )
    error_message = "The catalog task is a Fargate task of 4 vCPU and 24 GiB that runs under its own read-only role."
  }

  assert {
    condition     = one(aws_ecs_task_definition.openmetadata[0].runtime_platform).cpu_architecture == "X86_64"
    error_message = "The task defaults to x86 until the owner has checked that every pushed image lists linux/arm64."
  }

  assert {
    condition = (
      toset([for c in jsondecode(output.openmetadata_container_definitions) : c.name]) ==
      toset(["init", "database", "search", "migrate", "server", "bootstrap", "ingest_polaris", "ingest_dbt"])
    )
    error_message = "Without a Marquez address the task has no Marquez step."
  }

  assert {
    condition = alltrue([for c in jsondecode(output.openmetadata_container_definitions) :
      can(regex("@sha256:[a-f0-9]{64}$", c.image)) && !c.privileged && c.linuxParameters.initProcessEnabled
    ])
    error_message = "Every container runs a digest-pinned image, unprivileged."
  }

  assert {
    condition = (
      toset([for c in jsondecode(output.openmetadata_container_definitions) : c.name if !c.readonlyRootFilesystem]) == toset(["database", "search", "migrate", "server"])
    )
    error_message = "Only the upstream server images get a writable root file system."
  }

  assert {
    condition = alltrue([for c in jsondecode(output.openmetadata_container_definitions) :
      !contains(["database", "search", "server"], c.name) || can(regex("openmetadata_bounded_run", join(" ", c.entryPoint)))
    ])
    error_message = "Each long-running server runs inside the deadline wrapper."
  }

  assert {
    condition = (
      [for c in jsondecode(output.openmetadata_container_definitions) : [for e in c.environment : e.value if e.name == "OM_LIFETIME_SECONDS"][0] if c.name == "init"][0] == "7200"
    )
    error_message = "The init step fixes the deadline from the lifetime variable, which defaults to two hours."
  }

  assert {
    condition = (
      [for c in jsondecode(output.openmetadata_container_definitions) : [for e in c.environment : e.value if e.name == "PIPELINE_SERVICE_CLIENT_ENABLED"][0] if c.name == "server"][0] == "false"
    )
    error_message = "The server cannot deploy or trigger ingestion pipelines, so no profiler or sampling pipeline can be started from its UI."
  }

  assert {
    condition = (
      [for c in jsondecode(output.openmetadata_container_definitions) : [for s in lookup(c, "secrets", []) : s.name] if c.name == "bootstrap"][0] == ["OM_ADMIN_PASSWORD"] &&
      [for c in jsondecode(output.openmetadata_container_definitions) : [for s in lookup(c, "secrets", []) : s.name] if c.name == "ingest_polaris"][0] == ["POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"] &&
      alltrue([for c in jsondecode(output.openmetadata_container_definitions) : length(lookup(c, "secrets", [])) == 0 if !contains(["bootstrap", "ingest_polaris"], c.name)])
    )
    error_message = "Only the bootstrap step reads the admin password and only the Polaris step reads the reader client pair."
  }

  assert {
    condition = alltrue([for c in jsondecode(output.openmetadata_container_definitions) :
      alltrue([for s in lookup(c, "secrets", []) : !can(regex("(writer|silver|gold|features|bronze|e2e|root)", s.valueFrom))])
    ])
    error_message = "No writer or administrator catalog credential reaches the catalog task."
  }

  assert {
    condition     = length(output.openmetadata_container_definitions) < 40000
    error_message = "The container JSON must stay well under the 64 KiB task definition limit."
  }

  assert {
    condition     = length(output.openmetadata_secret_flow.violations) == 0 && length(output.openmetadata_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check must pass and its mutations must be caught."
  }

  assert {
    condition = (
      output.openmetadata_run_task.assign_public_ip == "DISABLED" &&
      output.openmetadata_run_task.capacity_providers == ["FARGATE"] &&
      output.openmetadata_run_task.ui_port == 8585 &&
      output.openmetadata_run_task.lifetime_seconds == 7200 &&
      output.openmetadata_run_task.broker_deadline_seconds == 8100 &&
      output.openmetadata_run_task.hourly_usd_range.x86 == 0.27 &&
      output.openmetadata_run_task.hourly_usd_range.arm == 0.21
    )
    error_message = "The run settings give the private UI port, the lifetime, the broker deadline of lifetime plus 900 seconds and the hourly cost."
  }
}

run "arm_and_marquez_are_opt_in" {
  command = plan

  variables {
    openmetadata_enabled          = true
    openmetadata_cpu_architecture = "ARM64"
    openmetadata_marquez_url      = "http://10.0.1.20:5000"
    openmetadata_lifetime_seconds = 3600
    openmetadata_images = {
      server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_server_dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
      search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_search_dev@sha256:3333333333333333333333333333333333333333333333333333333333333333"
      database  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_database_dev@sha256:4444444444444444444444444444444444444444444444444444444444444444"
    }
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    }
  }

  assert {
    condition     = one(aws_ecs_task_definition.openmetadata[0].runtime_platform).cpu_architecture == "ARM64"
    error_message = "The owner can switch the whole task to Arm."
  }

  assert {
    condition     = contains([for c in jsondecode(output.openmetadata_container_definitions) : c.name], "ingest_marquez")
    error_message = "A Marquez address adds the Marquez step."
  }

  assert {
    condition     = output.openmetadata_run_task.broker_deadline_seconds == 4500
    error_message = "The broker deadline follows the lifetime."
  }
}

run "refuses_unbounded_lifetime" {
  command = plan

  variables {
    openmetadata_lifetime_seconds = 90000
  }

  expect_failures = [var.openmetadata_lifetime_seconds]
}

run "refuses_a_tag_instead_of_a_digest" {
  command = plan

  variables {
    openmetadata_enabled = true
    openmetadata_images = {
      server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_server_dev:latest"
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
      search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_search_dev@sha256:3333333333333333333333333333333333333333333333333333333333333333"
      database  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_database_dev@sha256:4444444444444444444444444444444444444444444444444444444444444444"
    }
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    }
  }

  expect_failures = [var.openmetadata_images]
}

run "refuses_a_writer_role" {
  command = plan

  variables {
    openmetadata_enabled = true
    openmetadata_images = {
      server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_server_dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
      search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_search_dev@sha256:3333333333333333333333333333333333333333333333333333333333333333"
      database  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_database_dev@sha256:4444444444444444444444444444444444444444444444444444444444444444"
    }
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    }
  }

  expect_failures = [var.openmetadata_roles]
}

run "refuses_enabling_without_images" {
  command = plan

  variables {
    openmetadata_enabled = true
  }

  expect_failures = [var.openmetadata_images, var.openmetadata_roles]
}

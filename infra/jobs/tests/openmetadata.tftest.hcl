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
      alltrue([for c in jsondecode(output.openmetadata_container_definitions) : length(lookup(c, "secrets", [])) == 0 if c.name != "bootstrap"])
    )
    error_message = "Only the bootstrap step reads the admin password. The lakehouse step reads the Glue mirror with the task role and needs no Polaris client pair."
  }

  # OpenMetadata 2.0.7 has no Iceberg connector: the step keeps its broker-table name ingest_polaris but runs the Glue source (owner decision of Nov 7 2026).
  # The server and migration commands are those of the 2.0.7 server image (ENTRYPOINT /bin/bash, CMD /openmetadata-start.sh, WORKDIR /opt/openmetadata).
  assert {
    condition = (
      [for c in jsondecode(output.openmetadata_container_definitions) : contains(c.entryPoint, "glue") && !contains(c.entryPoint, "polaris") if c.name == "ingest_polaris"][0] &&
      [for c in jsondecode(output.openmetadata_container_definitions) : slice(c.entryPoint, length(c.entryPoint) - 2, length(c.entryPoint)) if c.name == "server"][0] == ["/bin/bash", "/openmetadata-start.sh"] &&
      [for c in jsondecode(output.openmetadata_container_definitions) : slice(c.entryPoint, length(c.entryPoint) - 3, length(c.entryPoint)) if c.name == "migrate"][0] == ["/bin/bash", "./bootstrap/openmetadata-ops.sh", "migrate"]
    )
    error_message = "The lakehouse step runs the Glue source, and the server and migration run the image's own scripts through bash."
  }

  assert {
    condition = alltrue([for c in jsondecode(output.openmetadata_container_definitions) :
      alltrue([for s in lookup(c, "secrets", []) : !can(regex("(writer|silver|gold|features|bronze|e2e|root)", s.valueFrom))])
    ])
    error_message = "No writer or administrator catalog credential reaches the catalog task."
  }

  assert {
    condition = (
      [for c in jsondecode(output.openmetadata_container_definitions) : c.essential if c.name == "server"][0] &&
      [for c in jsondecode(output.openmetadata_container_definitions) : c.essential if c.name == "database"][0] &&
      [for c in jsondecode(output.openmetadata_container_definitions) : c.essential if c.name == "search"][0]
    )
    error_message = "A server that stops must stop the task, so the server is essential beside the database and the search index."
  }

  assert {
    condition = (
      can(regex("chmod 1777 /tmp", [for c in jsondecode(output.openmetadata_container_definitions) : join("\n", c.entryPoint) if c.name == "init"][0])) &&
      can(regex("chmod 1777 /run/om", [for c in jsondecode(output.openmetadata_container_definitions) : join("\n", c.entryPoint) if c.name == "init"][0])) &&
      !can(regex("chown -R 10001:10001 /workspace /tmp", [for c in jsondecode(output.openmetadata_container_definitions) : join("\n", c.entryPoint) if c.name == "init"][0]))
    )
    error_message = "The temporary folder and the run folder must be writable by every container user, so they are sticky and open and not owned by one user."
  }

  assert {
    condition     = can(regex("openmetadata_migrate", [for c in jsondecode(output.openmetadata_container_definitions) : join(" ", c.entryPoint) if c.name == "migrate"][0]))
    error_message = "The migration runs through the retry script that writes the abort marker."
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

run "containers_follow_the_broker_table" {
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
      toset([for container in jsondecode(output.openmetadata_container_definitions) : container.name if length([for variable in container.environment : variable.name if variable.value == ""]) > 0]) ==
      toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.metadata.containers)) &&
      alltrue([for container in jsondecode(output.openmetadata_container_definitions) : (
        length([for variable in container.environment : variable.name if variable.value == ""]) == 0 ||
        toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.metadata.containers[container.name])
      )]) &&
      contains([for container in jsondecode(output.openmetadata_container_definitions) : container.name], jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.metadata.log_container)
    )
    error_message = "Every container with an empty variable is named in the broker table with exactly those variables, so no container of the catalog task starts with an empty run ID."
  }
}

run "marquez_step_needs_its_own_table_entry" {
  command = plan

  variables {
    openmetadata_enabled     = true
    openmetadata_marquez_url = "http://10.0.1.20:5000"
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
      !contains(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.metadata.containers), "ingest_marquez") &&
      [for variable in one([for container in jsondecode(output.openmetadata_container_definitions) : container if container.name == "ingest_marquez"]).environment : variable.name if variable.value == ""] == ["RUN_ID"]
    )
    error_message = "With a Marquez address the Marquez step needs RUN_ID, and the broker table does not name it yet. Add it to the metadata job in the same change that turns the address on."
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

# With openmetadata_server_url set, the catalog task keeps its container names, so the broker table still binds it, but runs only the two ingestion steps against the
# persistent catalog on the host: no database, search, migration, server or bootstrap, and no Marquez step. The ingestion bot token comes from Parameter Store.
run "persistent_server_makes_an_ingestion_only_task" {
  command = plan

  variables {
    openmetadata_enabled              = true
    openmetadata_server_url           = "http://10.0.1.20:8585"
    openmetadata_ingestion_token_name = "/example_project/openmetadata_bot_ingestion"
    openmetadata_marquez_url          = "http://10.0.1.20:5000"
    openmetadata_images = {
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
    }
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    }
  }

  assert {
    condition     = [for c in jsondecode(output.openmetadata_container_definitions) : c.name] == ["init", "ingest_polaris", "ingest_dbt"]
    error_message = "The persistent mode runs the init step and the two ingestion steps only, and no Marquez step even when an address is set."
  }

  assert {
    condition = (
      alltrue([for c in jsondecode(output.openmetadata_container_definitions) : endswith(c.image, "example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222")])
      && [for c in jsondecode(output.openmetadata_container_definitions) : c.essential] == [false, false, true]
    )
    error_message = "Every step runs the one ingestion image, and the task ends when the last ingestion step ends."
  }

  assert {
    condition = alltrue([
      for c in jsondecode(output.openmetadata_container_definitions) : c.name == "init" || (
        contains(c.entryPoint, "--token-env") && contains(c.entryPoint, "OM_INGESTION_TOKEN") && contains(c.entryPoint, "--server-url") && contains(c.entryPoint, "http://10.0.1.20:8585/api") && !contains(c.entryPoint, "--token-file")
      )
    ])
    error_message = "The ingestion steps read the token from the environment and call the persistent catalog at the host address."
  }

  assert {
    condition = (
      [for c in jsondecode(output.openmetadata_container_definitions) : [for s in c.secrets : s.valueFrom if s.name == "OM_INGESTION_TOKEN"] if c.name == "ingest_dbt"][0] == ["arn:aws:ssm:us-west-2:111111111111:parameter/example_project/openmetadata_bot_ingestion"]
      && [for c in jsondecode(output.openmetadata_container_definitions) : [for s in c.secrets : s.name] if c.name == "ingest_polaris"][0] == ["OM_INGESTION_TOKEN"]
      && [for c in jsondecode(output.openmetadata_container_definitions) : contains(c.entryPoint, "glue") if c.name == "ingest_polaris"][0]
      && [for c in jsondecode(output.openmetadata_container_definitions) : length(c.secrets) if c.name == "init"][0] == 0
    )
    error_message = "Each ingestion step receives exactly the ingestion bot token parameter, and the init step receives no secret."
  }

  assert {
    condition = (
      toset([for c in jsondecode(output.openmetadata_container_definitions) : c.name if length([for variable in c.environment : variable.name if variable.value == ""]) > 0]) ==
      toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.metadata.containers))
    )
    error_message = "The persistent mode keeps the containers and the empty variables that the broker table names."
  }

  assert {
    condition     = output.openmetadata_secret_flow.violations == [] && output.openmetadata_secret_flow.mutation_mismatches == []
    error_message = "A secret reaches a file, an argument, the log or a dump in the persistent mode."
  }
}

run "persistent_server_needs_only_the_ingestion_image" {
  command = plan

  variables {
    openmetadata_enabled              = true
    openmetadata_server_url           = "http://10.0.1.20:8585"
    openmetadata_ingestion_token_name = "/example_project/openmetadata_bot_ingestion"
    openmetadata_images               = {}
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    }
  }

  expect_failures = [var.openmetadata_images]
}

run "refuses_a_public_or_other_port_server_address" {
  command = plan

  variables {
    openmetadata_server_url = "http://203.0.113.9:8585"
  }

  expect_failures = [var.openmetadata_server_url]
}

run "refuses_a_plain_name_other_than_the_project_token" {
  command = plan

  variables {
    openmetadata_enabled              = true
    openmetadata_server_url           = "http://10.0.1.20:8585"
    openmetadata_ingestion_token_name = "/example_project/openmetadata_admin_password"
    openmetadata_images = {
      ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
    }
    openmetadata_roles = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
    }
  }

  expect_failures = [var.openmetadata_ingestion_token_name]
}

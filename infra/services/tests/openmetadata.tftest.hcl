mock_provider "aws" {
  override_during = plan
}

# Persistent OpenMetadata and its OpenSearch node run as one host task behind openmetadata_persistent_enabled, with a one-shot bootstrap task. These cases pin the flag-off
# plan (every limit exactly as before), the flag-on shape, the secret flow, the loopback rules and the memory arithmetic on r7g.large with Marquez also running (the
# transition set). Every case reads the plan only: no image runs and nothing reaches AWS.
variables {
  aws_profile                     = "example_project_dev"
  environment                     = "dev"
  project_name                    = "example_project"
  project_tag                     = "example_project"
  aws_region                      = "us-west-2"
  expected_account_id             = "111111111111"
  cluster_name                    = "example-project-dev"
  ec2_capacity_provider_name      = "example-project-dev-host"
  execution_role_arn              = "arn:aws:iam::111111111111:role/example-execution"
  polaris_task_role_arn           = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn     = "arn:aws:iam::111111111111:role/example-control"
  openmetadata_task_role_arn      = "arn:aws:iam::111111111111:role/example-openmetadata"
  data_mount_path                 = "/mnt/example-data"
  marquez_enabled                 = true
  openmetadata_persistent_enabled = true

  log_group_names = {
    postgres = "/example/postgres"
    redis    = "/example/redis"
    polaris  = "/example/polaris"
    airflow  = "/example/airflow"
    marquez  = "/example/marquez"
  }

  ecr_repository_urls = {
    postgres               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    marquez                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez"
    marquez_web            = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez_web"
    openmetadata_server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_server"
    openmetadata_ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_ingestion"
    opensearch             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/opensearch"
  }

  image_digests = {
    postgres               = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis                  = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris                = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin          = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow                = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    marquez                = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    marquez_web            = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    openmetadata_server    = "sha256:5555555555555555555555555555555555555555555555555555555555555555"
    openmetadata_ingestion = "sha256:6666666666666666666666666666666666666666666666666666666666666666"
    opensearch             = "sha256:7777777777777777777777777777777777777777777777777777777777777777"
  }
}

run "flag_off_plans_nothing_new_and_keeps_every_limit" {
  command = plan

  variables {
    openmetadata_persistent_enabled = false
    openmetadata_task_role_arn      = null
  }

  assert {
    condition = (
      !contains(keys(aws_ecs_task_definition.service), "openmetadata") && !contains(keys(aws_ecs_service.service), "openmetadata")
      && !contains(keys(aws_ecs_task_definition.one_shot), "openmetadata_bootstrap")
    )
    error_message = "With the flag off the stack plans no OpenMetadata task, service or one-shot."
  }

  assert {
    condition = {
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : key => {
        for container in jsondecode(definition.container_definitions) : container.name => [container.memory, container.memoryReservation]
      }
      } == {
      airflow_apiserver     = { airflow_apiserver = [1536, 256], airflow_migrate = [640, 256] }
      airflow_dag_processor = { airflow_dag_processor = [1280, 192] }
      airflow_scheduler     = { airflow_scheduler = [1536, 320] }
      airflow_triggerer     = { airflow_triggerer = [768, 256] }
      host_prepare          = { host_prepare = [128, 64] }
      marquez               = { marquez_api = [1280, 768], marquez_web = [256, 128] }
      polaris               = { polaris = [2048, 320] }
      polaris_bootstrap     = { polaris_bootstrap = [1024, 512] }
      postgres              = { postgres = [2048, 128], postgres_databases = [128, 64] }
      redis                 = { redis = [1024, 64] }
    }
    error_message = "With the flag off every hard limit is the deployed one and every reservation is the measured one of the services stack (D19)."
  }

  assert {
    condition     = !anytrue([for name in output.parameter_names_used : strcontains(name, "openmetadata")])
    error_message = "With the flag off no task definition reads an OpenMetadata parameter."
  }

  assert {
    condition     = output.host_ports_reachable_from_jobs == { polaris = 8181, redis = 6379 }
    error_message = "With the flag off the job tasks reach only Polaris and Redis."
  }

  assert {
    condition     = !strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "openmetadata")
    error_message = "With the flag off the database script creates no OpenMetadata role or database."
  }

  assert {
    condition     = !contains(keys(output.task_definition_arns), "openmetadata") && !contains(keys(output.one_shot_run_task), "openmetadata_bootstrap")
    error_message = "With the flag off the outputs name no OpenMetadata task."
  }
}

run "flag_on_adds_one_service_and_one_bootstrap_task" {
  command = plan

  assert {
    condition     = contains(keys(aws_ecs_service.service), "openmetadata") && contains(keys(aws_ecs_task_definition.service), "openmetadata") && contains(keys(aws_ecs_task_definition.one_shot), "openmetadata_bootstrap")
    error_message = "The flag adds the openmetadata service, its task definition and the openmetadata_bootstrap one-shot."
  }

  assert {
    condition     = [for container in jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions) : container.name] == ["opensearch", "openmetadata_migrate", "openmetadata_server"]
    error_message = "The task runs OpenSearch first, then the migration, then the server."
  }

  assert {
    condition = (
      aws_ecs_task_definition.service["openmetadata"].network_mode == "host" && aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].network_mode == "host"
      && aws_ecs_task_definition.service["openmetadata"].runtime_platform[0].cpu_architecture == "ARM64"
      && aws_ecs_task_definition.service["openmetadata"].task_role_arn == var.control_plane_task_role_arn
      && aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].task_role_arn == var.openmetadata_task_role_arn
    )
    error_message = "Both tasks run on the ARM64 host in host network mode. The bootstrap has its own task role; the server and OpenSearch hold no AWS permission."
  }

  assert {
    condition = alltrue([
      for container in concat(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions), jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)) :
      can(regex("^111111111111\\.dkr\\.ecr\\.us-west-2\\.amazonaws\\.com/example_project/(openmetadata_server|openmetadata_ingestion|opensearch)@sha256:[0-9a-f]{64}$", container.image))
    ])
    error_message = "Every image runs by digest from the project registry."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/opensearch@${var.image_digests["opensearch"]}"
      && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[2].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_server@${var.image_digests["openmetadata_server"]}"
      && jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_ingestion@${var.image_digests["openmetadata_ingestion"]}"
    )
    error_message = "OpenSearch, the server and the bootstrap use their own repositories and digests."
  }

  assert {
    condition     = output.host_ports_reachable_from_jobs == { polaris = 8181, redis = 6379, openmetadata = 8585 }
    error_message = "The API opens one host port, 8585, to the job tasks, like Polaris on 8181."
  }

  assert {
    condition     = output.service_names["openmetadata"] == "example_project_dev_openmetadata"
    error_message = "The service name follows the project naming."
  }
}

run "start_order_and_health" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[1].dependsOn == [{ containerName = "opensearch", condition = "HEALTHY" }]
      && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[2].dependsOn == [{ containerName = "openmetadata_migrate", condition = "SUCCESS" }]
      && !jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[1].essential
      && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].essential
      && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[2].essential
    )
    error_message = "The migration waits for a healthy OpenSearch, the server for a finished migration, and the migration alone may exit."
  }

  assert {
    condition = alltrue([
      for index in [0, 2] : (
        jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[index].healthCheck.retries >= 5
        && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[index].healthCheck.startPeriod >= 60
        && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[index].restartPolicy.enabled
      )
    ])
    error_message = "OpenSearch and the server have a health check that waits for a slow JVM start, and a restart policy."
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].restartPolicy == { enabled = false }
    error_message = "The bootstrap runs once and is never restarted by ECS."
  }
}

run "search_is_loopback_with_a_fixed_heap_on_the_data_volume" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].environment, { name = "network.host", value = "127.0.0.1" })
      && contains(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].environment, { name = "discovery.type", value = "single-node" })
      && contains(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].environment, { name = "OPENSEARCH_JAVA_OPTS", value = "-Xms896m -Xmx896m" })
      && lookup(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0], "portMappings", []) == []
    )
    error_message = "OpenSearch is a single node bound to loopback, publishes no port and has the same minimum and maximum heap."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].mountPoints, { sourceVolume = "opensearch_data", containerPath = "/usr/share/opensearch/data", readOnly = false })
      && contains([for volume in aws_ecs_task_definition.service["openmetadata"].volume : volume.host_path], "/mnt/example-data/opensearch")
    )
    error_message = "The index lives on the persistent data volume of the host."
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].user == "1000:1000" && contains(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[0].ulimits, { name = "nofile", softLimit = 65536, hardLimit = 65536 })
    error_message = "OpenSearch runs as the image user and has the open-file limit it needs."
  }

  assert {
    condition     = strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/opensearch") && strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chown 1000:1000 /host/opensearch")
    error_message = "The host preparation creates the index folder for the OpenSearch user."
  }
}

run "server_uses_the_host_database_and_basic_sign_in" {
  command = plan

  assert {
    condition = alltrue([
      for pair in [
        { name = "DB_HOST", value = "127.0.0.1" },
        { name = "DB_PORT", value = "5432" },
        { name = "DB_USER", value = "openmetadata_user" },
        { name = "OM_DATABASE", value = "openmetadata_db" },
        { name = "SEARCH_TYPE", value = "opensearch" },
        { name = "ELASTICSEARCH_HOST", value = "127.0.0.1" },
        { name = "ELASTICSEARCH_PORT", value = "9200" },
        { name = "AUTHENTICATION_PROVIDER", value = "basic" },
        { name = "PIPELINE_SERVICE_CLIENT_ENABLED", value = "false" },
        { name = "OPENMETADATA_HEAP_OPTS", value = "-Xmx1024m -Xms1024m" },
      ] : contains(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[2].environment, pair)
    ])
    error_message = "The server uses a separate logical database of the host PostgreSQL, the loopback OpenSearch, built-in basic sign-in and no pipeline client (no profiling, sampling or duplicate data-quality runs from the server)."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[2].secrets == [{ name = "DB_USER_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/openmetadata_db_password" }]
      && jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[1].secrets == [{ name = "DB_USER_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/openmetadata_db_password" }]
    )
    error_message = "The database password reaches the migration and the server only as a Parameter Store secret."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions)[2].portMappings == [{ containerPort = 8585, hostPort = 8585, protocol = "tcp" }, { containerPort = 8586, hostPort = 8586, protocol = "tcp" }]
    )
    error_message = "The server publishes the API port 8585 and the admin port 8586 and nothing else; the security group opens only 8585 to the job tasks."
  }

  assert {
    condition     = !anytrue([for container in jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions) : can(regex("0\\.0\\.0\\.0|\\[::\\]", jsonencode(container)))])
    error_message = "No OpenMetadata container names a wildcard address."
  }
}

run "database_script_creates_the_logical_database_only_with_the_flag" {
  command = plan

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "\\getenv openmetadata_password OPENMETADATA_DB_PASSWORD")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE ROLE openmetadata_user LOGIN")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "ALTER ROLE openmetadata_user PASSWORD :'openmetadata_password'")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE DATABASE openmetadata_db OWNER openmetadata_user ENCODING ''UTF8''")
      && contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].secrets, { name = "OPENMETADATA_DB_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/openmetadata_db_password" })
    )
    error_message = "With the flag on, the database container creates the role and the logical database from the Parameter Store password, repeatably."
  }
}

run "bootstrap_reads_one_password_and_names_the_token_parameters" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].secrets == [{ name = "OM_ADMIN_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/openmetadata_admin_password" }]
      && contains(jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].environment, { name = "OM_BOT_PARAMETERS", value = jsonencode({ comparison = "/healthcare_hai_prediction/openmetadata_bot_comparison", ingestion = "/healthcare_hai_prediction/openmetadata_bot_ingestion", lineage = "/healthcare_hai_prediction/openmetadata_bot_lineage" }) })
    )
    error_message = "The bootstrap reads the administrator password as a secret and is told the parameter name of each caller's bot token. It receives no token value."
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].entryPoint[0] == "python" && strcontains(jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].entryPoint[1], "openmetadata_persistent_bootstrap.py")
    error_message = "The bootstrap runs the tracked bootstrap script from the mounted checkout."
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)[0].mountPoints == [{ sourceVolume = "code_repo", containerPath = "/opt/hai/repo", readOnly = true }]
    error_message = "The bootstrap sees the repository checkout read-only and nothing else of the host."
  }

  assert {
    condition = (
      !anytrue([for name in output.parameter_names_used : strcontains(name, "openmetadata_bot")])
    )
    error_message = "No task definition reads a bot token: the bootstrap only writes them, so the execution role grants none."
  }

  assert {
    condition     = alltrue([for name in ["/healthcare_hai_prediction/openmetadata_admin_password", "/healthcare_hai_prediction/openmetadata_db_password"] : contains(output.parameter_names_used, name)])
    error_message = "The execution role grant covers the administrator and database passwords."
  }
}

run "secrets_reach_no_file_argument_log_or_dump" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.start_order_and_health.task_definition_settings
    definitions = run.start_order_and_health.container_definitions
  }

  assert {
    condition     = output.violations == [] && alltrue([for name in ["openmetadata/openmetadata_server", "openmetadata/openmetadata_migrate", "openmetadata_bootstrap/openmetadata_bootstrap"] : contains(output.containers_checked, name)])
    error_message = "A secret reaches a file, an argument, the log or a dump in the new containers: ${jsonencode(output.violations)}"
  }
}

run "memory_fits_r7g_xlarge_with_marquez_running" {
  command = plan

  assert {
    condition     = output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "The reservations, OpenMetadata and Marquez included, leave the reserve for the operating system and the later services."
  }

  assert {
    condition = sum(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [for container in jsondecode(definition.container_definitions) : container.memory]
    ])) <= var.host_memory_mib - 2048
    error_message = "If every container, the one-shot tasks included, reached its hard limit, the host would still keep 2 GiB for the operating system and the ECS agent."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : container.memory >= container.memoryReservation && container.memoryReservation > 0
      ]
    ]))
    error_message = "Every container needs a reservation and a hard limit at least as large."
  }

  assert {
    condition = (
      output.reserved_memory_mib == sum(flatten([for key, definition in aws_ecs_task_definition.service : [for container in jsondecode(definition.container_definitions) : container.memoryReservation]]))
      && output.reserved_memory_mib == 5280
    )
    error_message = "Recorded figure: with Marquez and OpenMetadata the always-on reservations total 5280 MiB. Current: ${output.reserved_memory_mib}"
  }

  assert {
    condition = sum(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [for container in jsondecode(definition.container_definitions) : container.memory]
    ])) == 17600
    error_message = "Recorded figure: with Marquez and OpenMetadata the hard limits total 17600 MiB of the 30720 MiB allowed on r7g.xlarge."
  }
}

run "measured_services_keep_their_measured_reservations_with_the_flag_on" {
  command = plan

  assert {
    condition = {
      for key, definition in aws_ecs_task_definition.service : key => {
        for container in jsondecode(definition.container_definitions) : container.name => [container.memory, container.memoryReservation]
        if contains(["postgres", "redis", "polaris", "airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"], container.name)
      }
      if contains(["postgres", "redis", "polaris", "airflow_apiserver", "airflow_dag_processor", "airflow_scheduler", "airflow_triggerer"], key)
      } == {
      airflow_apiserver     = { airflow_apiserver = [1536, 256] }
      airflow_dag_processor = { airflow_dag_processor = [1280, 192] }
      airflow_scheduler     = { airflow_scheduler = [1536, 320] }
      airflow_triggerer     = { airflow_triggerer = [768, 256] }
      polaris               = { polaris = [2048, 320] }
      postgres              = { postgres = [2048, 128] }
      redis                 = { redis = [1024, 64] }
    }
    error_message = "With the flag on the seven measured services keep their measured reservations and their deployed limits (D19)."
  }
}

run "heaps_stay_inside_their_limits" {
  command = plan

  assert {
    condition = (
      var.openmetadata_memory_mib.search.heap_mib * 2 <= var.openmetadata_memory_mib.search.limit
      && var.openmetadata_memory_mib.server.heap_mib * 100 <= var.openmetadata_memory_mib.server.limit * 60
    )
    error_message = "The OpenSearch heap is at most half its container limit, the server heap at most 60 percent of its limit."
  }
}

run "search_heap_over_half_the_limit_is_refused" {
  command = plan

  variables {
    openmetadata_memory_mib = {
      server    = { limit = 1792, reservation = 1280, heap_mib = 1024 }
      search    = { limit = 1792, reservation = 1280, heap_mib = 1024 }
      migrate   = { limit = 768, reservation = 256 }
      bootstrap = { limit = 512, reservation = 256 }
    }
  }

  expect_failures = [var.openmetadata_memory_mib]
}

run "reservation_above_limit_is_refused" {
  command = plan

  variables {
    openmetadata_memory_mib = {
      server    = { limit = 1792, reservation = 2048, heap_mib = 1024 }
      search    = { limit = 1792, reservation = 1280, heap_mib = 896 }
      migrate   = { limit = 768, reservation = 256 }
      bootstrap = { limit = 512, reservation = 256 }
    }
  }

  expect_failures = [var.openmetadata_memory_mib]
}

run "limits_that_exceed_the_host_are_refused" {
  command = plan

  variables {
    host_memory_mib = 16384
    openmetadata_memory_mib = {
      server    = { limit = 4096, reservation = 1280, heap_mib = 2048 }
      search    = { limit = 4096, reservation = 1280, heap_mib = 2048 }
      migrate   = { limit = 768, reservation = 256 }
      bootstrap = { limit = 512, reservation = 256 }
    }
  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "r7g_xlarge_has_room_for_the_larger_limits" {
  command = plan

  variables {
    host_memory_mib = 32768
    openmetadata_memory_mib = {
      server    = { limit = 4096, reservation = 2048, heap_mib = 2048 }
      search    = { limit = 4096, reservation = 2048, heap_mib = 2048 }
      migrate   = { limit = 768, reservation = 256 }
      bootstrap = { limit = 512, reservation = 256 }
    }
  }

  assert {
    condition     = output.reserved_memory_mib <= 32768 - var.other_services_reserve_mib
    error_message = "The resized host accepts the larger limits."
  }
}

run "missing_role_image_or_digest_is_refused" {
  command = plan

  variables {
    openmetadata_task_role_arn = null
  }

  expect_failures = [var.openmetadata_task_role_arn]
}

run "shared_bootstrap_role_is_refused" {
  command = plan

  variables {
    openmetadata_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  }

  expect_failures = [var.openmetadata_task_role_arn]
}

run "missing_openmetadata_digest_is_refused" {
  command = plan

  variables {
    image_digests = {
      postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      marquez       = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      marquez_web   = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }
  }

  expect_failures = [var.image_digests]
}

run "bot_token_name_with_a_dash_is_refused" {
  command = plan

  variables {
    openmetadata_bot_token_names = {
      lineage    = "/healthcare_hai_prediction/openmetadata-bot-lineage"
      ingestion  = "/healthcare_hai_prediction/openmetadata_bot_ingestion"
      comparison = "/healthcare_hai_prediction/openmetadata_bot_comparison"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

run "two_callers_sharing_one_token_parameter_are_refused" {
  command = plan

  variables {
    openmetadata_bot_token_names = {
      lineage    = "/healthcare_hai_prediction/openmetadata_bot_lineage"
      ingestion  = "/healthcare_hai_prediction/openmetadata_bot_lineage"
      comparison = "/healthcare_hai_prediction/openmetadata_bot_comparison"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

run "token_parameter_equal_to_the_admin_password_is_refused" {
  command = plan

  variables {
    openmetadata_bot_token_names = {
      lineage    = "/healthcare_hai_prediction/openmetadata_admin_password"
      ingestion  = "/healthcare_hai_prediction/openmetadata_bot_ingestion"
      comparison = "/healthcare_hai_prediction/openmetadata_bot_comparison"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

run "every_resource_carries_the_project_tag" {
  command = plan

  assert {
    condition = (
      aws_ecs_service.service["openmetadata"].tags["Project"] == "example_project"
      && aws_ecs_task_definition.service["openmetadata"].tags["Project"] == "example_project"
      && aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].tags["Project"] == "example_project"
      && aws_ecs_service.service["openmetadata"].propagate_tags == "TASK_DEFINITION"
    )
    error_message = "The new service and task definitions carry the Project tag and propagate it to their tasks."
  }

  assert {
    condition     = output.one_shot_run_task["openmetadata_bootstrap"].tags == { Project = "example_project" } && output.one_shot_run_task["openmetadata_bootstrap"].propagate_tags == "TASK_DEFINITION"
    error_message = "The manual run-task arguments of the bootstrap carry the Project tag."
  }
}

run "logs_go_to_a_foundation_group" {
  command = plan

  assert {
    condition = alltrue([
      for container in concat(jsondecode(aws_ecs_task_definition.service["openmetadata"].container_definitions), jsondecode(aws_ecs_task_definition.one_shot["openmetadata_bootstrap"].container_definitions)) :
      container.logConfiguration.options["awslogs-group"] == "/example/marquez" && container.logConfiguration.options["awslogs-stream-prefix"] == container.name
    ])
    error_message = "The new containers log to the existing lineage group under their own stream prefix."
  }
}

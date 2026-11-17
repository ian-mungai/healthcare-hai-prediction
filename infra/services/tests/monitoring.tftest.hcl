mock_provider "aws" {
  override_during = plan
}

# Prometheus, Grafana and the StatsD exporter run as one task behind monitoring_enabled. These cases turn the flag on and pin the flag-off plan in the first case. Every case reads the plan only: no image runs and nothing reaches AWS.
variables {
  # TEST FIXTURE ONLY, not a measured value: the host job cap (host_job_cap_mib) these plans use. The default is the room the live stack leaves, which the
  # larger fixtures here do not have.
  host_job_cap_mib            = 4096
  aws_profile                 = "example_project_dev"
  environment                 = "dev"
  project_name                = "example_project"
  project_tag                 = "example_project"
  aws_region                  = "us-west-2"
  expected_account_id         = "111111111111"
  cluster_name                = "example-project-dev"
  ec2_capacity_provider_name  = "example-project-dev-host"
  execution_role_arn          = "arn:aws:iam::111111111111:role/example-execution"
  polaris_task_role_arn       = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  data_mount_path             = "/mnt/example-data"
  monitoring_enabled          = true
  alerts_topic_arn            = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"

  # The committed defaults of the services stack are the live ones (MLflow, model API, page, monitoring, OpenMetadata and HTTP lineage on). Every case here
  # that does not set a switch tests it off, so the fixture sets each one explicitly.
  mlflow_enabled                  = false
  model_api_enabled               = false
  streamlit_enabled               = false
  openmetadata_persistent_enabled = false
  airflow_lineage_http_enabled    = false
  model_api_mlflow_uri            = ""

  log_group_names = {
    postgres     = "/example/postgres"
    redis        = "/example/redis"
    polaris      = "/example/polaris"
    airflow      = "/example/airflow"
    openmetadata = "/example/openmetadata"
    monitoring   = "/example/monitoring"
  }

  ecr_repository_urls = {
    postgres          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    prometheus        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
    grafana           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
    statsd_exporter   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
    pushgateway       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/pushgateway"
    redis_exporter    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis_exporter"
    postgres_exporter = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres_exporter"
    node_exporter     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/node_exporter"
  }

  image_digests = {
    postgres          = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis             = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris           = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow           = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    prometheus        = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    grafana           = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    statsd_exporter   = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    pushgateway       = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    redis_exporter    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    postgres_exporter = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    node_exporter     = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
}

run "flag_off_plans_no_monitoring_and_no_statsd" {
  command = plan

  variables {
    monitoring_enabled = false
    log_group_names = {
      postgres     = "/example/postgres"
      redis        = "/example/redis"
      polaris      = "/example/polaris"
      airflow      = "/example/airflow"
      openmetadata = "/example/openmetadata"
    }
    ecr_repository_urls = {
      postgres      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
      redis         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
      polaris       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
      polaris_admin = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
      airflow       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    }
    image_digests = {
      postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    }
  }

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.service), "monitoring") && !contains(keys(aws_ecs_service.service), "monitoring")
    error_message = "With monitoring_enabled false the stack plans no monitoring task or service."
  }

  assert {
    condition = !anytrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [for entry in lookup(container, "environment", []) : strcontains(entry.name, "STATSD")]
      ]
    ]))
    error_message = "With monitoring_enabled false no Airflow container gets a StatsD setting, so the Airflow task definitions stay as they are."
  }

  assert {
    condition     = !contains(output.parameter_names_used, "/healthcare_hai_prediction/grafana_admin_password")
    error_message = "With monitoring_enabled false no task definition reads the Grafana password."
  }
}

run "flag_on_adds_one_service_with_six_containers" {
  command = plan

  assert {
    condition     = contains(keys(aws_ecs_service.service), "monitoring") && contains(keys(aws_ecs_task_definition.service), "monitoring")
    error_message = "monitoring_enabled adds the monitoring service and its task definition."
  }

  assert {
    condition     = [for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.name] == ["monitoring_prepare", "prometheus", "grafana", "statsd_exporter", "pushgateway", "host_state_exporter", "redis_exporter", "postgres_exporter", "node_exporter"]
    error_message = "The monitoring task is the preparation container, Prometheus, Grafana, the StatsD exporter, the Pushgateway and the host exporter, in that order."
  }

  assert {
    condition     = aws_ecs_task_definition.service["monitoring"].task_role_arn == var.control_plane_task_role_arn && aws_ecs_task_definition.service["monitoring"].network_mode == "host"
    error_message = "The monitoring task holds the control plane role, which has no data permission, and shares the host network."
  }
}

run "every_monitoring_listener_binds_loopback" {
  command = plan

  assert {
    condition = sort(flatten([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : [for mapping in lookup(container, "portMappings", []) : "${mapping.hostPort}/${mapping.protocol}"]
    ])) == sort(["3001/tcp", "9090/tcp", "9091/tcp", "9100/tcp", "9102/tcp", "9103/tcp", "9121/tcp", "9125/udp", "9187/tcp"])
    error_message = "The monitoring ports are Grafana 3001, Prometheus 9090, the Pushgateway 9091, the StatsD exporter 9102, the host exporter 9103 and the StatsD UDP port 9125 and nothing else."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--web.listen-address=127.0.0.1:9090")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[3].command, "--web.listen-address=127.0.0.1:9102")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[3].command, "--statsd.listen-udp=127.0.0.1:9125")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[3].command, "--statsd.listen-tcp=")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[3].command, "--statsd.listen-unixgram=")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[2].environment, { name = "GF_SERVER_HTTP_ADDR", value = "127.0.0.1" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[2].environment, { name = "GF_SERVER_HTTP_PORT", value = "3001" })
    )
    error_message = "Prometheus, the exporter (web and UDP) and Grafana bind 127.0.0.1, and the exporter's TCP and Unix datagram listeners are off."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      container.name == "pushgateway" || !can(regex("0\\.0\\.0\\.0|\\[::\\]", jsonencode(container)))
    ])
    error_message = "No monitoring container but the Pushgateway names a wildcard address."
  }

  assert {
    condition     = contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[4].command, "--web.listen-address=0.0.0.0:9091")
    error_message = "The Pushgateway listens on all interfaces so a bridge job reaches it at the host private address; the job firewall and the closed security group bound who can."
  }

  assert {
    condition     = contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].command, "127.0.0.1:9103")
    error_message = "The host exporter listens on loopback."
  }

  assert {
    condition     = output.host_ports_reachable_from_jobs == { polaris = 8181, redis = 6379, pushgateway = 9091 }
    error_message = "Of the monitoring ports only the Pushgateway's 9091 is on the list of ports the job tasks may reach."
  }
}

run "monitoring_images_run_by_digest_from_ecr" {
  command = plan

  assert {
    condition = [for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.image] == [
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus@sha256:2222222222222222222222222222222222222222222222222222222222222222",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus@sha256:2222222222222222222222222222222222222222222222222222222222222222",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana@sha256:3333333333333333333333333333333333333333333333333333333333333333",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter@sha256:4444444444444444444444444444444444444444444444444444444444444444",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/pushgateway@sha256:9999999999999999999999999999999999999999999999999999999999999999",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis_exporter@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres_exporter@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/node_exporter@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    ]
    error_message = "Each monitoring container runs its image from the project ECR repository by digest. The preparation container reuses the Prometheus image and the host exporter reuses the Airflow image."
  }
}

run "tag_instead_of_digest_for_grafana_is_refused" {
  command = plan

  variables {
    image_digests = {
      postgres          = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis             = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris           = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow           = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      prometheus        = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
      grafana           = "13.2.5-ubuntu"
      statsd_exporter   = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
      pushgateway       = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
      redis_exporter    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      postgres_exporter = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      node_exporter     = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  }

  expect_failures = [var.image_digests]
}

run "missing_monitoring_digest_is_refused" {
  command = plan

  variables {
    image_digests = {
      postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    }
  }

  expect_failures = [var.image_digests]
}

run "missing_monitoring_repository_is_refused" {
  command = plan

  variables {
    ecr_repository_urls = {
      postgres      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
      redis         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
      polaris       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
      polaris_admin = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
      airflow       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    }
  }

  expect_failures = [var.ecr_repository_urls]
}

run "missing_monitoring_log_group_is_refused" {
  command = plan

  variables {
    log_group_names = {
      postgres     = "/example/postgres"
      redis        = "/example/redis"
      polaris      = "/example/polaris"
      airflow      = "/example/airflow"
      openmetadata = "/example/openmetadata"
    }
  }

  expect_failures = [var.log_group_names]
}

run "monitoring_logs_go_to_the_monitoring_group" {
  command = plan

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      container.logConfiguration.options["awslogs-group"] == "/example/monitoring" && container.logConfiguration.options["awslogs-stream-prefix"] == container.name && container.logConfiguration.options["mode"] == "non-blocking"
    ])
    error_message = "Every monitoring container logs without blocking to the monitoring group with its own name as stream prefix."
  }
}

run "grafana_password_comes_from_the_secret_store" {
  command = plan

  assert {
    condition = (
      [for secret in one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "grafana"]).secrets : secret.name] == ["GF_SECURITY_ADMIN_PASSWORD"]
      && one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "grafana"]).secrets[0].valueFrom == "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/grafana_admin_password"
    )
    error_message = "Grafana receives its administrator password only as a Parameter Store secret reference."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      contains(["grafana", "redis_exporter", "postgres_exporter"], container.name) || length(lookup(container, "secrets", [])) == 0
    ])
    error_message = "Only Grafana and the Redis and Postgres exporters receive a secret."
  }

  assert {
    condition = !anytrue(flatten([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : [
        for entry in lookup(container, "environment", []) : can(regex("(?i)password|secret|token|key", entry.name))
      ]
    ]))
    error_message = "No environment literal is a password, secret, token or key."
  }

  assert {
    condition     = contains(output.parameter_names_used, "/healthcare_hai_prediction/grafana_admin_password") && contains(output.parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/grafana_admin_password")
    error_message = "The execution role grant covers the Grafana password parameter when monitoring is on."
  }
}

run "monitoring_definitions_for_review" {
  command = plan
}

run "grafana_secret_reaches_no_file_argument_log_or_dump" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.monitoring_definitions_for_review.task_definition_settings
    definitions = run.monitoring_definitions_for_review.container_definitions
  }

  assert {
    condition     = output.violations == []
    error_message = "A line lets the Grafana password reach a file, a command argument, the log or the environment dump."
  }

  assert {
    condition     = contains(output.containers_checked, "monitoring/grafana")
    error_message = "The secret flow check must follow the Grafana container."
  }
}

run "memory_limits_are_small_and_stated" {
  command = plan

  assert {
    condition = {
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.name => "${container.memoryReservation}/${container.memory}"
    } == { monitoring_prepare = "16/32", prometheus = "128/256", grafana = "128/256", statsd_exporter = "32/64", pushgateway = "32/96", host_state_exporter = "56/128", redis_exporter = "24/64", postgres_exporter = "24/64", node_exporter = "24/64" }
    error_message = "The default monitoring memory reserves 464 MiB (owner decisions of Nov 17): Prometheus 128, Grafana 128, the StatsD exporter 32, the Pushgateway 32, the host state exporter 56, the three exporters 24 each and the preparation container 16."
  }

  assert {
    condition     = output.monitoring.memory_mib == { grafana = { limit = 256, reservation = 128 }, host_state_exporter = { limit = 128, reservation = 56 }, node_exporter = { limit = 64, reservation = 24 }, postgres_exporter = { limit = 64, reservation = 24 }, prometheus = { limit = 256, reservation = 128 }, pushgateway = { limit = 96, reservation = 32 }, redis_exporter = { limit = 64, reservation = 24 }, statsd_exporter = { limit = 64, reservation = 32 } }
    error_message = "The monitoring output states the limits, so a measurement can be compared with them."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      contains(["monitoring_prepare", "host_state_exporter"], container.name) || can(regex("^[0-9]+MiB$", one([for entry in container.environment : entry.value if entry.name == "GOMEMLIMIT"])))
    ])
    error_message = "Each Go container gets a soft memory limit (GOMEMLIMIT), so the runtime collects garbage before the container limit. The host exporter is Python and has none."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      contains(["monitoring_prepare", "host_state_exporter"], container.name) || tonumber(trimsuffix(one([for entry in container.environment : entry.value if entry.name == "GOMEMLIMIT"]), "MiB")) < container.memory
    ])
    error_message = "The soft limit lies below the hard limit."
  }
}

run "monitoring_fits_on_the_default_host" {
  command = plan

  assert {
    condition     = output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "Reservations with monitoring on must stay within the budget."
  }

  assert {
    condition = sum(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [for container in jsondecode(definition.container_definitions) : container.memory]
    ])) <= var.host_memory_mib - 2048
    error_message = "Hard limits with monitoring on must leave 2 GiB for the operating system."
  }
}

run "limits_change_in_one_place" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus          = { limit = 512, reservation = 256 }
      grafana             = { limit = 384, reservation = 192 }
      statsd_exporter     = { limit = 64, reservation = 32 }
      pushgateway         = { limit = 96, reservation = 32 }
      host_state_exporter = { limit = 128, reservation = 56 }
      redis_exporter      = { limit = 64, reservation = 24 }
      postgres_exporter   = { limit = 64, reservation = 24 }
      node_exporter       = { limit = 64, reservation = 24 }
    }
    ecr_repository_urls = {
      postgres          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
      redis             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
      polaris           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
      polaris_admin     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
      airflow           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
      prometheus        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
      grafana           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
      statsd_exporter   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
      pushgateway       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/pushgateway"
      redis_exporter    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis_exporter"
      postgres_exporter = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres_exporter"
      node_exporter     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/node_exporter"
    }
    image_digests = {
      postgres          = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis             = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris           = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow           = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      prometheus        = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
      grafana           = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
      statsd_exporter   = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
      pushgateway       = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
      redis_exporter    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      postgres_exporter = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      node_exporter     = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  }

  assert {
    condition = {
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.name => "${container.memoryReservation}/${container.memory}"
    } == { monitoring_prepare = "16/32", prometheus = "256/512", grafana = "192/384", statsd_exporter = "32/64", pushgateway = "32/96", host_state_exporter = "56/128", redis_exporter = "24/64", postgres_exporter = "24/64", node_exporter = "24/64" }
    error_message = "monitoring_memory_mib alone sets the monitoring limits and reservations."
  }

  assert {
    condition     = contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].environment, { name = "GOMEMLIMIT", value = "409MiB" })
    error_message = "The Prometheus soft limit follows its hard limit at 80 percent."
  }
}

run "reservation_above_limit_is_refused" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus          = { limit = 128, reservation = 256 }
      grafana             = { limit = 256, reservation = 128 }
      statsd_exporter     = { limit = 64, reservation = 32 }
      pushgateway         = { limit = 96, reservation = 32 }
      host_state_exporter = { limit = 128, reservation = 56 }
      redis_exporter      = { limit = 64, reservation = 24 }
      postgres_exporter   = { limit = 64, reservation = 24 }
      node_exporter       = { limit = 64, reservation = 24 }
    }
  }

  expect_failures = [var.monitoring_memory_mib]
}

run "limit_below_the_working_floor_is_refused" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus          = { limit = 64, reservation = 32 }
      grafana             = { limit = 256, reservation = 128 }
      statsd_exporter     = { limit = 64, reservation = 32 }
      pushgateway         = { limit = 96, reservation = 32 }
      host_state_exporter = { limit = 128, reservation = 56 }
      redis_exporter      = { limit = 64, reservation = 24 }
      postgres_exporter   = { limit = 64, reservation = 24 }
      node_exporter       = { limit = 64, reservation = 24 }
    }
  }

  expect_failures = [var.monitoring_memory_mib]
}

run "monitoring_that_overruns_the_host_is_refused" {
  command = plan

  variables {
    host_memory_mib        = 16384 # the r7g.large size this refusal is about
    host_usable_memory_mib = 16384 # at most the smaller host; the 32768 MiB default is the r7g.xlarge size
    monitoring_memory_mib = {
      prometheus          = { limit = 1024, reservation = 512 }
      grafana             = { limit = 1024, reservation = 512 }
      statsd_exporter     = { limit = 64, reservation = 32 }
      pushgateway         = { limit = 96, reservation = 32 }
      host_state_exporter = { limit = 128, reservation = 56 }
      redis_exporter      = { limit = 64, reservation = 24 }
      postgres_exporter   = { limit = 64, reservation = 24 }
      node_exporter       = { limit = 64, reservation = 24 }
    }
  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "retention_is_bounded_by_time_and_size" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.retention.time=15d")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.retention.size=2048MB")
    )
    error_message = "Prometheus keeps 15 days and at most 2048 MB of blocks and write-ahead log; the older data goes first."
  }
}

run "retention_follows_its_variables" {
  command = plan

  variables {
    prometheus_retention_days    = 7
    prometheus_retention_size_mb = 1024
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.retention.time=7d")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.retention.size=1024MB")
    )
    error_message = "The retention flags follow prometheus_retention_days and prometheus_retention_size_mb."
  }
}

run "zero_retention_days_is_refused" {
  command = plan

  variables {
    prometheus_retention_days = 0
  }

  expect_failures = [var.prometheus_retention_days]
}

run "unbounded_retention_size_is_refused" {
  command = plan

  variables {
    prometheus_retention_size_mb = 0
  }

  expect_failures = [var.prometheus_retention_size_mb]
}

run "years_of_retention_are_refused" {
  command = plan

  variables {
    prometheus_retention_days = 400
  }

  expect_failures = [var.prometheus_retention_days]
}

run "airflow_sends_statsd_to_the_loopback_exporter" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : [
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) : [
          for pair in [
            { name = "AIRFLOW__METRICS__STATSD_ON", value = "true" },
            { name = "AIRFLOW__METRICS__STATSD_HOST", value = "127.0.0.1" },
            { name = "AIRFLOW__METRICS__STATSD_PORT", value = "9125" },
            { name = "AIRFLOW__METRICS__STATSD_PREFIX", value = "airflow" },
          ] : contains(container.environment, pair)
        ]
      ]
    ]))
    error_message = "Every Airflow container sends StatsD metrics to 127.0.0.1:9125 with the prefix airflow."
  }
}

run "monitoring_containers_are_not_root_and_restart" {
  command = plan

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      container.name == "monitoring_prepare" || (can(regex("^[0-9]+(:[0-9]+)?$", container.user)) && tonumber(split(":", container.user)[0]) != 0)
    ])
    error_message = "Prometheus, Grafana and the exporter run as a numeric non-root user."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).user == "0"
      && !one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).essential
      && one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).readonlyRootFilesystem
      && one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).linuxParameters.capabilities.drop == ["ALL"]
      && toset(one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).linuxParameters.capabilities.add) == toset(["CHOWN", "DAC_OVERRIDE", "FOWNER"])
    )
    error_message = "Only the preparation container runs as root: not essential, a read-only root file system and exactly the capabilities CHOWN, DAC_OVERRIDE and FOWNER."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      !container.essential || (
        length(container.healthCheck.command) > 1 && container.healthCheck.retries >= 5 && container.healthCheck.startPeriod >= 15 && lookup(container.restartPolicy, "enabled", false)
        && container.healthCheck.retries <= 10 && container.healthCheck.interval >= 5 && container.healthCheck.timeout >= 2 && container.healthCheck.timeout <= 60 && container.healthCheck.startPeriod <= 300
      )
    ])
    error_message = "Every essential monitoring container has a health check inside the ECS ranges and a restart policy."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].dependsOn == [{ containerName = "monitoring_prepare", condition = "SUCCESS" }]
      && jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[2].dependsOn == [{ containerName = "monitoring_prepare", condition = "SUCCESS" }]
    )
    error_message = "Prometheus and Grafana start after the preparation container hands them their data directories."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      contains(lookup(container, "ulimits", []), { name = "core", softLimit = 0, hardLimit = 0 }) && contains(container.dockerSecurityOptions, "no-new-privileges") && !container.privileged
    ])
    error_message = "No monitoring container is privileged, each blocks new privileges and writes no core file."
  }
}

run "state_and_configuration_mounts" {
  command = plan

  assert {
    condition = {
      for volume in aws_ecs_task_definition.service["monitoring"].volume : volume.name => volume.host_path
      } == {
      code_monitoring_grafana_dashboards   = "/mnt/example-data/repo/monitoring/grafana/dashboards"
      code_monitoring_grafana_provisioning = "/mnt/example-data/repo/monitoring/grafana/provisioning"
      code_monitoring_package              = "/mnt/example-data/repo/monitoring"
      code_monitoring_prometheus           = "/mnt/example-data/repo/monitoring/prometheus"
      code_monitoring_statsd               = "/mnt/example-data/repo/monitoring/statsd"
      grafana_data                         = "/mnt/example-data/grafana"
      host_data                            = "/mnt/example-data"
      node_data                            = "/mnt/example-data"
      node_scratch                         = "/mnt/example-data/job_scratch"
      prometheus_data                      = "/mnt/example-data/prometheus"
      pushgateway_data                     = "/mnt/example-data/pushgateway"
    }
    error_message = "Configuration comes from the repository checkout below the data mount and the three state folders lie on the data volume."
  }

  assert {
    condition = alltrue(flatten([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : [
        for mount in lookup(container, "mountPoints", []) : mount.readOnly if startswith(mount.sourceVolume, "code_")
      ]
    ]))
    error_message = "Every configuration mount is read-only."
  }

  assert {
    condition = (
      strcontains(one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).command[0], "chown 65534:65534 /prometheus")
      && strcontains(one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).command[0], "chown 472:0 /grafana")
      && strcontains(one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).command[0], "chown 65534:65534 /pushgateway")
    )
    error_message = "The preparation container hands the Prometheus and Pushgateway folders to 65534 and the Grafana folder to 472."
  }
}

run "code_path_moves_the_monitoring_mounts" {
  command = plan

  variables {
    dag_code_path = "/opt/hai/checkout"
  }

  assert {
    condition     = contains([for volume in aws_ecs_task_definition.service["monitoring"].volume : volume.host_path], "/opt/hai/checkout/monitoring/prometheus")
    error_message = "The monitoring mounts follow dag_code_path."
  }
}

run "grafana_password_name_must_be_a_plain_parameter_path" {
  command = plan

  variables {
    grafana_admin_password_name = "/aws/grafana_admin_password"
  }

  expect_failures = [var.grafana_admin_password_name]
}

run "grafana_gets_the_alerts_topic_and_the_region_for_its_sns_contact_point" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[2].environment, { name = "ALERTS_TOPIC_ARN", value = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[2].environment, { name = "AWS_REGION", value = "us-west-2" })
    )
    error_message = "The Grafana container carries the alerts topic ARN and the region that provisioning/alerting/contact_points.yml reads, and no access key or secret."
  }

  assert {
    condition     = !can(regex("AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN", aws_ecs_task_definition.service["monitoring"].container_definitions))
    error_message = "No monitoring container carries a static AWS credential: Grafana signs with the task role through the default credential chain."
  }
}

run "monitoring_without_the_alerts_topic_is_refused" {
  command = plan

  variables {
    alerts_topic_arn = null
  }

  expect_failures = [var.alerts_topic_arn]
}

run "a_topic_arn_with_a_wildcard_is_refused" {
  command = plan

  variables {
    alerts_topic_arn = "arn:aws:sns:us-west-2:111111111111:*"
  }

  expect_failures = [var.alerts_topic_arn]
}

run "the_pushgateway_keeps_its_last_values_on_the_data_volume" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[4].command, "--persistence.file=/pushgateway/pushgateway.db")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[4].command, "--persistence.interval=1m")
      && jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[4].mountPoints == [{ sourceVolume = "pushgateway_data", containerPath = "/pushgateway", readOnly = false }]
    )
    error_message = "The Pushgateway writes its persistence file on the data volume, so a service restart or a host stop and start keeps the last value of every job."
  }

  assert {
    condition     = !can(regex("web\\.enable-admin-api|web\\.enable-lifecycle", aws_ecs_task_definition.service["monitoring"].container_definitions))
    error_message = "The Pushgateway admin API and lifecycle endpoints stay off."
  }
}

run "the_host_exporter_gets_the_cluster_the_region_and_the_job_cap_and_no_credential" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "ECS_CLUSTER", value = "example-project-dev" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "AWS_REGION", value = "us-west-2" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "HAI_LAUNCH_HOST_JOB_CAP_MIB", value = tostring(var.host_job_cap_mib) })
    )
    error_message = "The host exporter reads the cluster, the region and the host job cap that the launch preflight reads, from the stack's own values."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].readonlyRootFilesystem
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].mountPoints, { sourceVolume = "code_monitoring_package", containerPath = "/opt/hai/monitoring", readOnly = true })
      && jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].linuxParameters.capabilities.drop == ["ALL"]
    )
    error_message = "The host exporter has a read-only root file system, a read-only code mount and no capability."
  }
}

run "the_pushgateway_limit_below_its_floor_is_refused" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus          = { limit = 256, reservation = 128 }
      grafana             = { limit = 256, reservation = 128 }
      statsd_exporter     = { limit = 64, reservation = 32 }
      pushgateway         = { limit = 16, reservation = 8 }
      host_state_exporter = { limit = 128, reservation = 56 }
      redis_exporter      = { limit = 64, reservation = 24 }
      postgres_exporter   = { limit = 64, reservation = 24 }
      node_exporter       = { limit = 64, reservation = 24 }
    }
  }

  expect_failures = [var.monitoring_memory_mib]
}

run "the_host_exporter_limit_below_its_floor_is_refused" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus          = { limit = 256, reservation = 128 }
      grafana             = { limit = 256, reservation = 128 }
      statsd_exporter     = { limit = 64, reservation = 32 }
      pushgateway         = { limit = 96, reservation = 32 }
      host_state_exporter = { limit = 32, reservation = 16 }
      redis_exporter      = { limit = 64, reservation = 24 }
      postgres_exporter   = { limit = 64, reservation = 24 }
      node_exporter       = { limit = 64, reservation = 24 }
    }
  }

  expect_failures = [var.monitoring_memory_mib]
}

run "a_missing_pushgateway_digest_is_refused" {
  command = plan

  variables {
    image_digests = {
      postgres        = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis           = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris         = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin   = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow         = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      marquez         = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      marquez_web     = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
      prometheus      = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
      grafana         = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
      statsd_exporter = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    }
  }

  expect_failures = [var.image_digests]
}

run "the_host_state_exporter_sees_the_data_and_scratch_volumes_read_only_and_knows_the_service_prefix" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "ECS_SERVICE_PREFIX", value = "example_project_dev_" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "HOST_DATA_PATH", value = "/host/data" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "HOST_SCRATCH_PATH", value = "/host/data/job_scratch" })
    )
    error_message = "The host state exporter gets the prefix of the ECS service names and the two folders whose volumes it measures."
  }

  assert {
    condition     = contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].mountPoints, { sourceVolume = "host_data", containerPath = "/host/data", readOnly = true })
    error_message = "The data mount is read-only in the exporter: it reads the size of the volumes below it (the scratch volume is mounted inside it) and writes nothing."
  }

  assert {
    condition     = length([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if length([for mount in lookup(container, "mountPoints", []) : mount if mount.sourceVolume == "host_data"]) > 0]) == 1
    error_message = "Only the host state exporter mounts the data volume."
  }
}

run "the_three_exporters_are_optional_restart_small_and_unprivileged" {
  command = plan

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      !container.essential && container.restartPolicy.enabled && container.memory == 64 && container.memoryReservation == 24 && !container.privileged
      && container.user == "65534:65534" && container.readonlyRootFilesystem && contains(container.environment, { name = "GOMEMLIMIT", value = "51MiB" })
      if contains(["redis_exporter", "postgres_exporter", "node_exporter"], container.name)
    ])
    error_message = "Each of the three exporters is non-essential, restarts, reserves 24 MiB of 64 MiB, runs as nobody on a read-only root file system and keeps its Go heap below the limit (80 percent)."
  }

  assert {
    condition     = [for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.name][6] == "redis_exporter"
    error_message = "The exporters follow the host state exporter in the task: Redis, Postgres, then the node exporter."
  }
}

run "the_exporters_listen_on_loopback_with_the_flags_that_were_verified" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[6].command, "--web.listen-address=127.0.0.1:9121")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[7].command, "--web.listen-address=127.0.0.1:9187")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command, "--web.listen-address=127.0.0.1:9100")
    )
    error_message = "The three exporters bind 127.0.0.1."
  }

  assert {
    condition = (
      toset(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[7].command) == toset([
        "--web.listen-address=127.0.0.1:9187", "--no-collector.settings", "--no-collector.stat_user_tables", "--no-collector.statio_user_tables", "--no-collector.replication",
        "--no-collector.replication_slots", "--no-collector.stat_replication", "--no-collector.stat_archiver", "--no-collector.stat_progress_vacuum", "--no-collector.roles",
        "--collector.database_wraparound", "--collector.long_running_transactions", "--collector.postmaster",
      ])
    )
    error_message = "The Postgres exporter runs exactly the collectors that were verified: nine off and the wraparound, long transaction and postmaster collectors on."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command, "--path.rootfs=/host")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command, "--collector.disable-defaults")
      && toset([for flag in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command : flag if can(regex("^--collector\\.(cpu|meminfo|loadavg|diskstats|filesystem|vmstat)$", flag))]) == toset(["--collector.cpu", "--collector.meminfo", "--collector.loadavg", "--collector.diskstats", "--collector.filesystem", "--collector.vmstat"])
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command, "--collector.filesystem.mount-points-include=^/(data|scratch)$")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command, "--collector.vmstat.fields=^(oom_kill|pgmajfault|pswpin|pswpout)$")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].command, "--collector.diskstats.device-include=^(nvme[0-9]+n[0-9]+|xvd[a-z]+)$")
    )
    error_message = "The node exporter turns every default collector off and runs six, limited to the two volumes, the disks and four vmstat fields."
  }
}

run "the_exporters_get_their_logins_from_the_secret_store_and_never_from_a_command_or_the_environment" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[6].environment, { name = "REDIS_ADDR", value = "redis://127.0.0.1:6379" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[6].environment, { name = "REDIS_USER", value = "exporter" })
      && jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[6].secrets == [{ name = "REDIS_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_exporter_password" }]
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[7].environment, { name = "DATA_SOURCE_URI", value = "127.0.0.1:5432/polaris?sslmode=disable" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[7].environment, { name = "DATA_SOURCE_USER", value = "postgres_exporter" })
      && jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[7].secrets == [{ name = "DATA_SOURCE_PASS", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/postgres_exporter_password" }]
    )
    error_message = "The Redis and Postgres exporters log in as their own users, with the password injected from Parameter Store at start."
  }

  assert {
    condition     = !can(regex("redis_exporter_password|postgres_exporter_password", jsonencode([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : [container.command, container.environment] if container.name != "x"])))
    error_message = "No parameter name or value appears in a command or the environment of a monitoring container."
  }
}

run "the_node_exporter_reads_the_two_volumes_read_only_and_asks_for_no_privilege" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].mountPoints, { sourceVolume = "node_data", containerPath = "/host/data", readOnly = true })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].mountPoints, { sourceVolume = "node_scratch", containerPath = "/host/scratch", readOnly = true })
      && length(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].mountPoints) == 2
      && jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[8].linuxParameters.capabilities.drop == ["ALL"]
    )
    error_message = "The node exporter mounts the data folder and the job scratch folder read-only, mounts nothing else (no /proc, no pid namespace) and drops every capability."
  }
}

run "redis_gets_the_exporter_user_with_the_verified_rules_and_the_password_from_the_secret_store" {
  command = plan

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0], "resetkeys resetchannels -@all +info +ping +config|get +client|list +client|setname +slowlog|get +slowlog|len +latency|latest +latency|histogram +command|info +dbsize")
      && contains(jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].secrets, { name = "EXPORTER_REDIS_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_exporter_password" })
      && !can(regex("redis_exporter_password", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
    )
    error_message = "Redis creates the user exporter with exactly the verified command list, from a password that comes from the secret store and is named by the secret, not by the script."
  }
}

run "postgres_creates_the_exporter_role_again_without_harm" {
  command = plan

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE ROLE postgres_exporter LOGIN CONNECTION LIMIT 3' WHERE NOT EXISTS")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "ALTER ROLE postgres_exporter PASSWORD :'exporter_password'")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "GRANT pg_monitor TO postgres_exporter")
      && contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].secrets, { name = "POSTGRES_EXPORTER_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/postgres_exporter_password" })
    )
    error_message = "Postgres creates the role once, sets its password on every start and grants it pg_monitor, so a repeat changes nothing."
  }
}

run "without_monitoring_redis_and_postgres_keep_their_definitions" {
  command = plan

  variables {
    monitoring_enabled = false
  }

  assert {
    condition = (
      !strcontains(jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0], "exporter")
      && !strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "postgres_exporter")
      && !contains([for secret in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].secrets : secret.name], "EXPORTER_REDIS_PASSWORD")
    )
    error_message = "With monitoring off the Redis and Postgres definitions carry no exporter user, role or secret."
  }
}

run "an_exporter_password_name_that_is_not_a_plain_parameter_path_is_refused" {
  command = plan

  variables {
    redis_exporter_password_name = "/bad name"
  }

  expect_failures = [var.redis_exporter_password_name]
}


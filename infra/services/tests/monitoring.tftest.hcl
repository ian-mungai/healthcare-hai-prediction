mock_provider "aws" {
  override_during = plan
}

# Prometheus, Grafana and the StatsD exporter run as one task behind monitoring_enabled. These cases turn the flag on with Marquez also on, the largest memory load the
# stack plans today, and pin the flag-off plan in the first case. Every case reads the plan only: no image runs and nothing reaches AWS.
variables {
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
  marquez_enabled             = true
  monitoring_enabled          = true

  log_group_names = {
    postgres   = "/example/postgres"
    redis      = "/example/redis"
    polaris    = "/example/polaris"
    airflow    = "/example/airflow"
    marquez    = "/example/marquez"
    monitoring = "/example/monitoring"
  }

  ecr_repository_urls = {
    postgres        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    marquez         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez"
    marquez_web     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez_web"
    prometheus      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
    grafana         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
    statsd_exporter = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
  }

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

run "flag_off_plans_no_monitoring_and_no_statsd" {
  command = plan

  variables {
    monitoring_enabled = false
    log_group_names = {
      postgres = "/example/postgres"
      redis    = "/example/redis"
      polaris  = "/example/polaris"
      airflow  = "/example/airflow"
      marquez  = "/example/marquez"
    }
    ecr_repository_urls = {
      postgres      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
      redis         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
      polaris       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
      polaris_admin = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
      airflow       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
      marquez       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez"
      marquez_web   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez_web"
    }
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

run "flag_on_adds_one_service_with_four_containers" {
  command = plan

  assert {
    condition     = contains(keys(aws_ecs_service.service), "monitoring") && contains(keys(aws_ecs_task_definition.service), "monitoring")
    error_message = "monitoring_enabled adds the monitoring service and its task definition."
  }

  assert {
    condition     = [for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.name] == ["monitoring_prepare", "prometheus", "grafana", "statsd_exporter"]
    error_message = "The monitoring task is the preparation container, Prometheus, Grafana and the StatsD exporter, in that order."
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
    ])) == sort(["3001/tcp", "9090/tcp", "9102/tcp", "9125/udp"])
    error_message = "The monitoring ports are Grafana 3001, Prometheus 9090, the exporter 9102 and the StatsD UDP port 9125 and nothing else."
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
    condition     = !can(regex("0\\.0\\.0\\.0|\\[::\\]", aws_ecs_task_definition.service["monitoring"].container_definitions))
    error_message = "No monitoring container names a wildcard address."
  }

  assert {
    condition     = output.host_ports_reachable_from_jobs == { polaris = 8181, redis = 6379 }
    error_message = "The monitoring ports stay off the list of ports the job tasks may reach."
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
    ]
    error_message = "Each monitoring container runs its image from the project ECR repository by digest. The preparation container reuses the Prometheus image."
  }
}

run "tag_instead_of_digest_for_grafana_is_refused" {
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
      grafana         = "13.2.5-ubuntu"
      statsd_exporter = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
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
      marquez       = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      marquez_web   = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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
      marquez       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez"
      marquez_web   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez_web"
    }
  }

  expect_failures = [var.ecr_repository_urls]
}

run "missing_monitoring_log_group_is_refused" {
  command = plan

  variables {
    log_group_names = {
      postgres = "/example/postgres"
      redis    = "/example/redis"
      polaris  = "/example/polaris"
      airflow  = "/example/airflow"
      marquez  = "/example/marquez"
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
      container.name == "grafana" || length(lookup(container, "secrets", [])) == 0
    ])
    error_message = "Only Grafana receives a secret."
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
    } == { monitoring_prepare = "16/32", prometheus = "128/256", grafana = "128/256", statsd_exporter = "32/64" }
    error_message = "The default monitoring memory is Prometheus 256 MiB, Grafana 256 MiB, the exporter 64 MiB and the preparation container 32 MiB, reserving about half of each."
  }

  assert {
    condition     = output.monitoring.memory_mib == { grafana = { limit = 256, reservation = 128 }, prometheus = { limit = 256, reservation = 128 }, statsd_exporter = { limit = 64, reservation = 32 } }
    error_message = "The monitoring output states the limits, so a measurement can be compared with them."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      container.name == "monitoring_prepare" || can(regex("^[0-9]+MiB$", one([for entry in container.environment : entry.value if entry.name == "GOMEMLIMIT"])))
    ])
    error_message = "Each Go container gets a soft memory limit (GOMEMLIMIT), so the runtime collects garbage before the container limit."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      container.name == "monitoring_prepare" || tonumber(trimsuffix(one([for entry in container.environment : entry.value if entry.name == "GOMEMLIMIT"]), "MiB")) < container.memory
    ])
    error_message = "The soft limit lies below the hard limit."
  }
}

run "monitoring_fits_beside_marquez_on_the_default_host" {
  command = plan

  assert {
    condition     = output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "Reservations with monitoring and Marquez on must stay within the budget."
  }

  assert {
    condition = sum(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [for container in jsondecode(definition.container_definitions) : container.memory]
    ])) <= var.host_memory_mib - 2048
    error_message = "Hard limits with monitoring and Marquez on must leave 2 GiB for the operating system."
  }
}

run "limits_change_in_one_place" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus      = { limit = 512, reservation = 256 }
      grafana         = { limit = 384, reservation = 192 }
      statsd_exporter = { limit = 64, reservation = 32 }
    }
    marquez_enabled = false
    ecr_repository_urls = {
      postgres        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
      redis           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
      polaris         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
      polaris_admin   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
      airflow         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
      prometheus      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
      grafana         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
      statsd_exporter = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
    }
    image_digests = {
      postgres        = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis           = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris         = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin   = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow         = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      prometheus      = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
      grafana         = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
      statsd_exporter = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    }
  }

  assert {
    condition = {
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container.name => "${container.memoryReservation}/${container.memory}"
    } == { monitoring_prepare = "16/32", prometheus = "256/512", grafana = "192/384", statsd_exporter = "32/64" }
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
      prometheus      = { limit = 128, reservation = 256 }
      grafana         = { limit = 256, reservation = 128 }
      statsd_exporter = { limit = 64, reservation = 32 }
    }
  }

  expect_failures = [var.monitoring_memory_mib]
}

run "limit_below_the_working_floor_is_refused" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus      = { limit = 64, reservation = 32 }
      grafana         = { limit = 256, reservation = 128 }
      statsd_exporter = { limit = 64, reservation = 32 }
    }
  }

  expect_failures = [var.monitoring_memory_mib]
}

run "monitoring_that_overruns_the_host_is_refused" {
  command = plan

  variables {
    monitoring_memory_mib = {
      prometheus      = { limit = 1024, reservation = 512 }
      grafana         = { limit = 1024, reservation = 512 }
      statsd_exporter = { limit = 64, reservation = 32 }
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
      code_monitoring_prometheus           = "/mnt/example-data/repo/monitoring/prometheus"
      code_monitoring_statsd               = "/mnt/example-data/repo/monitoring/statsd"
      grafana_data                         = "/mnt/example-data/grafana"
      prometheus_data                      = "/mnt/example-data/prometheus"
    }
    error_message = "Configuration comes from the repository checkout below the data mount and the two state folders lie on the data volume."
  }

  assert {
    condition = alltrue(flatten([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : [
        for mount in container.mountPoints : mount.readOnly if startswith(mount.sourceVolume, "code_")
      ]
    ]))
    error_message = "Every configuration mount is read-only."
  }

  assert {
    condition = (
      strcontains(one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).command[0], "chown 65534:65534 /prometheus")
      && strcontains(one([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : container if container.name == "monitoring_prepare"]).command[0], "chown 472:0 /grafana")
    )
    error_message = "The preparation container hands the Prometheus folder to 65534 and the Grafana folder to 472."
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

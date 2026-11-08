mock_provider "aws" {
  override_during = plan
}

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
  serving_execution_role_arn  = "arn:aws:iam::111111111111:role/example-serving-execution"
  polaris_task_role_arn       = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  data_mount_path             = "/mnt/example-data"
  marquez_enabled             = true
  mlflow_task_role_arn        = "arn:aws:iam::111111111111:role/example-mlflow"
  mlflow_host_ip              = "10.0.1.20"
  mlflow_artifact_bucket      = "example-project-data-dev"

  log_group_names = {
    postgres = "/example/postgres"
    redis    = "/example/redis"
    polaris  = "/example/polaris"
    airflow  = "/example/airflow"
    marquez  = "/example/marquez"
    mlflow   = "/example/mlflow"
  }

  ecr_repository_urls = {
    postgres      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    marquez       = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez"
    marquez_web   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez_web"
    mlflow        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/mlflow"
    model_api     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/model_api"
    streamlit     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/streamlit"
  }

  image_digests = {
    postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    marquez       = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    marquez_web   = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    mlflow        = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    model_api     = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    streamlit     = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
  }
}


run "memory_fits_host" {
  command = plan

  assert {
    condition     = output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "The memory reservations of the control plane must leave the reserve for the operating system and the later services free."
  }

  assert {
    condition = sum(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [for container in jsondecode(definition.container_definitions) : container.memory]
    ])) <= var.host_memory_mib - 2048
    error_message = "If every container, the one-shot tasks included, reached its hard limit, the host would still keep 2 GiB for the operating system."
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
    condition = output.reserved_memory_mib == sum(flatten([
      for key, definition in aws_ecs_task_definition.service : [for container in jsondecode(definition.container_definitions) : container.memoryReservation]
    ]))
    error_message = "The reserved memory output must equal the sum of the always-on container reservations."
  }
}

run "reservations_follow_measured_use_and_hard_limits_stay" {
  command = plan

  # Owner rule "B then A" (Nov 3 2026): the reservations of the seven services measured during the real gold run are their
  # measured use rounded up to 64 MiB; the hard limits stay as they were until a peak sample on the r7g.xlarge host.
  assert {
    condition = alltrue([
      for name, sizes in {
        postgres              = { reservation = 128, limit = 2048 }
        redis                 = { reservation = 64, limit = 2304 }
        polaris               = { reservation = 320, limit = 2048 }
        airflow_apiserver     = { reservation = 256, limit = 1536 }
        airflow_scheduler     = { reservation = 320, limit = 1536 }
        airflow_dag_processor = { reservation = 192, limit = 1280 }
        airflow_triggerer     = { reservation = 256, limit = 768 }
        } : anytrue([
          for container in jsondecode(aws_ecs_task_definition.service[name].container_definitions) :
          container.name == name && container.memoryReservation == sizes.reservation && container.memory == sizes.limit
      ])
    ])
    error_message = "Each measured service reserves its measured use rounded up to 64 MiB and keeps its hard limit."
  }
}

run "host_too_small_is_refused" {
  command = plan

  variables {
    host_memory_mib = 10240
  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "hard_limits_beyond_host_are_refused" {
  command = plan

  variables {
    host_memory_mib            = 12288
    other_services_reserve_mib = 2048
  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "reserve_below_floor_is_refused" {
  command = plan

  variables {
    other_services_reserve_mib = 512
  }

  expect_failures = [var.other_services_reserve_mib]
}

run "resized_host_fits_more" {
  command = plan

  variables {
    host_memory_mib = 32768
  }

  assert {
    condition     = output.reserved_memory_mib <= 32768 - var.other_services_reserve_mib
    error_message = "The resized host must accept the same reservations."
  }
}

run "health_checks_and_restarts" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.service : [
        for container in jsondecode(definition.container_definitions) :
        !container.essential || (
          length(lookup(lookup(container, "healthCheck", {}), "command", [])) > 1
          && container.healthCheck.retries >= 5
          && container.healthCheck.startPeriod >= 15
          && lookup(lookup(container, "restartPolicy", {}), "enabled", false)
        )
      ]
    ]))
    error_message = "Every essential long-running container needs a health check with retries and a start period, and a restart policy."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.one_shot : [
        for container in jsondecode(definition.container_definitions) :
        !container.essential || (
          length(lookup(lookup(container, "healthCheck", {}), "command", [])) > 1
          && can(container.restartPolicy.enabled)
          && (!container.restartPolicy.enabled || contains(lookup(container.restartPolicy, "ignoredExitCodes", []), 0))
        )
      ]
    ]))
    error_message = "Every essential one-shot container needs a health check and a restart policy that is off or ignores a successful exit."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].readonlyRootFilesystem
      && jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].linuxParameters.capabilities.drop == ["ALL"]
      && toset(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].linuxParameters.capabilities.add) == toset(["CHOWN", "DAC_OVERRIDE", "FOWNER"])
      && length(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].linuxParameters.capabilities.add) == 3
    )
    error_message = "The host preparation container runs as root only with a read-only root file system and exactly the capabilities CHOWN, DAC_OVERRIDE and FOWNER, and no others."
  }

  assert {
    condition = alltrue([
      for key, service in merge(aws_ecs_service.service, aws_ecs_service.serving) :
      one(service.deployment_circuit_breaker).enable && one(service.deployment_circuit_breaker).rollback && service.desired_count == 1
    ])
    error_message = "Every service keeps one task and stops a failing deployment through the circuit breaker."
  }

  assert {
    condition     = alltrue([for key, service in merge(aws_ecs_service.service, aws_ecs_service.serving) : service.deployment_minimum_healthy_percent == 0 && service.deployment_maximum_percent == 100])
    error_message = "Host ports and data directories allow one task at a time, so a deployment stops the old task first."
  }
}

run "health_check_values_fit_ecs_limits" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) :
        !contains(keys(container), "healthCheck") || (
          container.healthCheck.retries >= 1 && container.healthCheck.retries <= 10
          && container.healthCheck.interval >= 5 && container.healthCheck.interval <= 300
          && container.healthCheck.timeout >= 2 && container.healthCheck.timeout <= 60
          && container.healthCheck.startPeriod >= 0 && container.healthCheck.startPeriod <= 300
        )
      ]
    ]))
    error_message = "ECS accepts health check retries from 1 to 10, an interval from 5 to 300 seconds, a timeout from 2 to 60 seconds and a start period from 0 to 300 seconds; a task definition outside these ranges cannot register."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) :
        !contains(keys(container), "healthCheck") || alltrue([for field in ["retries", "interval", "timeout", "startPeriod"] : can(tonumber(container.healthCheck[field]))])
      ]
    ]))
    error_message = "Every health check sets all four numeric fields, so no default hides an out-of-range value."
  }
}

run "core_dumps_are_disabled_in_every_container" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) :
        contains(lookup(container, "ulimits", []), { name = "core", softLimit = 0, hardLimit = 0 })
      ]
    ]))
    error_message = "Every container needs a zero soft and hard core limit, so a crash cannot write injected credentials to a core file. The host must also stop piped core capture (kernel.core_pattern)."
  }
}

run "replaced_task_definitions_stay_registered" {
  command = plan

  assert {
    condition = alltrue([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : definition.skip_destroy == true
    ]) && length(merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot)) > 0
    error_message = "Every task definition needs skip_destroy = true: the project user may not deregister task definitions, so a replacement would fail the apply."
  }
}

run "container_definitions_for_review" {
  command = plan
}

run "secrets_reach_no_file_argument_log_or_dump" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.container_definitions_for_review.task_definition_settings
    definitions = run.container_definitions_for_review.container_definitions
  }

  assert {
    condition     = output.violations == []
    error_message = "A line lets an injected secret, or a variable that received it, reach a file, a command argument, the log or the environment dump."
  }

  assert {
    condition     = output.alias_chains_converged
    error_message = "A variable chain is longer than the passes the check follows; shorten the chain or add a pass."
  }

  assert {
    condition = toset(output.containers_checked) == toset([
      "airflow_apiserver/airflow_apiserver",
      "airflow_apiserver/airflow_migrate",
      "airflow_dag_processor/airflow_dag_processor",
      "airflow_scheduler/airflow_scheduler",
      "airflow_triggerer/airflow_triggerer",
      "marquez/marquez_api",
      "polaris/polaris",
      "polaris_bootstrap/polaris_bootstrap",
      "postgres/postgres",
      "postgres/postgres_databases",
      "redis/redis",
    ])
    error_message = "The check must follow every container that receives a secret; a container missing here was skipped."
  }
}

run "leak_to_a_file_before_redis_starts_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "printf %s \\\"$pw\\\" >/tmp/review-synthetic-leak\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: printf %s \"$pw\" >/tmp/review-synthetic-leak")
    error_message = "A password written to a file through a variable must fail."
  }
}

run "leak_through_a_chain_of_variables_to_an_appended_file_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "copy=$pw\\nsaved=\\\"$copy\\\"\\necho \\\"$saved\\\" >> /tmp/leak\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: echo \"$saved\" >> /tmp/leak")
    error_message = "A password copied through two variables and appended to a file must fail."
  }
}

run "leak_through_a_substitution_alias_into_an_argument_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "first=$(printf %s \\\"$${REDIS_PASSWORD}\\\")\\nsecond=\\\"x$${first}y\\\"\\ncurl -u \\\"user:$second\\\" http://127.0.0.1/\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: curl -u \"user:$second\" http://127.0.0.1/")
    error_message = "A password passed through a substitution and an interpolated variable into a program argument must fail."
  }
}

run "leak_through_a_pipe_to_tee_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "printf %s \\\"$pw\\\" | tee /tmp/leak\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: printf %s \"$pw\" | tee /tmp/leak")
    error_message = "A password piped to tee must fail."
  }
}

run "leak_through_a_block_redirection_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "{\\nprintf %s \\\"$pw\\\"\\n} > /tmp/leak\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: } > /tmp/leak") && contains(output.violations, "redis/redis: printf %s \"$pw\"")
    error_message = "A password printed inside a block whose output goes to a file must fail, both at the redirection and at the bare printf."
  }
}

run "leak_through_an_unquoted_here_document_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "cat <<EOF\\n$pw\\nEOF\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: cat <<EOF") && contains(output.violations, "redis/redis: $pw")
    error_message = "A password expanded in an unquoted here-document must fail."
  }
}

run "leak_to_the_log_through_a_bare_printf_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "echo \\\"$REDIS_PASSWORD\\\"\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: echo \"$REDIS_PASSWORD\"")
    error_message = "A password printed to standard output reaches the container log and must fail."
  }
}

run "leak_through_a_long_variable_chain_is_reported" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "a1=$pw\\na2=$a1\\na3=$a2\\na4=$a3\\na5=$a4\\na6=$a5\\nexec redis-server")
    }
  }

  assert {
    condition     = !output.alias_chains_converged
    error_message = "A chain of six variables is longer than the passes followed, so the check must say so instead of passing silently."
  }
}

run "leak_through_an_environment_dump_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "printenv REDIS_PASSWORD\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: printenv REDIS_PASSWORD")
    error_message = "Printing the secret variable from the environment must fail."
  }
}

run "leak_through_shell_tracing_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"-ec\"", "\"-exc\"")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: -exc")
    error_message = "A shell started with tracing writes every expanded command, password included, to the log and must fail."
  }
}

run "a_different_program_reading_the_secret_pipe_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server -", "exec tee /tmp/leak")
    }
  }

  assert {
    condition     = length([for line in output.violations : line if strcontains(line, "exec tee /tmp/leak")]) > 0
    error_message = "Only the pinned programs may read a secret from a process substitution."
  }
}

run "leak_through_a_psql_echo_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      postgres = replace(run.container_definitions_for_review.container_definitions["postgres"], "\\\\getenv airflow_password AIRFLOW_DB_PASSWORD", "\\\\getenv airflow_password AIRFLOW_DB_PASSWORD\\n\\\\echo :'airflow_password'")
    }
  }

  assert {
    condition     = contains(output.violations, "postgres/postgres_databases: \\echo :'airflow_password'")
    error_message = "A password echoed by psql reaches the container log and must fail."
  }
}

run "leak_through_a_psql_copy_to_a_file_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      postgres = replace(run.container_definitions_for_review.container_definitions["postgres"], "\\\\getenv airflow_password AIRFLOW_DB_PASSWORD", "\\\\getenv airflow_password AIRFLOW_DB_PASSWORD\\n\\\\set copy :'airflow_password'\\nCOPY (SELECT :'copy') TO '/tmp/leak';")
    }
  }

  assert {
    condition     = contains(output.violations, "postgres/postgres_databases: COPY (SELECT :'copy') TO '/tmp/leak';")
    error_message = "A password copied through a psql variable into a server-side file must fail."
  }
}

run "no_secret_in_command_arguments" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [
          for line in flatten([for element in concat(lookup(container, "entryPoint", []), lookup(container, "command", [])) : split("\n", element)]) :
          !can(regex("(^|\\s)(psql|redis-cli|redis-server|curl|python|airflow|java)\\s.*(-v\\s+\\w+=|--requirepass|--password|-a\\s|-p\\s|PGPASSWORD=|:[^@/\\s]+@)", line))
        ]
      ]
    ]))
    error_message = "No program may receive a password option or a URL with a password on its command line."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [
          for entry in lookup(container, "environment", []) : [
            for secret in lookup(container, "secrets", []) :
            !can(regex("\\$\\{?${secret.name}([^A-Za-z0-9_]|$)", entry.value)) || can(regex("^bash -c 'printf %s \"[^\"`()<>|;&]*\"'$", entry.value))
          ]
        ]
      ]
    ]))
    error_message = "An environment value may expand a secret only through a _CMD option that runs the printf builtin on one quoted string with no substitution, redirection or pipe."
  }
}

run "no_secret_in_environment_literals" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [
          for entry in lookup(container, "environment", []) : !can(regex("(?i)(password|secret|token|key|credential)", entry.name)) || can(regex("_CMD$", entry.name))
        ]
      ]
    ]))
    error_message = "An environment entry must not carry a secret: its name suggests a credential."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [
          for entry in lookup(container, "environment", []) : !can(regex("://[^/@[:space:]]*:[^/@[:space:]$]+@", entry.value)) && (!can(regex("^[A-Za-z0-9+/=_-]{32,}$", entry.value)) || can(regex("^/?[a-z0-9]+(?:_[a-z0-9]+)+_?(?:/[a-z0-9]+(?:_[a-z0-9]+)*_?)*$", entry.value))) && !can(regex("^arn:aws:(secretsmanager|ssm):", entry.value))
        ]
      ]
    ]))
    error_message = "An environment value must not hold a URL with a password, a long random token or a secret reference. Lowercase words joined by underscores (the project naming format) are names, not tokens."
  }
}

run "database_passwords_stay_out_of_statement_logs" {
  command = plan

  assert {
    condition = (
      can(regex("(?s)exec psql -X <<'SQL'\\n.*\\\\getenv airflow_password AIRFLOW_DB_PASSWORD\\n\\\\getenv marquez_password MARQUEZ_DB_PASSWORD\\n", jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0]))
      && can(regex("(?s)SET log_statement = 'none';\\nSET log_min_error_statement = 'panic';\\nSET log_min_duration_statement = -1;\\n.*ALTER ROLE airflow PASSWORD :'airflow_password';.*ALTER ROLE marquez PASSWORD :'marquez_password';", jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0]))
      && can(regex("\\\\set VERBOSITY terse\\n", jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0]))
    )
    error_message = "The databases script must read each password with \\getenv, switch statement and error-statement logging off before the first ALTER ROLE and keep error reports terse."
  }
}

run "secrets_come_from_parameter_store" {
  command = plan

  assert {
    condition = toset(output.parameter_names_used) == toset([
      "/healthcare_hai_prediction/airflow_api_secret_key",
      "/healthcare_hai_prediction/airflow_db_password",
      "/healthcare_hai_prediction/airflow_fernet_key",
      "/healthcare_hai_prediction/airflow_jwt_secret",
      "/healthcare_hai_prediction/marquez_db_password",
      "/healthcare_hai_prediction/polaris_root_secret",
      "/healthcare_hai_prediction/postgres_password",
      "/healthcare_hai_prediction/redis_password",
    ])
    error_message = "The task definitions must read exactly the committed parameter names."
  }

  assert {
    condition     = toset(flatten([for service in merge(aws_ecs_service.service, aws_ecs_service.serving) : service.name])) == toset([for key in keys(merge(aws_ecs_service.service, aws_ecs_service.serving)) : "example_project_dev_${key}"])
    error_message = "Service names follow the project, environment and service naming."
  }
}

run "secret_references_are_parameter_arns" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [
          for secret in lookup(container, "secrets", []) : can(regex("^arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/[a-z0-9_]+$", secret.valueFrom))
        ]
      ]
    ]))
    error_message = "Every secret reference must be a full Parameter Store ARN of this account and region (the name without its leading slash after parameter/), not a bare name and not a Secrets Manager ARN."
  }
}

run "each_secret_gets_its_own_arn" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[0].secrets[0].valueFrom == "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/postgres_password"
      && jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].secrets[0].valueFrom == "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_password"
      && contains(output.parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_password")
      && jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)[0].secrets[0].valueFrom == "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/polaris_root_secret"
    )
    error_message = "Each container must receive the Parameter Store ARN of its own parameter."
  }
}

run "the_arn_set_is_the_execution_role_grant" {
  command = plan

  # The foundation grants ssm:GetParameters on arn:aws:ssm:<region>:<account>:parameter/healthcare_hai_prediction/<name> for these eight names, no other action and no KMS action.
  assert {
    condition = toset(output.parameter_arns_used) == toset([
      for name in ["postgres_password", "airflow_db_password", "marquez_db_password", "airflow_fernet_key", "airflow_jwt_secret", "airflow_api_secret_key", "redis_password", "polaris_root_secret"] :
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/${name}"
    ])
    error_message = "The task definitions must inject exactly the eight parameters the execution role may read (the Airflow administrator password is not injected)."
  }
}

run "secret_names_follow_their_variables" {
  command = plan

  variables {
    redis_password_name = "/example/redis"
  }

  assert {
    condition     = contains(output.parameter_names_used, "/example/redis") && !contains(output.parameter_names_used, "/healthcare_hai_prediction/redis_password") && contains(output.parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter/example/redis")
    error_message = "A secret name variable must change the name and the ARN the task definition reads."
  }
}

run "name_without_leading_slash_is_refused" {
  command = plan

  variables {
    postgres_password_name = "healthcare_hai_prediction/postgres_password"
  }

  expect_failures = [var.postgres_password_name]
}

run "dashed_or_wildcard_parameter_name_is_refused" {
  command = plan

  variables {
    airflow_fernet_key_name  = "/healthcare-hai-prediction/airflow-fernet-key"
    marquez_db_password_name = "/healthcare_hai_prediction/*"
  }

  expect_failures = [var.airflow_fernet_key_name, var.marquez_db_password_name]
}

run "aws_prefixed_parameter_name_is_refused" {
  command = plan

  variables {
    polaris_root_secret_name = "/aws/polaris_root_secret"
  }

  expect_failures = [var.polaris_root_secret_name]
}

run "every_service_propagates_the_task_definition_tags" {
  command = plan

  assert {
    condition     = length(aws_ecs_service.service) > 0 && alltrue([for service in merge(aws_ecs_service.service, aws_ecs_service.serving) : service.propagate_tags == "TASK_DEFINITION" && service.enable_ecs_managed_tags])
    error_message = "Every service must set propagate_tags to TASK_DEFINITION, or its tasks start without the Project tag."
  }
}

run "one_shot_launch_propagates_the_task_definition_tags" {
  command = plan

  assert {
    condition = (
      toset(keys(output.one_shot_run_task)) == toset(keys(aws_ecs_task_definition.one_shot))
      && alltrue([
        for launch in values(output.one_shot_run_task) :
        launch.propagate_tags == "TASK_DEFINITION" && launch.enable_ecs_managed_tags
        && try(launch.tags["Project"] == "example_project" && length(launch.tags) == 1, false)
      ])
    )
    error_message = "Every one-shot task needs --tags key=Project,value=<project_tag>, --propagate-tags TASK_DEFINITION and --enable-ecs-managed-tags."
  }
}

run "every_resource_carries_the_project_tag" {
  command = plan

  assert {
    condition = alltrue([
      for definition in concat(values(aws_ecs_task_definition.service), values(aws_ecs_task_definition.one_shot), values(aws_ecs_service.service)) :
      definition.tags["Project"] == var.project_tag && definition.tags["Environment"] == var.environment && definition.tags["ManagedBy"] == "Terraform"
    ])
    error_message = "Every task definition and service carries the foundation project_tag, so the budget counts it."
  }
}

run "dashed_project_tag_is_refused" {
  command = plan

  variables {
    project_tag = "example-project"
  }

  expect_failures = [var.project_tag]
}

run "logs_go_to_foundation_groups" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) :
        container.logConfiguration.logDriver == "awslogs"
        && contains(values(var.log_group_names), container.logConfiguration.options["awslogs-group"])
        && container.logConfiguration.options["awslogs-region"] == var.aws_region
        && container.logConfiguration.options["awslogs-stream-prefix"] == container.name
        && container.logConfiguration.options["mode"] == "non-blocking"
      ]
    ]))
    error_message = "Every container logs without blocking to a foundation log group, in its region, with its own name as stream prefix."
  }
}

run "host_networking_on_the_ec2_provider" {
  command = plan

  assert {
    condition = alltrue([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) :
      definition.network_mode == (key == "host_prepare" ? "none" : "host") && definition.requires_compatibilities == toset(["EC2"]) && one(definition.runtime_platform).cpu_architecture == "ARM64"
    ])
    error_message = "Tasks use host networking on the Graviton host, except host preparation, which runs as root and makes no network call, so it has no network."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.one_shot : [
        for container in jsondecode(definition.container_definitions) : length(lookup(container, "portMappings", [])) == 0
      ] if definition.network_mode == "none"
    ]))
    error_message = "A task without a network cannot publish ports."
  }

  assert {
    condition = alltrue([
      for key, service in merge(aws_ecs_service.service, aws_ecs_service.serving) :
      length(service.network_configuration) == 0
      && one(service.capacity_provider_strategy).capacity_provider == var.ec2_capacity_provider_name
      && service.cluster == var.cluster_name
    ])
    error_message = "Services run on the host capacity provider of the foundation cluster and have no awsvpc network configuration."
  }
}

run "state_lives_on_the_data_mount" {
  command = plan

  assert {
    condition = (
      one(aws_ecs_task_definition.service["postgres"].volume).host_path == "/mnt/example-data/postgres"
      && length(aws_ecs_task_definition.service["redis"].volume) == 0
      && contains([for volume in aws_ecs_task_definition.service["airflow_scheduler"].volume : volume.host_path], "/mnt/example-data/airflow/logs")
      && length(aws_ecs_task_definition.service["polaris"].volume) == 0
      && one(aws_ecs_task_definition.one_shot["host_prepare"].volume).host_path == "/mnt/example-data"
    )
    error_message = "Postgres and the Airflow logs live on the data volume; Redis and Polaris keep no state on the host."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.service : [
        for volume in definition.volume : startswith(volume.host_path, "/mnt/example-data/") || (volume.name == "job_scratch" && volume.host_path == var.airflow_launch_scratch_path)
      ]
    ]))
    error_message = "Every host path lies below the data mount, including the default code checkout; only the job scratch volume lies at the job scratch path."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.service : [
        for container in jsondecode(definition.container_definitions) : [
          for mount in lookup(container, "mountPoints", []) : mount.readOnly == true && mount.containerPath == "/opt/hai/job_scratch" if mount.sourceVolume == "job_scratch"
        ]
      ]
    ]))
    error_message = "A service mounts the job scratch volume only read-only at /opt/hai/job_scratch: the jobs own that volume, and a service only measures its free space."
  }
}

run "code_checkout_path_is_configurable" {
  command = plan

  variables {
    dag_code_path = "/opt/hai/checkout"
  }

  assert {
    condition     = contains([for volume in aws_ecs_task_definition.service["airflow_apiserver"].volume : volume.host_path], "/opt/hai/checkout/dags")
    error_message = "The Airflow code mounts follow dag_code_path."
  }
}

run "images_run_by_digest_from_ecr" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) :
        can(regex("^111111111111\\.dkr\\.ecr\\.us-west-2\\.amazonaws\\.com/example_project/[a-z_]+@sha256:[0-9a-f]{64}$", container.image))
      ]
    ]))
    error_message = "Every image comes from the foundation ECR repositories and is pinned by digest."
  }
}

run "roles_are_separated" {
  command = plan

  assert {
    condition = (
      aws_ecs_task_definition.service["polaris"].task_role_arn == var.polaris_task_role_arn
      && alltrue([for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : key == "polaris" || definition.task_role_arn == var.control_plane_task_role_arn])
      && alltrue([for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : definition.execution_role_arn == var.execution_role_arn])
    )
    error_message = "Only Polaris holds the lakehouse role; Airflow, Postgres, Redis, Marquez and the one-shot tasks hold none."
  }
}

run "containers_run_as_non_root" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.service : [
        for container in jsondecode(definition.container_definitions) :
        can(regex("^[0-9]+(:[0-9]+)?$", lookup(container, "user", ""))) && tonumber(split(":", lookup(container, "user", "0"))[0]) != 0
      ]
    ]))
    error_message = "Every long-running container needs a numeric non-root user, because the host blocks the metadata service only for non-root users."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.one_shot : [
        for container in jsondecode(definition.container_definitions) :
        key == "host_prepare" || (can(regex("^[0-9]+(:[0-9]+)?$", lookup(container, "user", ""))) && tonumber(split(":", lookup(container, "user", "0"))[0]) != 0)
      ]
    ]))
    error_message = "Only the host preparation task may run as root: it must change the owner of the data directories."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].user == "0"
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chown 999:999 /host/postgres\n")
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/redis")
    )
    error_message = "The root exception is the host preparation task, which also hands the Postgres directory to its non-root user."
  }
}

run "listeners_bind_loopback" {
  command = plan

  assert {
    condition = toset(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : [for mapping in lookup(container, "portMappings", []) : mapping.hostPort]
      ]
    ])) == toset([3000, 5000, 5001, 5432, 6379, 8080, 8181, 8182, 8974])
    error_message = "A new host port needs a loopback bind and a place in this list."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[0].command, "listen_addresses=127.0.0.1")
      && can(regex("(^|\\n)exec airflow api-server --host 127\\.0\\.0\\.1 --port 8080$", jsondecode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions)[1].command[2]))
      && contains(jsondecode(aws_ecs_task_definition.service["polaris"].container_definitions)[0].environment, { name = "QUARKUS_MANAGEMENT_HOST", value = "127.0.0.1" })
      && contains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].environment, { name = "AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_HOST", value = "127.0.0.1" })
    )
    error_message = "Postgres, the Airflow API server, the Polaris management interface and the scheduler health server must bind 127.0.0.1."
  }

  assert {
    condition = (
      can(regex("\\nexec airflow scheduler --skip-serve-logs$", jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].command[2]))
      && can(regex("\\nexec airflow triggerer --skip-serve-logs$", jsondecode(aws_ecs_task_definition.service["airflow_triggerer"].container_definitions)[0].command[2]))
    )
    error_message = "The scheduler and the triggerer log servers bind every interface and have no host option, so both must run with --skip-serve-logs."
  }

  assert {
    condition = (
      can(regex("type: http\\n\\s+bindHost: 127\\.0\\.0\\.1\\n\\s+port: 5000\\n", jsondecode(aws_ecs_task_definition.service["marquez"].container_definitions)[0].command[0]))
      && can(regex("type: http\\n\\s+bindHost: 127\\.0\\.0\\.1\\n\\s+port: 5001\\n", jsondecode(aws_ecs_task_definition.service["marquez"].container_definitions)[0].command[0]))
      && contains(jsondecode(aws_ecs_task_definition.service["marquez"].container_definitions)[1].environment, { name = "WEB_HOST", value = "127.0.0.1" })
    )
    error_message = "Both Marquez API connectors and the web server must bind 127.0.0.1."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [
        for container in jsondecode(definition.container_definitions) : container.name == "redis" || !can(regex("0\\.0\\.0\\.0|\\[::\\]", jsonencode(container)))
      ]
    ]))
    error_message = "Only Redis may name a wildcard address; every other service stays off it."
  }

  assert {
    condition = (
      can(regex("'bind 0\\.0\\.0\\.0' 'port 6379'", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
      && can(regex("'maxmemory-policy noeviction'", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
      && can(regex("'maxmemory 2048mb'", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
      && can(regex("'save \"\"'", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
      && length(regexall("save", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0])) == 1
      && can(regex("printf 'requirepass \"%s\"\\\\n' \"\\$pw\"", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
      && can(regex("exec redis-server - < <\\(", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
    )
    error_message = "Redis listens on 6379 only with a password, read from standard input, holds 2048 MiB of feature values, never evicts them and saves no snapshots."
  }

  assert {
    condition     = output.host_ports_reachable_from_jobs == { polaris = 8181, redis = 6379 }
    error_message = "Only Polaris and Redis are reachable from the job tasks."
  }
}

run "start_order_inside_a_task" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].essential == false
      && jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].dependsOn[0].condition == "HEALTHY"
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE DATABASE airflow")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE DATABASE marquez")
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "polaris_schema")
    )
    error_message = "The databases container waits for a healthy Postgres, creates the airflow, marquez and Polaris schema objects and then exits."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions)[1].dependsOn[0].condition == "SUCCESS"
      && jsondecode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions)[0].name == "airflow_migrate"
      && strcontains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].command[2], "db check-migrations")
    )
    error_message = "The API server starts after the migration and the other Airflow services wait for it."
  }
}

run "airflow_sign_in_needs_no_password_file" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions)[1].environment, { name = "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS", value = "true" })
      && !strcontains(jsonencode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions), "PASSWORDS_FILE")
      && !strcontains(jsonencode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions), "AIRFLOW_ADMIN")
    )
    error_message = "The Airflow API server listens on loopback only and signs in without a stored password."
  }
}

run "polaris_bootstrap_keeps_the_root_secret_off_the_command_line" {
  command = plan

  assert {
    condition = (
      length(jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)) == 1
      && startswith(jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)[0].image, "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin@sha256:")
      && can(regex("run-java\\.sh bootstrap --credentials-file /dev/stdin < <\\(", jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)[0].command[0]))
      && !can(regex("--credential[ =]|bootstrap .* -c ", jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)[0].command[0]))
      && !jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)[0].restartPolicy.enabled
      && jsondecode(aws_ecs_task_definition.one_shot["polaris_bootstrap"].container_definitions)[0].secrets[0].name == "POLARIS_ROOT_SECRET"
      && aws_ecs_task_definition.one_shot["polaris_bootstrap"].task_role_arn == var.control_plane_task_role_arn
    )
    error_message = "The Polaris bootstrap reads its root secret from the task secrets field and passes it on standard input, and it is not restarted."
  }
}

run "marquez_is_optional" {
  command = plan

  variables {
    marquez_enabled = false
    image_digests = {
      postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    }
  }

  assert {
    condition     = !contains(keys(aws_ecs_service.service), "marquez") && length(aws_ecs_service.service) == 7
    error_message = "Without Marquez the stack has the seven other services and needs no Marquez image."
  }
}

run "marquez_without_image_is_refused" {
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

run "tag_instead_of_digest_is_refused" {
  command = plan

  variables {
    image_digests = {
      postgres      = "latest"
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

run "missing_log_group_is_refused" {
  command = plan

  variables {
    log_group_names = {
      postgres = "/example/postgres"
      polaris  = "/example/polaris"
      airflow  = "/example/airflow"
      marquez  = "/example/marquez"
    }
  }

  expect_failures = [var.log_group_names]
}

run "shared_task_role_is_refused" {
  command = plan

  variables {
    control_plane_task_role_arn = "arn:aws:iam::111111111111:role/example-polaris"
  }

  expect_failures = [var.control_plane_task_role_arn]
}

run "relative_data_mount_is_refused" {
  command = plan

  variables {
    data_mount_path = "mnt/data"
  }

  expect_failures = [var.data_mount_path]
}

run "repository_without_url_is_refused" {
  command = plan

  variables {
    ecr_repository_urls = {
      postgres = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    }
  }

  expect_failures = [var.ecr_repository_urls]
}

run "other_profile_is_refused" {
  command = plan

  variables {
    aws_profile = "another_project_dev"
  }

  expect_failures = [var.aws_profile]
}

run "container_definitions_match_the_approved_versions" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.container_definitions_for_review.task_definition_settings
    definitions = run.container_definitions_for_review.container_definitions
  }

  assert {
    condition = output.execution_digests == {
      "airflow_apiserver/airflow_apiserver"         = "dc57463c23236f2823ecbabffcacc2347259003ffd42808e6375c90a7c20be55"
      "airflow_apiserver/airflow_migrate"           = "0daf62772bcd2e88304c71f0791c9f0b98983c289f715f046cc34c146b239be5"
      "airflow_dag_processor/airflow_dag_processor" = "123d1308431f66be7702f9566547fce86b14e4b533ce77bd85afe16e7bb7dd3e"
      "airflow_scheduler/airflow_scheduler"         = "4a529d0ac5414d00b74ea4fbc7655906ae30bcd37cb4ad44d873699a932d1715"
      "airflow_triggerer/airflow_triggerer"         = "b3530c6e55c47f3ac37ee820db80901303c4fa23439f7c5dae5b4b165afe412d"
      "host_prepare/host_prepare"                   = "a208b8e2ca2bc45db88ede4b24aa7acdbe5b4392194ba73f3b68ac8ac03454bb"
      "marquez/marquez_api"                         = "9df0f81c5440fa10c2df2fc83d01edb1b5e09b06c2f5cbc1930659d87fae1033"
      "marquez/marquez_web"                         = "4ff0cc6590b24265a20486f1b4aeab92bf28afeb6eceb93ea83d022d279743c9"
      "polaris/polaris"                             = "4091249731a1ee59e36999c93cacc38ad67f1c8be37c13308b5b65f3d673e487"
      "polaris_bootstrap/polaris_bootstrap"         = "1a0bffac42991a144cec7b17cc7809f2316317c916bd9af2f3271f62ec7899c5"
      "postgres/postgres"                           = "84ffdb77ff2045be5b3abcf4437443a9a14b42cd406a155da052da8c5c4d860f"
      "postgres/postgres_databases"                 = "d1832558975c44ccb1c94ed39efbe9e024edaa1ddd6f3161b9d7e95288db71f4"
      "redis/redis"                                 = "65ca3d65c4cf5e89cb31edbdbbc2eda34fb067c7ed8b58bfc4c078ff25f447be"
    }
    error_message = "A container script changed from its reviewed version. Review the change, then record its digest here. Current digests: ${jsonencode(output.execution_digests)}"
  }
}

run "indent_change_in_a_script_changes_its_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      marquez = replace(run.container_definitions_for_review.container_definitions["marquez"], "  password: $${MARQUEZ_DB_PASSWORD}", " password: $${MARQUEZ_DB_PASSWORD}")
    }
  }

  assert {
    condition     = output.execution_digests["marquez/marquez_api"] != run.container_definitions_match_the_approved_versions.execution_digests["marquez/marquez_api"]
    error_message = "A changed indent inside the Marquez configuration here-document must change the execution digest."
  }
}

run "argument_split_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\\nexec redis-server", "\",\"exec redis-server")
    }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "Splitting the Redis script into separate command arguments must change the execution digest."
  }
}

run "added_tracing_variable_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"entryPoint\":", "\"environment\":[{\"name\":\"SHELLOPTS\",\"value\":\"xtrace\"}],\"entryPoint\":")
    }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "An environment variable that turns on shell tracing must change the execution digest."
  }
}

run "injected_tracing_variable_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"secrets\":[", "\"secrets\":[{\"name\":\"SHELLOPTS\",\"valueFrom\":\"arn:aws:ssm:us-west-2:111111111111:parameter/example/shellopts\"},")
    }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "A shell-control variable injected as a secret binding must change the execution digest."
  }
}

run "added_environment_file_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"entryPoint\":", "\"environmentFiles\":[{\"type\":\"s3\",\"value\":\"arn:aws:s3:::example/tracing.env\"}],\"entryPoint\":")
    }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "An environment file can set shell-control variables, so adding one must change the execution digest."
  }
}

run "every_field_of_every_container_and_referenced_volume_is_bound_unless_excluded" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.container_definitions_for_review.task_definition_settings
    definitions = run.container_definitions_for_review.container_definitions
  }

  assert {
    condition     = output.mutation_mismatches == []
    error_message = "A field changed without changing the execution digest, or an excluded field changed it. Mismatches: ${jsonencode(output.mutation_mismatches)}"
  }

  assert {
    condition     = output.mutations_checked > 1000
    error_message = "The mutations are generated from the definitions and the task settings; a count this low means most were not generated. Count: ${output.mutations_checked}"
  }

  assert {
    condition     = toset(output.excluded_fields_exercised) == toset(["cpu", "memory", "memoryReservation"])
    error_message = "Every excluded field needs a mutation that leaves the digest unchanged, so the exclusion list holds no unused or misspelled name. Exercised: ${jsonencode(output.excluded_fields_exercised)}"
  }

  assert {
    condition = alltrue([
      for id, paths in output.mutation_paths : alltrue([for path in ["container.name", "container.image", "container.user", "container.essential", "container.logConfiguration", "container.dockerSecurityOptions", "task.network_mode", "task.task_role_arn", "task.execution_role_arn", "task.volume", "task.family", "task.runtime_platform[0].cpu_architecture"] : contains(paths, path)])
    ])
    error_message = "Fields every container and task has must each have been mutated, in the container definition and in the task settings."
  }

  assert {
    condition = alltrue([
      for path in [
        "container.linuxParameters.tmpfs[0]",
        "container.secrets[0].valueFrom",
        "container.healthCheck.command",
        "container.healthCheck.command[1]",
        "container.logConfiguration.options.awslogs-group",
        "container.command[0]",
        "container.entryPoint[1]",
        "container.portMappings[0].hostPort",
        "container.ulimits[0].name",
        "task.tags.Project",
      ] : contains(output.mutation_paths["redis/redis"], path)
      ]) && alltrue([
      for path in [
        "container.mountPoints",
        "container.mountPoints[0].containerPath",
        "task.volume[0].host_path",
        "task.volume[0].name",
      ] : contains(output.mutation_paths["postgres/postgres"], path)
    ])
    error_message = "The Redis mount, secret, script, health check, log, port, limit, volume and tag values must each have been mutated."
  }
}

# Control: a task volume that no container mounts is excluded from the digest, so adding one (and the generated mutations of its fields)
# changes no digest and produces no mismatch.
run "an_unmounted_volume_changes_no_digest_and_no_mutation" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = merge(run.container_definitions_for_review.task_definition_settings, {
      airflow_apiserver = merge(run.container_definitions_for_review.task_definition_settings["airflow_apiserver"], {
        volume = concat(run.container_definitions_for_review.task_definition_settings["airflow_apiserver"].volume, [
          merge(run.container_definitions_for_review.task_definition_settings["airflow_apiserver"].volume[0], { name = "unused_data", host_path = "/srv/unused" })
        ])
      })
    })
    definitions = run.container_definitions_for_review.container_definitions
  }

  assert {
    condition     = output.mutation_mismatches == []
    error_message = "An unmounted volume or a mutation of its fields changed a digest. Mismatches: ${jsonencode(output.mutation_mismatches)}"
  }

  assert {
    condition     = output.execution_digests == run.every_field_of_every_container_and_referenced_volume_is_bound_unless_excluded.execution_digests
    error_message = "Adding a volume that no container mounts changed an execution digest."
  }
}

run "redis_mount_target_change_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      postgres = replace(run.container_definitions_for_review.container_definitions["postgres"], "\"containerPath\":\"/var/lib/postgresql/data\"", "\"containerPath\":\"/usr/local/bin\"")
    }
  }

  assert {
    condition     = output.execution_digests["postgres/postgres"] != run.container_definitions_match_the_approved_versions.execution_digests["postgres/postgres"]
    error_message = "Mounting the Postgres data volume over /usr/local/bin can hide the executables the approved script runs, so it must change the execution digest."
  }
}

run "working_directory_change_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"entryPoint\":", "\"workingDirectory\":\"/data\",\"entryPoint\":")
    }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "A working directory decides which relative paths the script resolves, so adding one must change the execution digest."
  }
}

run "mounted_host_path_change_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = jsondecode(replace(jsonencode(run.container_definitions_for_review.task_definition_settings), "/mnt/example-data/postgres", "/mnt/example-data/other"))
    definitions = { postgres = run.container_definitions_for_review.container_definitions["postgres"] }
  }

  assert {
    condition     = output.execution_digests["postgres/postgres"] != run.container_definitions_match_the_approved_versions.execution_digests["postgres/postgres"]
    error_message = "The host path behind a mounted volume decides which files the container sees, so changing it must change the execution digest."
  }
}

run "task_network_mode_change_changes_the_digest" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = merge(run.container_definitions_for_review.task_definition_settings, { redis = merge(run.container_definitions_for_review.task_definition_settings["redis"], { network_mode = "bridge" }) })
    definitions = { redis = run.container_definitions_for_review.container_definitions["redis"] }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "The task network mode decides what the container can reach, so changing it must change the execution digest."
  }
}

run "memory_change_leaves_the_digest_unchanged" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.container_definitions_for_review.task_definition_settings
    definitions = { redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"memory\":2304", "\"memory\":4096") }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] == run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "The memory limit is a named exclusion: resizing it needs no new approval."
  }
}

run "quoted_tracing_before_a_secret_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "set -o \\\"xtrace\\\"\\npw=$REDIS_PASSWORD\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: set -o \"xtrace\"")
    error_message = "Tracing switched on with a quoted option name must fail, because it prints every later expansion of the secret."
  }
}

run "indirect_eval_of_a_secret_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "exec redis-server", "key=REDIS_PASSWORD\\neval 'printf %s \\\"$'\\\"$key\\\"'\\\"'\\nexec redis-server")
    }
  }

  assert {
    condition     = length([for line in output.violations : line if startswith(line, "redis/redis: eval ")]) == 1
    error_message = "An eval that expands a secret through a variable holding its name must fail."
  }
}

run "mlflow_is_off_by_default" {
  command = plan

  assert {
    condition = (
      !contains(keys(aws_ecs_service.service), "mlflow")
      && length(aws_ecs_service.service) == 8
      && !contains(keys(output.host_ports_reachable_from_jobs), "mlflow")
      && !contains(output.parameter_names_used, var.mlflow_db_password_name)
      && !contains(output.parameter_names_used, var.mlflow_admin_password_name)
      && !strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "mlflow")
    )
    error_message = "MLflow adds no service, no open port, no secret and no database until mlflow_enabled is true."
  }
}

run "mlflow_off_needs_no_inputs" {
  command = plan

  variables {
    mlflow_task_role_arn   = null
    mlflow_host_ip         = null
    mlflow_artifact_bucket = null
    log_group_names = {
      postgres = "/example/postgres"
      redis    = "/example/redis"
      polaris  = "/example/polaris"
      airflow  = "/example/airflow"
      marquez  = "/example/marquez"
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
    condition     = length(aws_ecs_service.service) == 8
    error_message = "With MLflow off the stack needs no MLflow role, address, bucket, log group or image."
  }
}

run "mlflow_service_shape" {
  command = plan

  variables {
    mlflow_enabled = true
  }

  assert {
    condition = (
      contains(keys(aws_ecs_service.service), "mlflow")
      && length(aws_ecs_service.service) == 9
      && aws_ecs_task_definition.service["mlflow"].task_role_arn == var.mlflow_task_role_arn
      && aws_ecs_task_definition.service["mlflow"].network_mode == "host"
      && length(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)) == 1
    )
    error_message = "MLflow is one more host service with its own task role."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/mlflow@sha256:9999999999999999999999999999999999999999999999999999999999999999"
      && jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].user == "10005:10005"
      && jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].readonlyRootFilesystem
      && jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].linuxParameters.capabilities.drop == ["ALL"]
      && jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].entryPoint == ["python", "-I", "/opt/mlflow/start_server.py"]
    )
    error_message = "MLflow runs by digest as a non-root user on a read-only file system with every capability dropped."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].command == [
        "--host", "0.0.0.0", "--port", "5100", "--workers", "1", "--serve-artifacts",
        "--artifacts-destination", "s3://example-project-data-dev/mlflow/",
        "--allowed-hosts", "127.0.0.1:5100,localhost:5100,10.0.1.20:5100",
      ]
    )
    error_message = "MLflow proxies artifacts into the mlflow/ prefix of the data bucket and accepts only its own addresses as Host headers."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].environment, { name = "MLFLOW_BACKEND_STORE_URI", value = "postgresql+psycopg2://mlflow@127.0.0.1:5432/mlflow" })
      && contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].environment, { name = "MLFLOW_DISABLE_TELEMETRY", value = "true" })
      && contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].environment, { name = "MLFLOW_AUTH_DATABASE_URI", value = "postgresql+psycopg2://mlflow@127.0.0.1:5432/mlflow_auth" })
      && contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].environment, { name = "MLFLOW_ADMIN_USERNAME", value = "admin" })
      && jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].secrets == [
        { name = "PGPASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/mlflow_db_password" },
        { name = "MLFLOW_ADMIN_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/mlflow_admin_password" },
      ]
      && !strcontains(aws_ecs_task_definition.service["mlflow"].container_definitions, "postgresql+psycopg2://mlflow:")
    )
    error_message = "The database password and the administrator password reach MLflow only as task secrets, never in the URI or the command."
  }

  assert {
    condition = (
      !strcontains(aws_ecs_task_definition.service["mlflow"].container_definitions, "--app-name")
      && !strcontains(aws_ecs_task_definition.service["mlflow"].container_definitions, "NO_PERMISSIONS")
      && strcontains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].healthCheck.command[3], "401")
    )
    error_message = "The start script owns the app name and the default permission. The health check accepts the 401 that an unauthenticated probe gets from a server that asks for a login."
  }

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE DATABASE mlflow_auth OWNER mlflow")
      && contains(output.parameter_names_used, var.mlflow_admin_password_name)
      && output.parameter_names_used == distinct(output.parameter_names_used)
    )
    error_message = "The databases container also creates the mlflow_auth database, and the execution role injects the administrator password."
  }

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "CREATE DATABASE mlflow OWNER mlflow")
      && contains([for secret in jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].secrets : secret.name], "MLFLOW_DB_PASSWORD")
      && contains(output.parameter_names_used, var.mlflow_db_password_name)
    )
    error_message = "The databases container creates the mlflow role and database from the managed password."
  }

  assert {
    condition     = output.host_ports_reachable_from_jobs.mlflow == 5100 && output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "MLflow names its one port for the job security group and still fits the host memory budget."
  }
}

run "mlflow_without_role_is_refused" {
  command = plan

  variables {
    mlflow_enabled       = true
    mlflow_task_role_arn = null
  }

  expect_failures = [var.mlflow_task_role_arn]
}

run "mlflow_with_shared_role_is_refused" {
  command = plan

  variables {
    mlflow_enabled       = true
    mlflow_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  }

  expect_failures = [var.mlflow_task_role_arn]
}

run "mlflow_without_address_is_refused" {
  command = plan

  variables {
    mlflow_enabled = true
    mlflow_host_ip = "mlflow.example.com"
  }

  expect_failures = [var.mlflow_host_ip]
}

run "mlflow_without_bucket_is_refused" {
  command = plan

  variables {
    mlflow_enabled         = true
    mlflow_artifact_bucket = null
  }

  expect_failures = [var.mlflow_artifact_bucket]
}

run "mlflow_without_image_is_refused" {
  command = plan

  variables {
    mlflow_enabled = true
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

run "mlflow_without_log_group_is_refused" {
  command = plan

  variables {
    mlflow_enabled = true
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

run "mlflow_definitions_for_review" {
  command = plan

  variables {
    mlflow_enabled = true
  }
}

run "mlflow_secrets_reach_no_file_argument_log_or_dump" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.mlflow_definitions_for_review.task_definition_settings
    definitions = run.mlflow_definitions_for_review.container_definitions
  }

  assert {
    condition     = output.violations == []
    error_message = "A line lets the MLflow database or administrator password reach a file, a command argument, the log or the environment dump."
  }

  assert {
    condition     = contains(output.containers_checked, "mlflow/mlflow") && contains(output.containers_checked, "postgres/postgres_databases")
    error_message = "The shared check follows the secrets of the MLflow container and of the databases container."
  }
}

run "broker_is_off_by_default" {
  command = plan

  assert {
    condition = (
      !contains(keys(aws_ecs_service.service), "broker")
      && length(aws_ecs_service.service) == 8
      && !contains(output.parameter_names_used, var.broker_token_name)
      && !strcontains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].command[2], "broker")
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "broker")
      && !contains(keys(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].linuxParameters), "tmpfs")
      && !contains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].secrets[*].name, "BROKER_TOKEN")
    )
    error_message = "The broker adds no service, no secret, no scheduler setting and no host folder until broker_enabled is true."
  }
}

run "broker_off_needs_no_inputs" {
  command = plan

  variables {
    broker_task_role_arn = null
    broker_jobs          = null
  }

  assert {
    condition     = length(aws_ecs_service.service) == 8
    error_message = "With the broker off the stack needs no broker role and no job settings."
  }
}

run "broker_service_shape" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train", "silver"]
    }
  }

  assert {
    condition = (
      contains(keys(aws_ecs_service.service), "broker")
      && length(aws_ecs_service.service) == 9
      && aws_ecs_task_definition.service["broker"].task_role_arn == var.broker_task_role_arn
      && aws_ecs_task_definition.service["broker"].network_mode == "host"
      && length(jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)) == 1
    )
    error_message = "The broker is one more host service with its own task role."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].secrets == [
        { name = "BROKER_TOKEN", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/broker_token" }
      ]
      && contains(output.parameter_names_used, "/healthcare_hai_prediction/broker_token")
      && !contains([for item in jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].environment : item.name], "BROKER_TOKEN")
    )
    error_message = "The broker token reaches the container only as a Parameter Store secret, never as an environment value."
  }

  assert {
    condition = (
      alltrue([
        for expected in [
          "BROKER_FAMILIES=train,silver",
          "BROKER_FAMILY_PREFIX=example_project_dev_job_",
          "BROKER_CLUSTER=example-project-dev",
          "BROKER_PORT=18790",
          "BROKER_POLARIS_URI=http://10.0.1.20:8181/api/catalog",
          "BROKER_MLFLOW_URI=http://10.0.1.20:5100",
          "BROKER_SIGNED_PARAMETER=/healthcare_hai_prediction/first_publishes_signed",
          "BROKER_SUBNETS=subnet-0example1",
          "BROKER_NETWORKS={\"lakehouse\":\"sg-0example1\",\"training\":\"sg-0example2\"}",
        ] : contains([for item in jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].environment : "${item.name}=${item.value}"], expected)
      ])
      && jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].entryPoint == ["python", "-m", "scripts.orchestration.ecs_broker"]
      && jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].readonlyRootFilesystem == true
    )
    error_message = "The broker names its cluster, job definition prefix, families, subnet, security group and host addresses, and runs the broker module with a read-only root file system."
  }

  assert {
    condition = (
      toset([for volume in aws_ecs_task_definition.service["broker"].volume : "${volume.name}=${volume.host_path}"]) == toset(["code_repo=/mnt/example-data/repo", "broker_state=/mnt/example-data/broker"])
      && alltrue([for mount in jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].mountPoints : mount.readOnly if mount.sourceVolume == "code_repo"])
    )
    error_message = "The broker reads the code checkout read-only and keeps its ledger on the data volume."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].secrets[*].name, "BROKER_TOKEN")
      && jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].linuxParameters.tmpfs[0].containerPath == "/opt/hai/broker"
      && strcontains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].command[2], "/opt/hai/broker/client.json")
      && strcontains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].command[2], "http://127.0.0.1:18790")
      && strcontains(jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions)[0].command[2], "/opt/hai/broker/broker_token")
    )
    error_message = "The scheduler gets the token and writes the client settings the unchanged DAG client reads into a memory-backed folder."
  }

  assert {
    condition     = strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/broker")
    error_message = "Host preparation creates the broker state folder."
  }
}

run "broker_without_role_is_refused" {
  command = plan

  variables {
    broker_enabled = true
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train"]
    }
  }

  expect_failures = [var.broker_task_role_arn]
}

run "broker_with_shared_role_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train"]
    }
  }

  expect_failures = [var.broker_task_role_arn]
}

run "broker_without_job_settings_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
  }

  expect_failures = [var.broker_jobs]
}

run "broker_with_public_host_address_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "203.0.113.20"
      families           = ["train"]
    }
  }

  expect_failures = [var.broker_jobs]
}

run "broker_accepts_the_bronze_family" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train", "bronze"]
    }
  }

  assert {
    condition     = contains(keys(aws_ecs_service.service), "broker") && strcontains(aws_ecs_task_definition.service["broker"].container_definitions, "bronze")
    error_message = "The broker accepts the bronze family and passes it to the broker container."
  }
}

run "broker_accepts_the_serve_and_metadata_families" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["serve", "metadata"]
    }
  }

  assert {
    condition     = strcontains(aws_ecs_task_definition.service["broker"].container_definitions, "serve,metadata")
    error_message = "The broker accepts the serve and metadata families and passes them to the broker container."
  }
}

run "serve_without_the_training_security_group_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["serve"]
    }
  }

  expect_failures = [var.broker_jobs]
}

run "broker_with_unknown_family_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train", "write_gold"]
    }
  }

  expect_failures = [var.broker_jobs]
}

run "a_family_without_the_security_group_of_its_network_class_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["silver", "drift"]
    }
  }

  expect_failures = [var.broker_jobs]
}

run "a_security_group_for_an_unknown_network_class_is_refused" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", offline = "sg-0example3" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["silver"]
    }
  }

  expect_failures = [var.broker_jobs]
}

run "broker_token_name_under_the_aws_prefix_is_refused" {
  command = plan

  variables {
    broker_token_name = "/aws/broker_token"
  }

  expect_failures = [var.broker_token_name]
}

run "broker_definitions_for_review" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train"]
    }
  }
}

run "broker_secrets_reach_no_argument_log_or_dump" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.broker_definitions_for_review.task_definition_settings
    definitions = run.broker_definitions_for_review.container_definitions
  }

  # The unchanged DAG client reads the token from a file, so the scheduler writes it into its private memory-backed folder. The shared check must report that
  # write and no other: nothing in the broker container, and no argument, log line or environment dump anywhere.
  assert {
    condition = (
      length(output.violations) > 0
      && alltrue([for line in output.violations : startswith(line, "airflow_scheduler/airflow_scheduler: printf ") && strcontains(line, "> /opt/hai/broker/")])
    )
    error_message = "Only the scheduler's write of the client files into /opt/hai/broker may reach a file. No other line may let the broker token out."
  }

  assert {
    condition     = contains(output.containers_checked, "broker/broker") && contains(output.containers_checked, "airflow_scheduler/airflow_scheduler")
    error_message = "The shared check follows the token in the broker container and in the scheduler."
  }
}

run "broker_accepts_the_promote_family" {
  command = plan

  variables {
    broker_enabled       = true
    broker_task_role_arn = "arn:aws:iam::111111111111:role/example-broker"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-data-dev"
      host_ip            = "10.0.1.20"
      families           = ["train", "promote"]
    }
  }

  assert {
    condition     = contains([for item in jsondecode(aws_ecs_task_definition.service["broker"].container_definitions)[0].environment : "${item.name}=${item.value}"], "BROKER_FAMILIES=train,promote")
    error_message = "The broker enables the promote job when it is named."
  }
}

run "model_api_is_off_by_default" {
  command = plan

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.service), "model_api") && !contains(keys(aws_ecs_service.serving), "model_api")
    error_message = "The model API must not exist unless model_api_enabled is true."
  }

  assert {
    condition     = alltrue([for name in output.parameter_names_used : !strcontains(name, "model_api")])
    error_message = "No model API parameter may be read while the API is off."
  }
}

run "model_api_is_private_and_pinned" {
  command = plan

  variables {
    model_api_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"

  }

  assert {
    condition     = contains(keys(aws_ecs_task_definition.service), "model_api") && aws_ecs_service.serving["model_api"].desired_count == 1
    error_message = "One model API task must run when the flag is true."
  }

  assert {
    condition     = aws_ecs_task_definition.service["model_api"].task_role_arn == "arn:aws:iam::111111111111:role/example-model-api"
    error_message = "The model API runs under its own task role."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["model_api"].container_definitions) :
      strcontains(container.image, "@sha256:2222") && container.readonlyRootFilesystem && !container.privileged
    ])
    error_message = "The model API image runs by digest on a read-only root file system."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["model_api"].container_definitions) :
      anytrue([for entry in container.environment : entry.name == "MODEL_API_BIND" && entry.value == "127.0.0.1"])
    ])
    error_message = "The API binds the host loopback only, so no network path reaches it except Session Manager port forwarding."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["model_api"].container_definitions) :
      alltrue([for entry in container.environment : !can(regex("(?i)password|secret|credential|key_hash", entry.name))])
    ])
    error_message = "No secret may sit in the plain environment. Secrets arrive through the secrets list."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["model_api"].container_definitions) :
      alltrue([for secret in container.secrets : startswith(secret.valueFrom, "arn:aws:ssm:us-west-2:111111111111:parameter/")])
    ])
    error_message = "Every model API secret is a Parameter Store reference."
  }

  assert {
    condition     = output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "With the model API on, the reservations must still fit the host."
  }

  assert {
    condition     = contains(output.parameter_names_used, "/healthcare_hai_prediction/model_api_key_hashes")
    error_message = "The execution role must be told to read the key hash parameter."
  }
}

run "model_api_without_a_role_is_refused" {
  command = plan

  variables {
    model_api_enabled      = true
    model_api_manifest_uri = "s3://example-data/serving/deployments"
    model_api_registry_uri = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri   = "http://127.0.0.1:5100"
  }

  expect_failures = [var.model_api_task_role_arn]
}

run "model_api_sharing_the_polaris_role_is_refused" {
  command = plan

  variables {
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-polaris"
  }

  expect_failures = [var.model_api_task_role_arn]
}

run "model_api_with_a_public_mlflow_address_is_refused" {
  command = plan

  variables {
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://203.0.113.5:5100"
  }

  expect_failures = [var.model_api_mlflow_uri]
}

run "model_api_with_marquez_does_not_fit_the_small_host" {
  command = plan

  variables {
    host_memory_mib         = 16384 # the r7g.large size this refusal is about
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"

  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "model_api_with_marquez_fits_the_resized_host" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"

  }

  assert {
    condition     = contains(keys(aws_ecs_task_definition.service), "model_api") && contains(keys(aws_ecs_task_definition.service), "marquez")
    error_message = "Both optional services must plan on the resized host."
  }
}

run "streamlit_is_off_by_default" {
  command = plan

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.service), "streamlit") && !contains(keys(aws_ecs_service.serving), "streamlit")
    error_message = "The Streamlit page must not exist unless streamlit_enabled is true."
  }

  assert {
    condition     = alltrue([for name in output.parameter_names_used : !strcontains(name, "streamlit")])
    error_message = "No Streamlit parameter may be read while the page is off."
  }
}

run "streamlit_is_private_pinned_and_small" {
  command = plan

  variables {
    host_memory_mib         = 32768
    streamlit_enabled       = true
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  assert {
    condition     = contains(keys(aws_ecs_task_definition.service), "streamlit") && aws_ecs_service.serving["streamlit"].desired_count == 1
    error_message = "One Streamlit task must run when the flag is true."
  }

  assert {
    condition     = aws_ecs_task_definition.service["streamlit"].task_role_arn == "arn:aws:iam::111111111111:role/example-streamlit"
    error_message = "The Streamlit page runs under its own task role."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["streamlit"].container_definitions) :
      strcontains(container.image, "@sha256:3333") && container.readonlyRootFilesystem && !container.privileged && container.user == "10020:10020"
    ])
    error_message = "The Streamlit image runs by digest, unprivileged, on a read-only root file system."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["streamlit"].container_definitions) :
      anytrue([for entry in container.environment : entry.name == "STREAMLIT_SERVER_ADDRESS" && entry.value == "127.0.0.1"])
    ])
    error_message = "The page binds the host loopback only, so Session Manager port forwarding is the one way in."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["streamlit"].container_definitions) :
      anytrue([for entry in container.environment : entry.name == "MODEL_API_URL" && entry.value == "http://127.0.0.1:8000"])
    ])
    error_message = "The page reaches the model API over the host loopback."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["streamlit"].container_definitions) :
      alltrue([for entry in container.environment : !can(regex("(?i)password|secret|credential|key", entry.name))])
    ])
    error_message = "No secret may sit in the plain environment."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["streamlit"].container_definitions) :
      length(container.secrets) == 1 && container.secrets[0].name == "MODEL_API_KEY" && startswith(container.secrets[0].valueFrom, "arn:aws:ssm:us-west-2:111111111111:parameter/")
    ])
    error_message = "The page's only secret is its API key, a Parameter Store reference."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["streamlit"].container_definitions) :
      container.memoryReservation == 256 && container.memory == 512
    ])
    error_message = "The assumed memory limits of the page are a 256 MiB reservation and a 512 MiB limit."
  }

  assert {
    condition     = contains(output.parameter_names_used, "/healthcare_hai_prediction/streamlit_model_api_key")
    error_message = "The execution role must be told to read the page's key parameter."
  }

  assert {
    condition     = output.reserved_memory_mib <= var.host_memory_mib - var.other_services_reserve_mib
    error_message = "With the page on, the reservations must still fit the host."
  }

  assert {
    condition = sum(flatten([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) : [for container in jsondecode(definition.container_definitions) : container.memory]
    ])) <= var.host_memory_mib - 2048
    error_message = "With the page on, the hard limits must still leave 2 GiB for the operating system."
  }
}

run "streamlit_without_the_model_api_is_refused" {
  command = plan

  variables {
    streamlit_enabled       = true
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
  }

  expect_failures = [var.streamlit_enabled]
}

run "streamlit_without_a_role_is_refused" {
  command = plan

  variables {
    streamlit_enabled       = true
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  expect_failures = [var.streamlit_task_role_arn]
}

run "streamlit_sharing_the_model_api_role_is_refused" {
  command = plan

  variables {
    streamlit_enabled       = true
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  expect_failures = [var.streamlit_task_role_arn]
}

run "streamlit_offering_an_unserved_type_is_refused" {
  command = plan

  variables {
    streamlit_enabled       = true
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
    streamlit_hai_types     = "HAI_2"
    model_api_enabled       = true
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  expect_failures = [var.streamlit_hai_types]
}

run "streamlit_off_does_not_check_the_api_types" {
  command = plan

  variables {
    model_api_hai_types = "HAI_2"
  }

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.service), "streamlit")
    error_message = "A page that is off must not constrain the types a valid API serves."
  }
}

run "streamlit_with_repeated_types_is_refused" {
  command = plan

  variables {
    streamlit_hai_types = "HAI_1,HAI_1"
  }

  expect_failures = [var.streamlit_hai_types]
}

# D18 serving: the model API and the dashboard are released by GitHub, which registers their task definition revisions and rolls the services to them. So they are
# services of their own with ignore_changes on task_definition (an owner apply must not roll a release back), their task definitions use the dedicated serving
# execution role, and the model API reads Feast through its own read-only Redis user.
run "serving_services_are_their_own_resource" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    streamlit_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  assert {
    condition     = toset(keys(aws_ecs_service.serving)) == toset(["model_api", "streamlit"]) && !contains(keys(aws_ecs_service.service), "model_api") && !contains(keys(aws_ecs_service.service), "streamlit")
    error_message = "The model API and the dashboard are services of the serving resource and are no longer in the shared one, so the moved blocks carry them over."
  }

  assert {
    condition = alltrue([
      for key, service in aws_ecs_service.serving :
      service.name == "example_project_dev_${key}" && service.desired_count == 1 && service.propagate_tags == "TASK_DEFINITION" && service.enable_ecs_managed_tags &&
      service.deployment_minimum_healthy_percent == 0 && service.deployment_maximum_percent == 100 && one(service.deployment_circuit_breaker).rollback
    ])
    error_message = "A serving service keeps the name, count, tag propagation and deployment settings it had in the shared resource, or the move would replace it."
  }

  assert {
    condition     = toset(keys(output.service_names)) == toset(concat(keys(aws_ecs_service.service), ["model_api", "streamlit"]))
    error_message = "The service names output lists the serving services too."
  }
}

run "serving_task_definitions_use_the_serving_execution_role" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    streamlit_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  assert {
    condition = alltrue([
      for key, definition in aws_ecs_task_definition.service :
      definition.execution_role_arn == (contains(["model_api", "streamlit"], key) ? "arn:aws:iam::111111111111:role/example-serving-execution" : "arn:aws:iam::111111111111:role/example-execution")
    ])
    error_message = "The model API and the dashboard name the serving execution role, and every other task keeps the shared one."
  }

  assert {
    condition = alltrue([
      for definition in values(aws_ecs_task_definition.one_shot) : definition.execution_role_arn == "arn:aws:iam::111111111111:role/example-execution"
    ])
    error_message = "The one-shot tasks keep the shared execution role."
  }
}

run "the_serving_tasks_inject_only_what_the_serving_execution_role_reads" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    streamlit_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  assert {
    condition = toset(output.serving_parameter_arns_used) == toset([
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/aws_polaris_serving_client_id",
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/aws_polaris_serving_client_secret",
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_key_hashes",
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_credential",
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_mlflow_user",
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_redis_password",
      "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/streamlit_model_api_key",
    ])
    error_message = "The serving tasks inject the seven parameters the serving execution role reads, and no control-plane secret."
  }

  assert {
    condition     = !contains(output.serving_parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_password")
    error_message = "The shared Redis password never reaches a serving task."
  }

  assert {
    condition     = !contains(output.control_plane_parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_key_hashes") && contains(output.control_plane_parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_redis_password")
    error_message = "The control-plane tasks inject the model API's Redis password (the Redis task creates the user from it) and none of the other serving parameters."
  }
}

run "serving_without_the_serving_execution_role_is_refused" {
  command = plan

  variables {
    serving_execution_role_arn = null
    model_api_enabled          = true
    marquez_enabled            = false
    model_api_task_role_arn    = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri     = "s3://example-data/serving/deployments"
    model_api_registry_uri     = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri       = "http://127.0.0.1:5100"
  }

  expect_failures = [var.serving_execution_role_arn]
}

run "serving_sharing_the_shared_execution_role_is_refused" {
  command = plan

  variables {
    serving_execution_role_arn = "arn:aws:iam::111111111111:role/example-execution"
    model_api_enabled          = true
    marquez_enabled            = false
    model_api_task_role_arn    = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri     = "s3://example-data/serving/deployments"
    model_api_registry_uri     = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri       = "http://127.0.0.1:5100"
  }

  expect_failures = [var.serving_execution_role_arn]
}

run "serving_off_needs_no_serving_execution_role" {
  command = plan

  variables {
    serving_execution_role_arn = null
  }

  assert {
    condition     = length(aws_ecs_service.serving) == 0 && length(output.serving_parameter_arns_used) == 0
    error_message = "With the serving services off the stack plans none and needs no serving execution role."
  }
}

run "redis_is_unchanged_while_the_model_api_is_off" {
  command = plan

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions) :
      !strcontains(jsonencode(container), "model_api") && toset([for secret in container.secrets : secret.name]) == toset(["REDIS_PASSWORD", "REDISCLI_AUTH"])
    ])
    error_message = "While the model API is off the Redis task names no model API user and injects only its own password."
  }
}

run "redis_keeps_no_data_across_starts" {
  command = plan

  assert {
    condition = (
      length(lookup(jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0], "mountPoints", [])) == 0
      && contains([for mount in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].linuxParameters.tmpfs : mount.containerPath], "/data")
    )
    error_message = "Redis keeps its data folder in task memory only, so a start never loads a dump left by an earlier task and the online store is empty until the next materialize."
  }
}

run "redis_creates_a_read_only_user_for_the_model_api" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions) :
      contains(container.secrets, { name = "MODEL_API_REDIS_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_redis_password" }) &&
      contains(container.secrets, { name = "REDIS_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_password" })
    ])
    error_message = "The Redis task injects the model API's Redis password beside its own."
  }

  assert {
    condition     = can(regex("'save \"\"'", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0])) && length(regexall("save", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0])) == 1
    error_message = "The Redis script with the model API user also saves no snapshots."
  }

  # Redis ACLs cannot bind a database, so the user may only run the read commands the serving client sends, select databases 1 (Feast) and 2 (the marker),
  # and nothing else: no write, no flush, no script, no administration. A connection starts in database 0, which stays empty.
  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions) :
      contains(split("\n", container.command[0]), "rules='resetkeys ~* resetchannels -@all +hmget +hget +hgetall +exists +ping +auth +hello +client|setname +select|1 +select|2'")
    ])
    error_message = "The model API user is read-only: only the listed read commands, and select for the two databases the client uses."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions) :
      anytrue([for line in split("\n", container.command[0]) : trimspace(line) == "printf 'user model_api on \"\\076%s\" %s\\n' \"$mp\" \"$rules\""]) &&
      !anytrue([for word in ["+@", "allkeys", "allcommands", "+set", "+del", "+flush", "+eval", "+config", "+acl", "+keys", "+scan", "+debug", "+shutdown", "+select|0", "+select|3"] : strcontains(container.command[0], word)])
    ])
    error_message = "The user is created from the password through a printf line, and the command grants no write, no administration and no select of another database."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["redis"].container_definitions) :
      anytrue([for line in split("\n", container.command[0]) : trimspace(line) == "printf 'requirepass \"%s\"\\n' \"$pw\""])
    ])
    error_message = "The default user keeps its password for the control plane."
  }
}

run "the_model_api_logs_in_as_its_own_redis_user" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["model_api"].container_definitions) :
      contains(container.secrets, { name = "REDIS_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/model_api_redis_password" }) &&
      contains(container.environment, { name = "REDIS_USERNAME", value = "model_api" }) &&
      !anytrue([for secret in container.secrets : secret.valueFrom == "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/redis_password"])
    ])
    error_message = "The model API presents the password of its own Redis user as REDIS_USERNAME model_api, never the shared Redis password."
  }
}

run "the_model_api_redis_password_name_must_differ_from_the_shared_one" {
  command = plan

  variables {
    model_api_redis_password_name = "/healthcare_hai_prediction/redis_password"
  }

  expect_failures = [var.model_api_redis_password_name]
}

run "container_definitions_with_serving_for_review" {
  command = plan

  variables {
    host_memory_mib         = 32768
    model_api_enabled       = true
    streamlit_enabled       = true
    marquez_enabled         = false
    model_api_task_role_arn = "arn:aws:iam::111111111111:role/example-model-api"
    streamlit_task_role_arn = "arn:aws:iam::111111111111:role/example-streamlit"
    model_api_manifest_uri  = "s3://example-data/serving/deployments"
    model_api_registry_uri  = "s3://example-data/lakehouse/feast/registry.pb"
    model_api_mlflow_uri    = "http://127.0.0.1:5100"
  }
}

run "the_redis_user_password_reaches_no_file_argument_log_or_dump" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.container_definitions_with_serving_for_review.task_definition_settings
    definitions = run.container_definitions_with_serving_for_review.container_definitions
  }

  assert {
    condition     = output.violations == []
    error_message = "A line lets the model API's Redis password, or a variable that received it, reach a file, a command argument, the log or the environment dump."
  }

  assert {
    condition     = contains(output.containers_checked, "redis/redis")
    error_message = "The Redis task, which now receives two secrets, is followed by the check."
  }
}

run "leak_of_the_model_api_redis_password_to_a_file_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_with_serving_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_with_serving_for_review.container_definitions["redis"], "exec redis-server", "printf %s \\\"$mp\\\" >/tmp/review-synthetic-leak\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: printf %s \"$mp\" >/tmp/review-synthetic-leak")
    error_message = "The ACL user's password written to a file through a variable must fail."
  }
}

run "leak_of_the_model_api_redis_password_to_the_log_is_caught" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks = run.container_definitions_with_serving_for_review.task_definition_settings
    definitions = {
      redis = replace(run.container_definitions_with_serving_for_review.container_definitions["redis"], "exec redis-server", "echo \\\"$MODEL_API_REDIS_PASSWORD\\\"\\nexec redis-server")
    }
  }

  assert {
    condition     = contains(output.violations, "redis/redis: echo \"$MODEL_API_REDIS_PASSWORD\"")
    error_message = "The ACL user's password echoed to the log must fail."
  }
}

run "serving_container_definitions_match_the_approved_versions" {
  command = plan

  module {
    source = "./tests/flow"
  }

  variables {
    tasks       = run.container_definitions_with_serving_for_review.task_definition_settings
    definitions = run.container_definitions_with_serving_for_review.container_definitions
  }

  assert {
    condition = alltrue([
      for pin in [
        { id = "model_api/model_api", digest = "f87f0f24695ce2d0d9e7a0f9306ccbf81fdc5e6ff2dfd680764e07516c884704" },
        { id = "redis/redis", digest = "7f52421e0183b5e1bb5b278b45dff274fcc87e3e52c57986cb169bfb71831a46" },
        { id = "streamlit/streamlit", digest = "f8e2c6e9514fc5a6205c06cbfed1d52950ddcce9fe60e33034de38c25a0438af" },
      ] : output.execution_digests[pin.id] == pin.digest
    ])
    error_message = "A serving container script changed from its reviewed version. Review the change, then record its digest here. Current digests: ${jsonencode(output.execution_digests)}"
  }
}

run "launch_mounts_are_always_present" {
  command = plan

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for path in [
          "/opt/hai/orchestration/airflow_steps.py",
          "/opt/hai/orchestration/ecs_preflight.py",
          "/opt/hai/orchestration/ecs_launch.py",
          "/opt/hai/orchestration/ecs_launch_provider.py",
          "/opt/hai/config/orchestration/host_jobs.json",
          "/opt/hai/config/orchestration/ecs_broker_jobs.json",
          "/opt/hai/scripts/lakehouse/bronze_dispatch.py",
          "/opt/hai/job_scratch",
        ] : strcontains(aws_ecs_task_definition.service[key].container_definitions, path)
      ])
    ])
    error_message = "Every Airflow container mounts the launch modules, tables and the scratch folder: every DAG step launches through the launch library."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) : alltrue([
          for mount in container.mountPoints : contains([for volume in aws_ecs_task_definition.service[key].volume : volume.name], mount.sourceVolume)
        ])
      ])
    ])
    error_message = "Every mount of an Airflow container names a volume of its task definition."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] :
      contains([for volume in aws_ecs_task_definition.service[key].volume : "${volume.name}=${volume.host_path}"], "job_scratch=/data/job_scratch")
    ])
    error_message = "Every Airflow task definition declares the scratch volume at airflow_launch_scratch_path."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) : alltrue([
          for mount in container.mountPoints : mount.readOnly || contains(["airflow_logs", "airflow_lineage_provider"], mount.sourceVolume)
        ])
      ])
    ])
    error_message = "The launch mounts are read-only: only the Airflow log and lineage provider folders are writable."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) :
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_POOL=host_jobs") if startswith(container.name, "airflow")
      ])
    ])
    error_message = "Every Airflow container names the pool of host jobs in HAI_LAUNCH_POOL."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) : (
          length(setsubtract(
            ["AWS_REGION", "HAI_LAUNCH_POOL", "HAI_LAUNCH_CLUSTER", "HAI_LAUNCH_FAMILY_PREFIX", "HAI_LAUNCH_PROJECT_TAG", "HAI_LAUNCH_LOG_GROUP", "HAI_LAUNCH_HOST_PROVIDER", "HAI_LAUNCH_BUCKET", "HAI_LAUNCH_SIGNED_PARAMETER", "HAI_LAUNCH_POLARIS_URI", "HAI_LAUNCH_MLFLOW_URI", "HAI_LAUNCH_SCRATCH_PATH"],
            [for item in container.environment : item.name]
          )) == 0
          && !anytrue([for item in try(container.secrets, []) : startswith(item.name, "HAI_LAUNCH_") || item.name == "AWS_REGION"])
          && !anytrue([for item in container.environment : startswith(item.name, "HAI_LAUNCH_") && (strcontains(item.value, "arn:aws:ssm") || strcontains(item.value, "arn:aws:secretsmanager"))])
        ) if startswith(container.name, "airflow")
      ])
    ])
    error_message = "Every Airflow container carries each launch setting as a plain environment value, and none of them is a secret or a secret reference."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions)[0].name == "airflow_migrate"
      && endswith(jsondecode(aws_ecs_task_definition.service["airflow_apiserver"].container_definitions)[0].command[2], "airflow db migrate\nexec airflow pools set host_jobs 1 'One host job at a time'\n")
    )
    error_message = "The migrate container creates the one-slot pool of host jobs after the migration."
  }
}

run "launch_settings_come_from_the_stack_variables" {
  command = plan

  variables {
    mlflow_host_ip               = "10.0.1.20"
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) : alltrue([
          for expected in [
            "AWS_REGION=us-west-2",
            "HAI_LAUNCH_CLUSTER=example-project-dev",
            "HAI_LAUNCH_FAMILY_PREFIX=example_project_dev_job_",
            "HAI_LAUNCH_PROJECT_TAG=example_project",
            "HAI_LAUNCH_LOG_GROUP=example_project_jobs_dev",
            "HAI_LAUNCH_HOST_PROVIDER=example-project-dev-host",
            "HAI_LAUNCH_BUCKET=example-project-data-dev",
            "HAI_LAUNCH_SIGNED_PARAMETER=/healthcare_hai_prediction/first_publishes_signed",
            "HAI_LAUNCH_POLARIS_URI=http://10.0.1.20:8181/api/catalog",
            "HAI_LAUNCH_MLFLOW_URI=http://10.0.1.20:5100",
            "HAI_LAUNCH_SCRATCH_PATH=/opt/hai/job_scratch",
            "HAI_LAUNCH_POOL=host_jobs",
          ] : contains([for item in container.environment : "${item.name}=${item.value}"], expected)
        ]) if startswith(container.name, "airflow")
      ])
    ])
    error_message = "The launch settings name the cluster, the job definitions, the jobs log group, the host provider, the data bucket, the signed record and the host's catalog and MLflow addresses from the stack's variables."
  }
}

# Owner decision A (Nov 7 2026): the Airflow task that runs DAG steps runs as the Airflow launch role of infra/iam/airflow_launch.tf. Under the LocalExecutor
# every task runs inside the scheduler's task, so only the scheduler takes the role. The API server (with the migrate container), the DAG processor and the
# triggerer run no step and call no AWS API, so they keep the control-plane role, which holds no permission.
run "airflow_keeps_the_control_plane_role_without_a_launch_role" {
  command = plan

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : aws_ecs_task_definition.service[key].task_role_arn == var.control_plane_task_role_arn
    ])
    error_message = "Without airflow_launch_task_role_arn every Airflow task definition keeps the control-plane role."
  }
}

run "only_the_scheduler_runs_as_the_launch_role" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  assert {
    condition     = aws_ecs_task_definition.service["airflow_scheduler"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
    error_message = "The scheduler, which runs every DAG step under the LocalExecutor, runs as the launch role."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_dag_processor", "airflow_triggerer"] : aws_ecs_task_definition.service[key].task_role_arn == var.control_plane_task_role_arn
    ])
    error_message = "The API server, the DAG processor and the triggerer run no DAG step, so they keep the control-plane role and gain no launch right."
  }

  assert {
    condition = alltrue([
      for key, definition in merge(aws_ecs_task_definition.service, aws_ecs_task_definition.one_shot) :
      definition.task_role_arn != "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev" if key != "airflow_scheduler"
    ])
    error_message = "No task definition but the scheduler's names the launch role."
  }

  assert {
    condition     = aws_ecs_task_definition.service["airflow_scheduler"].execution_role_arn == var.execution_role_arn
    error_message = "The scheduler keeps the shared execution role: the launch role is a task role only."
  }
}

run "launch_role_that_is_not_a_role_arn_is_refused" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "example_project_airflow_launch_task_dev"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  expect_failures = [var.airflow_launch_task_role_arn]
}

run "launch_role_shared_with_the_control_plane_is_refused" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  expect_failures = [var.airflow_launch_task_role_arn]
}

run "launch_role_shared_with_polaris_is_refused" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example-polaris"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  expect_failures = [var.airflow_launch_task_role_arn]
}

run "launch_role_shared_with_mlflow_is_refused" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example-mlflow"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  expect_failures = [var.airflow_launch_task_role_arn]
}

# The launch path has its own inputs (review, Nov 7 2026): the bucket of the launch records and staged inputs is airflow_launch_bucket_name, and the host
# address of the catalog and MLflow is mlflow_host_ip. Both are required once the scheduler runs as the launch role, and broker_jobs feeds nothing.
run "launch_role_without_the_launch_bucket_is_refused" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
  }

  expect_failures = [var.airflow_launch_bucket_name]
}

run "launch_bucket_that_is_not_a_bucket_name_is_refused" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
    airflow_launch_bucket_name   = "s3://example-project-data-dev"
  }

  expect_failures = [var.airflow_launch_bucket_name]
}

run "launch_role_without_the_host_address_is_refused" {
  command = plan

  variables {
    mlflow_host_ip               = null
    mlflow_enabled               = false
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
    airflow_launch_bucket_name   = "example-project-data-dev"
  }

  expect_failures = [var.airflow_launch_task_role_arn]
}

run "the_broker_bucket_never_feeds_the_launch_path" {
  command = plan

  variables {
    airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example_project_airflow_launch_task_dev"
    airflow_launch_bucket_name   = "example-project-launch-dev"
    broker_jobs = {
      subnet_id          = "subnet-0example1"
      security_group_ids = { lakehouse = "sg-0example1", training = "sg-0example2" }
      log_group_name     = "example_project_jobs_dev"
      bucket_name        = "example-project-broker-dev"
      host_ip            = "10.0.1.99"
      families           = ["train"]
    }
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["airflow_scheduler"].container_definitions) : (
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_BUCKET=example-project-launch-dev") &&
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_POLARIS_URI=http://10.0.1.20:8181/api/catalog") &&
        !strcontains(jsonencode(container.environment), "example-project-broker-dev") && !strcontains(jsonencode(container.environment), "10.0.1.99")
      ) if startswith(container.name, "airflow")
    ])
    error_message = "The launch settings come from airflow_launch_bucket_name and mlflow_host_ip; the broker's bucket and host address reach no Airflow container."
  }
}

# Round 2 review H3: coalesce(null, "") is an error in Terraform, so without a launch role and a launch bucket the plan must still succeed, with the bucket
# setting empty and the scheduler on the control-plane role.
run "no_launch_role_and_no_launch_bucket_plan_with_an_empty_bucket_setting" {
  command = plan

  variables {
    airflow_launch_task_role_arn = null
    airflow_launch_bucket_name   = null
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) :
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_BUCKET=") if startswith(container.name, "airflow")
      ])
    ]) && aws_ecs_task_definition.service["airflow_scheduler"].task_role_arn == var.control_plane_task_role_arn
    error_message = "Without a launch role and a launch bucket every Airflow container gets an empty HAI_LAUNCH_BUCKET and the scheduler keeps the control-plane role."
  }
}

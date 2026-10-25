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
  polaris_task_role_arn       = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  data_mount_path             = "/mnt/example-data"
  marquez_enabled             = true

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
      for key, service in aws_ecs_service.service :
      one(service.deployment_circuit_breaker).enable && one(service.deployment_circuit_breaker).rollback && service.desired_count == 1
    ])
    error_message = "Every service keeps one task and stops a failing deployment through the circuit breaker."
  }

  assert {
    condition     = alltrue([for key, service in aws_ecs_service.service : service.deployment_minimum_healthy_percent == 0 && service.deployment_maximum_percent == 100])
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
          for entry in lookup(container, "environment", []) : !can(regex("://[^/@[:space:]]*:[^/@[:space:]$]+@", entry.value)) && !can(regex("^[A-Za-z0-9+/=_-]{32,}$", entry.value)) && !can(regex("^arn:aws:(secretsmanager|ssm):", entry.value))
        ]
      ]
    ]))
    error_message = "An environment value must not hold a URL with a password, a long random token or a secret reference."
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
    condition     = toset(flatten([for service in aws_ecs_service.service : service.name])) == toset([for key in keys(aws_ecs_service.service) : "example_project_dev_${key}"])
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
    condition     = length(aws_ecs_service.service) > 0 && alltrue([for service in aws_ecs_service.service : service.propagate_tags == "TASK_DEFINITION" && service.enable_ecs_managed_tags])
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
      for key, service in aws_ecs_service.service :
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
      && one(aws_ecs_task_definition.service["redis"].volume).host_path == "/mnt/example-data/redis"
      && contains([for volume in aws_ecs_task_definition.service["airflow_scheduler"].volume : volume.host_path], "/mnt/example-data/airflow/logs")
      && length(aws_ecs_task_definition.service["polaris"].volume) == 0
      && one(aws_ecs_task_definition.one_shot["host_prepare"].volume).host_path == "/mnt/example-data"
    )
    error_message = "Postgres, Redis and the Airflow logs live on the data volume; Polaris keeps no state."
  }

  assert {
    condition = alltrue(flatten([
      for key, definition in aws_ecs_task_definition.service : [
        for volume in definition.volume : startswith(volume.host_path, "/mnt/example-data/")
      ]
    ]))
    error_message = "Every host path lies below the data mount, including the default code checkout."
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
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chown 999:999 /host/postgres /host/redis")
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
      && can(regex("printf 'requirepass \"%s\"\\\\n' \"\\$pw\"", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
      && can(regex("exec redis-server - < <\\(", jsondecode(aws_ecs_task_definition.service["redis"].container_definitions)[0].command[0]))
    )
    error_message = "Redis listens on 6379 only with a password, read from standard input, and never evicts feature values."
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
      "airflow_apiserver/airflow_apiserver"         = "00313dc171ffca4bcd4565765fa3067028f4fb164618520e8fe861a0d2407dfc"
      "airflow_apiserver/airflow_migrate"           = "388c19dbf540c8594fb8e99f366f3c255aa15e989e8f5102a85fdb1dc0e2d817"
      "airflow_dag_processor/airflow_dag_processor" = "69f503115530a62c226b0bad91618715a7c32cf8f5290bcf59f3f71cbe901a38"
      "airflow_scheduler/airflow_scheduler"         = "48d0a6c62ccfc1e18efb5b16aace02ab9300b1adb5d7fcfef164b14b720c0849"
      "airflow_triggerer/airflow_triggerer"         = "8ce12daa5b8d49cbf30f32ac79a98dc73ed9a9cf7fb69deb7bdf21b9ec7b5285"
      "host_prepare/host_prepare"                   = "c8d9a035f5588b9b5cb65f649058842ec6a980b6a3a8880abe6b306d8c953e1e"
      "marquez/marquez_api"                         = "9df0f81c5440fa10c2df2fc83d01edb1b5e09b06c2f5cbc1930659d87fae1033"
      "marquez/marquez_web"                         = "4ff0cc6590b24265a20486f1b4aeab92bf28afeb6eceb93ea83d022d279743c9"
      "polaris/polaris"                             = "4091249731a1ee59e36999c93cacc38ad67f1c8be37c13308b5b65f3d673e487"
      "polaris_bootstrap/polaris_bootstrap"         = "0d33ef24e4e1060358256973fc2d678892abd56956b4b533a3cc5dd00de0d5a1"
      "postgres/postgres"                           = "84ffdb77ff2045be5b3abcf4437443a9a14b42cd406a155da052da8c5c4d860f"
      "postgres/postgres_databases"                 = "d1832558975c44ccb1c94ed39efbe9e024edaa1ddd6f3161b9d7e95288db71f4"
      "redis/redis"                                 = "583fcf4e2db9335bd827c6b0cf2fc7b2428b7c187ba72e1acbcb247d4a01db10"
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
    condition     = toset(output.excluded_fields_exercised) == toset(["memory", "memoryReservation"])
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
        "container.mountPoints",
        "container.mountPoints[0].containerPath",
        "container.secrets[0].valueFrom",
        "container.healthCheck.command",
        "container.healthCheck.command[1]",
        "container.logConfiguration.options.awslogs-group",
        "container.command[0]",
        "container.entryPoint[1]",
        "container.portMappings[0].hostPort",
        "container.ulimits[0].name",
        "task.volume[0].host_path",
        "task.volume[0].name",
        "task.tags.Project",
      ] : contains(output.mutation_paths["redis/redis"], path)
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
      redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"containerPath\":\"/data\"", "\"containerPath\":\"/usr/local/bin\"")
    }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
    error_message = "Mounting the Redis data volume over /usr/local/bin can hide the executables the approved script runs, so it must change the execution digest."
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
    tasks       = jsondecode(replace(jsonencode(run.container_definitions_for_review.task_definition_settings), "/mnt/example-data/redis", "/mnt/example-data/other"))
    definitions = { redis = run.container_definitions_for_review.container_definitions["redis"] }
  }

  assert {
    condition     = output.execution_digests["redis/redis"] != run.container_definitions_match_the_approved_versions.execution_digests["redis/redis"]
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
    definitions = { redis = replace(run.container_definitions_for_review.container_definitions["redis"], "\"memory\":1024", "\"memory\":2048") }
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

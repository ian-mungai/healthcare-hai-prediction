mock_provider "aws" {}

# The Run 3 publisher runs only on the host: one EC2 bridge definition <project>_<environment>_job_publisher_host that the direct ECS launch starts
# (orchestration/publisher_launch.py). It has no Fargate twin, no awsvpc network and no public IP; the host firewall and the approved hosts file bound
# where it connects.

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  job_role_arns = {
    execution      = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    publisher_task = "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev"
  }
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  log_group_name = "example_project_jobs_dev"
  # TEST FIXTURE ONLY, not measured values: the services outputs host_job_memory_mib and host_capacity_binding for a 30720 MiB usable host. Like every
  # host job, the publisher gets the whole job room and carries the binding.
  host_job_memory_mib   = 23552
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
}

run "default_registers_no_publisher" {
  command = plan
  variables {
    images         = {}
    job_role_arns  = {}
    log_group_name = null
  }
  assert {
    condition     = length(aws_ecs_task_definition.publisher_host) == 0 && output.publisher_host_run_task == null && output.publisher_host_container_definitions == null
    error_message = "Disabled defaults register no publisher definition and add no publisher output."
  }
}

run "publisher_is_a_bridge_task_on_the_host_with_its_own_role" {
  command = plan
  variables {
    publisher_job_enabled = true
  }
  assert {
    condition = (
      aws_ecs_task_definition.publisher_host[0].family == "example_project_dev_job_publisher_host" &&
      aws_ecs_task_definition.publisher_host[0].requires_compatibilities == toset(["EC2"]) &&
      aws_ecs_task_definition.publisher_host[0].network_mode == "bridge" &&
      aws_ecs_task_definition.publisher_host[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev" &&
      aws_ecs_task_definition.publisher_host[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      length(aws_ecs_task_definition.publisher_host[0].ephemeral_storage) == 0
    )
    error_message = "The publisher is an EC2 bridge task named <project>_<environment>_job_publisher_host with its own task role and no Fargate ephemeral storage."
  }
  assert {
    condition     = aws_ecs_task_definition.publisher_host[0].cpu == "2048" && aws_ecs_task_definition.publisher_host[0].memory == "8192"
    error_message = "The publisher keeps its own size, 2048 CPU units and 8192 MiB, the row of config/orchestration/host_jobs.json."
  }
  assert {
    condition = (
      { for volume in aws_ecs_task_definition.publisher_host[0].volume : volume.name => volume.host_path } ==
      { workspace = "/data/job_scratch/publisher/work", tmp = "/data/job_scratch/publisher/tmp" }
    )
    error_message = "The publisher's two folders are on the host scratch volume, under its own family folder."
  }
}

run "publisher_init_container_carries_the_capacity_binding" {
  command = plan
  variables {
    publisher_job_enabled = true
  }
  assert {
    condition = (
      jsondecode(output.publisher_host_container_definitions)[0].name == "init" &&
      jsondecode(output.publisher_host_container_definitions)[0].dockerLabels == { "hai.host_capacity" = replace(var.host_capacity_binding, "/^job_memory_mib=[0-9]+/", "job_memory_mib=8192") } &&
      alltrue([for container in slice(jsondecode(output.publisher_host_container_definitions), 1, 3) : !contains(keys(container), "dockerLabels")])
    )
    error_message = "The init container of the publisher revision carries the capacity binding it was sized from (label hai.host_capacity), which the launch preflight checks, exactly as the host jobs do; no other container carries a label."
  }
  assert {
    condition     = tostring(regex("^job_memory_mib=([0-9]+) ", jsondecode(output.publisher_host_container_definitions)[0].dockerLabels["hai.host_capacity"])[0]) == aws_ecs_task_definition.publisher_host[0].memory
    error_message = "The task memory of the publisher revision is the job memory of its own binding label, which the launch preflight requires."
  }
}

run "publisher_definition_equals_the_fixture_the_airflow_mode_rig_reads" {
  command = plan
  variables {
    publisher_job_enabled = true
  }
  assert {
    condition = {
      containers              = [for container in jsondecode(output.publisher_host_container_definitions) : container.name]
      start_with_scratch_wipe = [for container in jsondecode(output.publisher_host_container_definitions) : container.name if startswith(container.command[0], "find /workspace /tmp -mindepth 1 -delete && ")]
      memory_mib              = tonumber(aws_ecs_task_definition.publisher_host[0].memory)
      init_labels             = jsondecode(output.publisher_host_container_definitions)[0].dockerLabels
    } == jsondecode(file("${path.module}/tests/publisher_host_definition.json")).publisher
    error_message = "The publisher definition differs from tests/publisher_host_definition.json, which the Airflow mode rig registers as the publisher's revision and passes through the launch preflight. If the change is intended, update the file to what the plan gives."
  }
}

run "publisher_follows_the_job_room_the_services_stack_gives" {
  command = plan
  variables {
    publisher_job_enabled = true
    host_job_memory_mib   = 20480
    host_capacity_binding = "job_memory_mib=20480 usable_memory_mib=27648 system_reserve_mib=2048 service_reservations_mib=5120"
  }
  assert {
    condition     = aws_ecs_task_definition.publisher_host[0].memory == "8192" && aws_ecs_task_definition.publisher_host[0].cpu == "2048"
    error_message = "The publisher keeps its own size whatever the room is, as long as it fits in it."
  }
}

run "publisher_needs_the_capacity_binding" {
  command = plan
  variables {
    publisher_job_enabled = true
    host_capacity_binding = null
  }
  expect_failures = [var.host_capacity_binding]
}

run "publisher_needs_the_job_memory" {
  command = plan
  variables {
    publisher_job_enabled = true
    host_job_memory_mib   = null
  }
  expect_failures = [var.host_job_memory_mib]
}

run "publisher_refuses_a_binding_for_another_job_memory" {
  command = plan
  variables {
    publisher_job_enabled = true
    host_capacity_binding = "job_memory_mib=20480 usable_memory_mib=27648 system_reserve_mib=2048 service_reservations_mib=5120"
  }
  expect_failures = [aws_ecs_task_definition.publisher_host]
}

run "publisher_needs_its_task_role" {
  command = plan
  variables {
    publisher_job_enabled = true
    job_role_arns         = { execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev" }
  }
  expect_failures = [var.publisher_job_enabled]
}

run "publisher_containers_fetch_the_code_then_run_the_publisher" {
  command = plan
  variables {
    publisher_job_enabled = true
  }
  assert {
    condition     = [for container in jsondecode(output.publisher_host_container_definitions) : container.name] == ["init", "fetch", "run"]
    error_message = "The publisher prepares its folders, fetches the code archive, then runs."
  }
  assert {
    condition = alltrue([
      for container in jsondecode(output.publisher_host_container_definitions) :
      container.essential == (container.name == "run") && container.readonlyRootFilesystem && length(container.secrets) == 0 &&
      container.user == (container.name == "init" ? "0:0" : "10001:10001") &&
      (container.name == "init" || container.entryPoint[0] == "/opt/hai/deadline_run") &&
      container.logConfiguration.options["awslogs-group"] == "example_project_jobs_dev"
    ])
    error_message = "Only the run container is essential; every container has a read-only root, no secret and the job user after init, the later ones start through the deadline entry point and all log to the jobs group."
  }
  assert {
    condition = (
      startswith(jsondecode(output.publisher_host_container_definitions)[0].command[0], "find /workspace /tmp -mindepth 1 -delete && ") &&
      strcontains(jsondecode(output.publisher_host_container_definitions)[2].command[0], "python -m scripts.acquisition.publisher_run") &&
      endswith(jsondecode(output.publisher_host_container_definitions)[2].command[0], ") && find /workspace /tmp -mindepth 1 -delete")
    )
    error_message = "Like every host job, init wipes what the last task left and the run phase wipes its folders only after it succeeds."
  }
  assert {
    condition = alltrue([
      for item in [
        { name = "HAI_CAPTURE_ROOT", value = "raw/" },
        { name = "EXPECTED_ACCOUNT_ID", value = "111111111111" },
        { name = "TASK_ROLE_NAME", value = "example_project_job_publisher_task_dev" },
      ] : contains(jsondecode(output.publisher_host_container_definitions)[2].environment, item)
    ])
    error_message = "The run container captures under raw/ unless the launch names a test folder, and runs as the publisher task role."
  }
}

run "the_key_switch_sets_its_variable_on_the_run_container_and_no_filtered_switch_exists" {
  command = plan
  variables {
    publisher_job_enabled  = true
    publisher_keys_enabled = true
  }
  assert {
    condition = (
      contains(jsondecode(output.publisher_host_container_definitions)[2].environment, { name = "HAI_TASK_KEYS", value = "1" }) &&
      alltrue([for container in jsondecode(output.publisher_host_container_definitions) : !contains([for variable in container.environment : variable.name], "HAI_TASK_FILTERED")])
    )
    error_message = "The key switch reaches the run container. All the data is public, so no container sets a filtered switch."
  }
}

run "publisher_launch_names_the_variables_the_launch_request_sets" {
  command = plan
  variables {
    publisher_job_enabled = true
  }
  assert {
    condition = output.publisher_host_run_task == {
      task_definition   = "example_project_dev_job_publisher_host"
      task_role_key     = "publisher_task"
      capacity_provider = "example_project_host_dev"
      network_mode      = "bridge"
      network           = "publisher"
      capture_root      = "raw/"
      propagate_tags    = "TASK_DEFINITION"
      container_environment = {
        fetch = ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "HAI_DEADLINE_UTC"]
        run   = ["RUN_ID", "CODE_SHA256", "PUBLISHER_RUN_KIND", "HAI_CAPTURE_ROOT", "PUBLISHER_ORIGINAL_RUN", "PUBLISHER_RETRY_DAY", "HAI_DEADLINE_UTC"]
      }
      scratch = { workspace = "/data/job_scratch/publisher/work", tmp = "/data/job_scratch/publisher/tmp" }
    }
    error_message = "The launch output names the host definition, the capacity provider, bridge networking with no public IP and the containers' run variables, which orchestration/publisher_launch.py sets."
  }
  assert {
    condition = alltrue([
      for container in jsondecode(output.publisher_host_container_definitions) : alltrue([
        for name in lookup(output.publisher_host_run_task.container_environment, container.name, []) : contains(container.environment, { name = name, value = name == "HAI_CAPTURE_ROOT" ? "raw/" : "" })
      ])
    ])
    error_message = "Each run variable exists empty in its container, except the capture root, which defaults to raw/, so the launch only overrides it."
  }
  assert {
    condition = alltrue([
      for container in jsondecode(output.publisher_host_container_definitions) : alltrue([
        for variable in container.environment : !contains(["PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256", "PUBLISHER_RESUME_SOURCES", "PUBLISHER_PRIOR_STATE", "PUBLISHER_REQUEST_SHA256", "HAI_STORED_MIRROR", "HAI_STORED_INDEX", "HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"], variable.name)
      ])
    ])
    error_message = "No container carries a variable of the staged inputs flow: the run takes its inputs from the code archive and names its approved hosts file itself."
  }
}

run "no_fargate_publisher_definition_remains" {
  command = plan
  variables {
    jobs_enabled                          = true
    enabled_definitions                   = ["publisher_fetch"]
    aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
    aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  }
  expect_failures = [var.enabled_definitions]
}

run "the_capture_root_is_declared_once_and_defaults_to_raw" {
  command = plan
  variables {
    publisher_job_enabled = true
  }
  assert {
    condition = alltrue([
      for container in jsondecode(output.publisher_host_container_definitions) :
      length([for variable in container.environment : variable.name]) == length(toset([for variable in container.environment : variable.name]))
    ])
    error_message = "No container declares an environment name twice."
  }
  assert {
    condition = (
      length([for variable in jsondecode(output.publisher_host_container_definitions)[2].environment : variable if variable.name == "HAI_CAPTURE_ROOT"]) == 1 &&
      !contains([for variable in jsondecode(output.publisher_host_container_definitions)[1].environment : variable.name], "HAI_CAPTURE_ROOT") &&
      output.publisher_host_run_task.capture_root == "raw/"
    )
    error_message = "Only the run container holds the capture root, once, and its default stays raw/."
  }
}

run "publisher_may_not_reserve_more_than_the_host_job_cap" {
  command = plan

  variables {
    publisher_job_enabled = true
    host_job_memory_mib   = 4096
    host_capacity_binding = "job_memory_mib=4096 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  # The publisher row of config/orchestration/host_jobs.json reserves 8192 MiB. Under a 4096 MiB job cap the launch preflight would refuse every launch of
  # that revision, so the plan must refuse before the jobs stack registers it, as it does for every other host family.
  expect_failures = [aws_ecs_task_definition.publisher_host[0]]
}

run "publisher_fits_a_cap_that_holds_its_row" {
  command = plan

  variables {
    publisher_job_enabled = true
    host_job_memory_mib   = 8192
    host_capacity_binding = "job_memory_mib=8192 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  assert {
    condition     = aws_ecs_task_definition.publisher_host[0].memory == "8192" && aws_ecs_task_definition.publisher_host[0].cpu == "2048"
    error_message = "A cap equal to the publisher row still registers the publisher at its own size."
  }
}

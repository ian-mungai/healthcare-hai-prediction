mock_provider "aws" {
  override_during = plan
}

# The Evidently UI runs as one small service behind evidently_ui_enabled. It serves the snapshots of the on-demand live drift check (monitoring.live_check) from a
# folder of the drift job family that the jobs stack writes and this service only reads. These cases pin the flag-off plan, the one container and its limits, the
# read-only workspace, the loopback port and the memory rule that recomputes the host job cap. Every case reads the plan only: no image runs and nothing reaches AWS.
variables {
  # TEST FIXTURE ONLY, not a measured value: the host job cap (host_job_cap_mib) these plans use.
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
  data_mount_path             = "/data"

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
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/drift"
  }

  image_digests = {
    postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    drift         = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
  }
}

run "flag_off_plans_no_evidently_ui" {
  command = plan

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.service), "evidently_ui") && !contains(keys(aws_ecs_service.service), "evidently_ui")
    error_message = "With evidently_ui_enabled false the stack plans no Evidently UI task or service."
  }

  assert {
    condition     = var.evidently_ui_enabled == false
    error_message = "The Evidently UI is off by default."
  }
}

run "flag_on_adds_one_loopback_service_on_the_pinned_drift_image" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
  }

  assert {
    condition     = contains(keys(aws_ecs_service.service), "evidently_ui") && contains(keys(aws_ecs_task_definition.service), "evidently_ui")
    error_message = "evidently_ui_enabled adds the Evidently UI service and its task definition."
  }

  assert {
    condition = (
      aws_ecs_task_definition.service["evidently_ui"].task_role_arn == "arn:aws:iam::111111111111:role/example-evidently-ui" &&
      aws_ecs_task_definition.service["evidently_ui"].execution_role_arn == "arn:aws:iam::111111111111:role/example-execution" &&
      aws_ecs_task_definition.service["evidently_ui"].network_mode == "host" &&
      aws_ecs_service.service["evidently_ui"].desired_count == 1
    )
    error_message = "The service runs one task on the host network with its own task role and the shared execution role."
  }

  assert {
    condition = (
      length(jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)) == 1 &&
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].name == "evidently_ui" &&
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/drift@sha256:9999999999999999999999999999999999999999999999999999999999999999"
    )
    error_message = "One container runs the existing drift image, pinned by its digest. No new image or repository exists."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].command == [
        "python", "-c", "from evidently.legacy.cli import app; app()", "ui", "--host", "127.0.0.1", "--port", "8600", "--workspace", "/workspace"
      ] &&
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].portMappings == [{ containerPort = 8600, hostPort = 8600, protocol = "tcp" }]
    )
    error_message = "The legacy Evidently UI of release 0.7.23 (the API of the report that drift_check runs) binds the host loopback on port 8600, never 8000 (the model API)."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].memory == 512 &&
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].memoryReservation == 256
    )
    error_message = "The container reserves 256 MiB and may use 512 MiB: the measured idle size is 237 MiB with four snapshots."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].user == "10001:10001" &&
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].readonlyRootFilesystem &&
      length(lookup(jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0], "secrets", [])) == 0 &&
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].healthCheck.command == [
        "CMD", "python", "-c", "import urllib.request as u; u.urlopen('http://127.0.0.1:8600/api/version', timeout=3)"
      ]
    )
    error_message = "The UI runs as the numeric job user (which owns the workspace) on a read-only root with no secret, and is healthy when /api/version answers."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions)[0].mountPoints == [
        { sourceVolume = "evidently_workspace", containerPath = "/workspace", readOnly = true }
      ] &&
      [for volume in aws_ecs_task_definition.service["evidently_ui"].volume : volume.host_path if volume.name == "evidently_workspace"] == ["/data/job_scratch/drift/evidently_workspace"]
    )
    error_message = "The UI mounts the workspace folder of the drift job family read-only. The folder is the one infra/jobs mounts into the drift check, so a job writes and the UI only reads."
  }
}

run "the_ui_port_is_free_on_the_host" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
  }

  assert {
    condition = length(flatten([
      for key, definition in aws_ecs_task_definition.service : [
        for container in jsondecode(definition.container_definitions) : [for mapping in lookup(container, "portMappings", []) : mapping.hostPort if mapping.protocol == "tcp"]
      ]
      ])) == length(distinct(flatten([
        for key, definition in aws_ecs_task_definition.service : [
          for container in jsondecode(definition.container_definitions) : [for mapping in lookup(container, "portMappings", []) : mapping.hostPort if mapping.protocol == "tcp"]
        ]
    ])))
    error_message = "No two containers of the planned services map the same TCP host port."
  }
}

run "the_ui_without_a_task_role_is_refused" {
  command = plan

  variables {
    evidently_ui_enabled = true
  }

  expect_failures = [var.evidently_ui_task_role_arn]
}

run "the_ui_without_the_drift_image_is_refused" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
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

run "the_ui_shares_no_role_with_another_service" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  }

  expect_failures = [var.evidently_ui_task_role_arn]
}

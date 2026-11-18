mock_provider "aws" {
  override_during = plan
}

# The Evidently UI runs as one small service behind evidently_ui_enabled. It serves the snapshots of the on-demand live drift check (monitoring.live_check) from a
# folder of the drift job family that the jobs stack writes and this service only reads. A restore step runs first in the same task: it rebuilds that folder from the
# completed runs under drift_demo/ in S3. These cases pin the flag-off plan, the two containers and their limits, the read-only workspace of the UI, the restore step's
# own mounts and rights, the loopback port and the memory rule that recomputes the host job cap. Every case reads the plan only: no image runs and nothing reaches AWS.
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
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/drift"
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training"
  }

  image_digests = {
    postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    drift         = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    training      = "sha256:8888888888888888888888888888888888888888888888888888888888888888"
  }
  mlflow_enabled                  = false
  monitoring_enabled              = false
  model_api_enabled               = false
  streamlit_enabled               = false
  openmetadata_persistent_enabled = false
  airflow_lineage_http_enabled    = false
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

run "flag_on_adds_one_loopback_service_with_a_restore_step" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
    evidently_ui_bucket_name   = "example-project-data-dev"
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
      [for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.name] == ["evidently_ui_restore", "evidently_ui"] &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.image if container.name == "evidently_ui"]) == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/drift@sha256:9999999999999999999999999999999999999999999999999999999999999999" &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.image if container.name == "evidently_ui_restore"]) == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training@sha256:8888888888888888888888888888888888888888888888888888888888888888"
    )
    error_message = "The UI runs the existing drift image and the restore step the existing training image (it holds boto3), each pinned by its digest. No new image or repository exists."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.command if container.name == "evidently_ui"]) == [
        "python", "-c", "from evidently.legacy.cli import app; app()", "ui", "--host", "127.0.0.1", "--port", "8600", "--workspace", "/workspace"
      ] &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.portMappings if container.name == "evidently_ui"]) == [{ containerPort = 8600, hostPort = 8600, protocol = "tcp" }]
    )
    error_message = "The legacy Evidently UI of release 0.7.23 (the API of the report that drift_check runs) binds the host loopback on port 8600, never 8000 (the model API)."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.memory if container.name == "evidently_ui"]) == 512 &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.memoryReservation if container.name == "evidently_ui"]) == 256 &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.memory if container.name == "evidently_ui_restore"]) == 256 &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.memoryReservation if container.name == "evidently_ui_restore"]) == 16
    )
    error_message = "The UI reserves 256 MiB and may use 512 MiB (measured idle size 237 MiB with four snapshots). The restore step, which runs once before it, reserves 16 MiB and may use 256 MiB."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.user if container.name == "evidently_ui"]) == "10001:10001" &&
      alltrue([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.readonlyRootFilesystem && length(lookup(container, "secrets", [])) == 0]) &&
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.healthCheck.command if container.name == "evidently_ui"]) == [
        "CMD", "python", "-c", "import urllib.request as u; u.urlopen('http://127.0.0.1:8600/api/version', timeout=3)"
      ]
    )
    error_message = "The UI runs as the numeric job user on a read-only root, healthy when /api/version answers. No container holds a secret, and both roots are read-only."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.mountPoints if container.name == "evidently_ui"]) == [
        { sourceVolume = "evidently_workspace", containerPath = "/workspace", readOnly = true }
      ] &&
      [for volume in aws_ecs_task_definition.service["evidently_ui"].volume : volume.host_path if volume.name == "evidently_workspace"] == ["/data/job_scratch/drift/evidently_workspace"]
    )
    error_message = "The UI mounts the workspace folder of the drift job family read-only. The folder is the one infra/jobs mounts into the drift check, so a job writes and the UI only reads."
  }
}

run "the_restore_step_runs_first_reads_s3_and_writes_only_the_workspace" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
    evidently_ui_bucket_name   = "example-project-data-dev"
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.dependsOn if container.name == "evidently_ui"]) == [{ containerName = "evidently_ui_restore", condition = "SUCCESS" }] &&
      !one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.essential if container.name == "evidently_ui_restore"])
    )
    error_message = "The UI starts only after the restore step ended with success, and the restore step is not essential, so it ends and stays ended."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.command if container.name == "evidently_ui_restore"]) == [
        "cd /opt/hai/repo && python -m monitoring.live_restore --bucket \"$DATA_BUCKET_NAME\" --workspace /workspace && chown -R 10001:10001 /workspace"
      ] &&
      contains(one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.environment if container.name == "evidently_ui_restore"]), { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" })
    )
    error_message = "The restore step runs monitoring.live_restore from the reviewed checkout for the data bucket, then hands the rebuilt folder to the job user, which the UI and the drift check run as."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.mountPoints if container.name == "evidently_ui_restore"]) == [
        { sourceVolume = "evidently_workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "code_repo", containerPath = "/opt/hai/repo", readOnly = true }
      ] &&
      [for volume in aws_ecs_task_definition.service["evidently_ui"].volume : volume.host_path if volume.name == "code_repo"] == ["/data/repo"] &&
      !one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.privileged if container.name == "evidently_ui_restore"])
    )
    error_message = "The restore step writes only the workspace folder and reads the code checkout read-only."
  }

  assert {
    condition = (
      one([for container in jsondecode(aws_ecs_task_definition.service["evidently_ui"].container_definitions) : container.linuxParameters.capabilities if container.name == "evidently_ui_restore"]) == { drop = ["ALL"], add = ["CHOWN", "DAC_OVERRIDE", "FOWNER"] }
    )
    error_message = "The restore step runs as root with only the capabilities to change owners and to read and write the folder it hands over."
  }
}

run "the_ui_port_is_free_on_the_host" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
    evidently_ui_bucket_name   = "example-project-data-dev"
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
    evidently_ui_enabled     = true
    evidently_ui_bucket_name = "example-project-data-dev"
  }

  expect_failures = [var.evidently_ui_task_role_arn]
}

run "the_ui_without_a_bucket_name_is_refused" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
  }

  expect_failures = [var.evidently_ui_bucket_name]
}

run "the_ui_without_the_drift_image_is_refused" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
    evidently_ui_bucket_name   = "example-project-data-dev"
    image_digests = {
      postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      training      = "sha256:8888888888888888888888888888888888888888888888888888888888888888"
    }
  }

  expect_failures = [var.image_digests]
}

run "the_ui_without_the_training_image_is_refused" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-evidently-ui"
    evidently_ui_bucket_name   = "example-project-data-dev"
    image_digests = {
      postgres      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      redis         = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      polaris       = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
      polaris_admin = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
      airflow       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      drift         = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    }
  }

  expect_failures = [var.image_digests]
}

run "the_ui_shares_no_role_with_another_service" {
  command = plan

  variables {
    evidently_ui_enabled       = true
    evidently_ui_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
    evidently_ui_bucket_name   = "example-project-data-dev"
  }

  expect_failures = [var.evidently_ui_task_role_arn]
}

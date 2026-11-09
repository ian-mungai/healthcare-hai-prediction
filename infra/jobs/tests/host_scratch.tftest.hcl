mock_provider "aws" {}

# Every family has its source switched on, so each run can compare the host definitions with the Fargate definitions the same inputs register.
variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  host_job_memory_mib = 23552
  log_group_name      = "example_project_jobs_dev"
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  drift_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_drift_dev@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  }
  openmetadata_images = {
    server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_server_dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
    ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
    search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_search_dev@sha256:3333333333333333333333333333333333333333333333333333333333333333"
    database  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_database_dev@sha256:4444444444444444444444444444444444444444444444444444444444444444"
  }
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    write_bronze_task   = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  }
  train_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    train_task = "arn:aws:iam::111111111111:role/example_project_job_train_task_dev"
  }
  drift_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    drift_task = "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev"
  }
  serve_roles = {
    serve_task = "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev"
  }
  openmetadata_roles = {
    execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
  }
  writer_parameter_names = {
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
    write_bronze_task   = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"]
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  mlflow_admin_password_name            = "/example_project/mlflow_admin_password"
  drift_mlflow_password_name            = "/example_project/mlflow_admin_password"
  openmetadata_admin_password_name      = "/example_project/openmetadata_admin_password"
  serve_manifest_prefix                 = "serving/deployments"
  layer_feast_redis_host                = "10.0.1.20"
  openmetadata_cpu_architecture         = "ARM64"
  openmetadata_ingestion_token_name     = "/example_project/openmetadata_bot_ingestion"
  host_openmetadata_api                 = "http://10.0.1.20:8585/api"
  host_repo_path                        = "/data/repo"
  layer_jobs_enabled                    = true
  bronze_job_enabled                    = true
  train_enabled                         = true
  drift_enabled                         = true
  promote_enabled                       = true
  serve_enabled                         = true
  openmetadata_enabled                  = true
}


# Failure modes pinned before the change:
# - a job starts on files left by the previous job of its family, so a failed run's output or a stale input is read again;
# - the wipe runs after another step or reaches beyond the two scratch folders;
# - the init container cannot delete files the job user wrote, because it dropped the capabilities that let root delete them,
#   so every job stops at its first step;
# - the Fargate definition, which needs no wipe, changes with the host definition;
# - a successful job leaves its files on the shared volume, so the next job of another family finds less room than it needs;
# - the metadata job, which had only one container, starts on the files of its previous attempt;
# - the cleanup runs although the last phase failed, so the files that show the failure are gone, or it runs before the evidence is durable;
# - the cleanup is a container that starts only after the essential last phase has ended, which stops the task first, so it never runs;
# - the cleanup reaches the folders of another family or the read-only repository checkout.

run "every_host_init_wipes_the_two_scratch_folders_first" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        jsondecode(output.host_container_definitions[family])[0].name == "init" &&
        startswith(jsondecode(output.host_container_definitions[family])[0].command[0], "find /workspace /tmp -mindepth 1 -delete && ") &&
        length(regexall("find ", jsondecode(output.host_container_definitions[family])[0].command[0])) == 1 &&
        !strcontains(jsondecode(output.host_container_definitions[family])[0].command[0], "rm ") &&
        jsondecode(output.host_container_definitions[family])[0].entryPoint == ["sh", "-c"] &&
        jsondecode(output.host_container_definitions[family])[0].user == "0:0" &&
        jsondecode(output.host_container_definitions[family])[0].dependsOn == []
      )
    ])
    error_message = "The first step of each job on the host, the metadata job included, wipes only the contents of /workspace and /tmp, as root, before any other command."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        strcontains(jsondecode(output.host_container_definitions[family])[0].command[0], "chown 10001:10001 /workspace /tmp")
      )
    ])
    error_message = "The wipe is followed by the init step the Fargate twin runs, which hands the folders to the job user."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        jsondecode(output.host_container_definitions[family])[0].image == jsondecode(output.host_container_definitions[family])[1].image || family == "metadata"
      )
    ]) && jsondecode(output.host_container_definitions["metadata"])[0].image == var.openmetadata_images["ingestion"]
    error_message = "The metadata init step runs in the ingestion image, the only image of that job, and the other families keep the init image of their Fargate twin."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        toset([for mount in jsondecode(output.host_container_definitions[family])[0].mountPoints : "${mount.sourceVolume}:${mount.containerPath}:${mount.readOnly}"]) ==
        toset(["workspace:/workspace:false", "tmp:/tmp:false"])
      )
    ])
    error_message = "The init step mounts the family's two scratch folders and nothing else, so its wipe cannot reach the repository checkout or another family."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        jsondecode(output.host_container_definitions[family])[1].dependsOn == [{ containerName = "init", condition = "SUCCESS" }]
      )
    ])
    error_message = "The first phase waits for the success of the init step in every family."
  }
}

run "host_init_keeps_the_capabilities_that_let_root_delete_the_job_users_files" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        !contains(jsondecode(output.host_container_definitions[family])[0].linuxParameters.capabilities.drop, "DAC_OVERRIDE") &&
        !contains(jsondecode(output.host_container_definitions[family])[0].linuxParameters.capabilities.drop, "FOWNER") &&
        jsondecode(output.host_container_definitions[family])[0].linuxParameters.capabilities.add == [] &&
        toset(jsondecode(output.host_container_definitions[family])[0].linuxParameters.capabilities.drop) == toset(["FSETID", "MKNOD", "NET_RAW", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE"])
      )
    ])
    error_message = "The host init step keeps DAC_OVERRIDE and FOWNER, so root can delete files the job user wrote, and still drops every other capability the Fargate init drops."
  }

  assert {
    condition = alltrue(flatten([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : [
        for container in slice(jsondecode(output.host_container_definitions[family]), 1, length(jsondecode(output.host_container_definitions[family]))) :
        container.linuxParameters.capabilities.drop == ["ALL"] && container.user == "10001:10001"
      ]
    ]))
    error_message = "Every step after init still drops all capabilities and runs as the job user."
  }
}

run "every_host_job_cleans_its_scratch_after_its_last_phase_succeeds" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  # The task stops when its essential container ends, so a cleanup in a later container would never start. The last phase is the essential container
  # and runs the cleanup itself, joined with && so that it follows only a success of the whole phase, after the phase has made its evidence durable.
  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        endswith(one([for container in jsondecode(output.host_container_definitions[family]) : container if container.essential && container.name != "guard"]).command[0], ") && find /workspace /tmp -mindepth 1 -delete") &&
        startswith(one([for container in jsondecode(output.host_container_definitions[family]) : container if container.essential && container.name != "guard"]).command[0], "(") &&
        slice(one([for container in jsondecode(output.host_container_definitions[family]) : container if container.essential && container.name != "guard"]).entryPoint, length(one([for container in jsondecode(output.host_container_definitions[family]) : container if container.essential && container.name != "guard"]).entryPoint) - 2, length(one([for container in jsondecode(output.host_container_definitions[family]) : container if container.essential && container.name != "guard"]).entryPoint)) == ["sh", "-c"]
      )
    ])
    error_message = "The last phase of every host job, the metadata job included, ends with a cleanup of /workspace and /tmp that runs only after the whole phase succeeded."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        length([for container in jsondecode(output.host_container_definitions[family]) : container if container.name != "guard" && container.essential]) == 1 &&
        jsondecode(output.host_container_definitions[family])[length([for container in jsondecode(output.host_container_definitions[family]) : container if container.name != "guard"]) - 1].essential &&
        !contains([for container in jsondecode(output.host_container_definitions[family]) : container.name], "cleanup")
      )
    ])
    error_message = "The cleanup is no container of its own: only one container stays essential, the last phase, and no container could start after it."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        length([for container in jsondecode(output.host_container_definitions[family]) : container if container.name != "guard" && strcontains(container.command[0], "find ")]) == 2 &&
        alltrue([
          for container in jsondecode(output.host_container_definitions[family]) :
          length(regexall("find ", container.command[0])) == length(regexall("find /workspace /tmp -mindepth 1 -delete", container.command[0]))
          if container.name != "guard"
        ]) &&
        alltrue([
          for container in jsondecode(output.host_container_definitions[family]) :
          !strcontains(container.command[0], "rm ") && !strcontains(container.command[0], "..") && !strcontains(container.command[0], var.host_scratch_root)
          if container.name != "guard"
        ])
      )
    ])
    error_message = "Only the init step and the last phase delete anything, only the contents of /workspace and /tmp, and no command names the scratch root, a parent folder or the repository checkout."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] : (
        alltrue([
          for container in jsondecode(output.host_container_definitions[family]) :
          alltrue([for mount in container.mountPoints : contains(["workspace", "tmp", "repo"], mount.sourceVolume) && (mount.sourceVolume != "repo" || mount.readOnly)])
          if container.name != "guard"
        ]) &&
        alltrue([
          for name, path in {
            for volume in aws_ecs_task_definition.host[family].volume : volume.name => volume.host_path
          } : name == "repo" ? path == var.host_repo_path : startswith(path, "${var.host_scratch_root}/${family}/")
        ])
      )
    ])
    error_message = "A cleanup can reach only the family's own two scratch folders: their host folders lie under <root>/<family>/ and the repository checkout stays read-only."
  }
}

run "the_cleanup_keeps_the_phase_command_and_follows_only_its_success" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([
      for pair in [
        { family = "silver", fargate = output.layer_container_definitions["silver"] },
        { family = "gold", fargate = output.layer_container_definitions["gold"] },
        { family = "features", fargate = output.layer_container_definitions["features"] },
        { family = "bronze", fargate = output.bronze_container_definitions },
        { family = "train", fargate = output.train_container_definitions },
        { family = "drift", fargate = output.drift_container_definitions },
        { family = "promote", fargate = output.promote_container_definitions },
        { family = "serve", fargate = output.serve_container_definitions },
        ] : (
        [for container in jsondecode(output.host_container_definitions[pair.family]) : container if container.name != "guard"][length([for container in jsondecode(output.host_container_definitions[pair.family]) : container if container.name != "guard"]) - 1].command[0] ==
        format("(%s) && find /workspace /tmp -mindepth 1 -delete", [for container in jsondecode(pair.fargate) : container if container.name != "guard"][length([for container in jsondecode(pair.fargate) : container if container.name != "guard"]) - 1].command[0])
      )
    ])
    error_message = "The host cleanup wraps the unchanged command of the last Fargate phase in a group and follows it with &&, so a failed phase keeps its files."
  }

  assert {
    condition = (
      jsondecode(output.host_container_definitions["metadata"])[1].name == "ingest" &&
      jsondecode(output.host_container_definitions["metadata"])[1].command[0] == "(python -m scripts.infrastructure.openmetadata_host_ingest --config-dir /opt/hai/repo/config/metadata/ingestion --work-dir /workspace polaris dbt) && find /workspace /tmp -mindepth 1 -delete" &&
      jsondecode(output.host_container_definitions["metadata"])[1].dependsOn == [{ containerName = "init", condition = "SUCCESS" }]
    )
    error_message = "The metadata ingestion command is unchanged inside the group, joined to the cleanup with &&, and it waits for the init step."
  }
}

run "the_fargate_definitions_need_no_wipe_and_do_not_change" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue(flatten([
      for json in [
        output.layer_container_definitions["silver"], output.bronze_container_definitions, output.train_container_definitions,
        output.drift_container_definitions, output.promote_container_definitions, output.serve_container_definitions
      ] : [for container in jsondecode(json) : !strcontains(jsonencode(container.command), "find /workspace")]
    ]))
    error_message = "The Fargate definitions start with an empty workspace and carry no wipe or cleanup."
  }

  assert {
    condition = (
      [for container in jsondecode(output.layer_container_definitions["silver"]) : container.name][0] == "init" &&
      alltrue([for json in [output.bronze_container_definitions, output.train_container_definitions, output.drift_container_definitions, output.promote_container_definitions, output.serve_container_definitions] : jsondecode(json)[length(jsondecode(json)) - 1].essential])
    )
    error_message = "The last Fargate phase stays the essential container."
  }
}

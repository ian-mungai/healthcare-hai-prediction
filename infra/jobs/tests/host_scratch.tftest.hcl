mock_provider "aws" {}

# Every family has its source switched on, so each run can compare the host definitions with the Fargate definitions the same inputs register.
variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
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
# - the Fargate definition, which needs no wipe, changes with the host definition.

run "every_host_init_wipes_the_two_scratch_folders_first" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve"] : (
        jsondecode(output.host_container_definitions[family])[0].name == "init" &&
        startswith(jsondecode(output.host_container_definitions[family])[0].command[0], "find /workspace /tmp -mindepth 1 -delete && ") &&
        length(regexall("find ", jsondecode(output.host_container_definitions[family])[0].command[0])) == 1 &&
        !strcontains(jsondecode(output.host_container_definitions[family])[0].command[0], "rm ") &&
        jsondecode(output.host_container_definitions[family])[0].entryPoint == ["sh", "-c"] &&
        jsondecode(output.host_container_definitions[family])[0].user == "0:0" &&
        jsondecode(output.host_container_definitions[family])[0].dependsOn == []
      )
    ])
    error_message = "The first step of each job on the host wipes only the contents of /workspace and /tmp, as root, before any other command."
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve"] : (
        strcontains(jsondecode(output.host_container_definitions[family])[0].command[0], "chown 10001:10001 /workspace /tmp")
      )
    ])
    error_message = "The wipe is followed by the init step the Fargate twin runs, which hands the folders to the job user."
  }
}

run "host_init_keeps_the_capabilities_that_let_root_delete_the_job_users_files" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve"] : (
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
      for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve"] : [
        for container in slice(jsondecode(output.host_container_definitions[family]), 1, length(jsondecode(output.host_container_definitions[family]))) :
        container.linuxParameters.capabilities.drop == ["ALL"] && container.user == "10001:10001"
      ]
    ]))
    error_message = "Every step after init still drops all capabilities and runs as the job user."
  }
}

run "the_fargate_definitions_and_the_metadata_job_need_no_wipe_and_do_not_change" {
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
    error_message = "The Fargate definitions start with an empty workspace and carry no wipe."
  }

  assert {
    condition     = length(jsondecode(output.host_container_definitions["metadata"])) == 1 && jsondecode(output.host_container_definitions["metadata"])[0].name == "ingest"
    error_message = "The metadata job keeps its one ingestion container, which the cleanup after a run and the host preparation cover."
  }
}

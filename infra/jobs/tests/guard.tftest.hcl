mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
  }
  writer_parameter_names = {
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  log_group_name                        = "example_project_jobs_dev"
  layer_feast_redis_host                = "10.0.1.20"
}

# A phase that exits with an error does not stop a layer task: the phases after it wait for its success and never start, and the only essential container never
# runs, so the task stays PENDING until somebody stops it. The guard container is a second essential container that ends the task when a phase has failed.

run "a_failed_phase_ends_the_task_through_the_guard" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      jsondecode(output.layer_container_definitions[layer])[length(jsondecode(output.layer_container_definitions[layer])) - 1].name == "guard" &&
      length(jsondecode(output.layer_container_definitions[layer])) == length(jsondecode(output.layer_phase_containers[layer])) + 1 &&
      [for container in jsondecode(output.layer_container_definitions[layer]) : container.name if container.essential] == [
        jsondecode(output.layer_phase_containers[layer])[length(jsondecode(output.layer_phase_containers[layer])) - 1].name, "guard"
      ]
    )])
    error_message = "Each layer task ends with a guard container, and the last phase and the guard are the only essential containers, so a failed phase stops the task."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).dependsOn == [] &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).entryPoint == ["python", "-I", "-c"] &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).command == [file("${path.module}/../../scripts/orchestration/task_guard.py")] &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).image == var.images.spark
    )])
    error_message = "The guard starts with the task, with no dependency, and runs scripts/orchestration/task_guard.py in the Spark image."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).user == "10001:10001" &&
      !one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).privileged &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).readonlyRootFilesystem &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).linuxParameters.capabilities.drop == ["ALL"] &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).secrets == [] &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).mountPoints == [] &&
      one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).logConfiguration.options["awslogs-stream-prefix"] == layer
    )])
    error_message = "The guard is unprivileged with a read-only root, no capability, no secret and no volume, and logs to the layer's stream prefix."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      tonumber(one([for item in one([for container in jsondecode(output.layer_container_definitions[layer]) : container if container.name == "guard"]).environment : item.value if item.name == "GUARD_SECONDS"]))
      >= jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[layer].deadline_seconds + 1800
    )])
    error_message = "The guard's time limit is not shorter than the job deadline plus the time the fetch phase may take, so it never ends a run that is within its deadline."
  }
}

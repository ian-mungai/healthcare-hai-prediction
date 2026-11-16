mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["eu-west-1a", "eu-west-1b"] }
  }

  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev" }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = { arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev" }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values          = { id = "lt-0123456789abcdef0", latest_version = 1 }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values          = { arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev" }
}

variables {
  aws_profile         = "example_project_dev"
  environment         = "dev"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
  owner_ip_cidrs      = []
}



run "layer_run_grants_default_off" {
  command = plan

  assert {
    condition = (
      !var.job_layer_runs_prepared &&
      length(aws_iam_role_policy.job_writer_layer_input) == 0 &&
      length(aws_iam_role_policy.job_writer_layer_upload) == 0 &&
      length(aws_iam_role_policy.job_writer_features_feast) == 0 &&
      length(aws_iam_role_policy.job_writer_layer_dbt) == 0 &&
      output.job_layer_runs_contract.prepared == false
    )
    error_message = "The layer run grants default to off and create no policy."
  }
}

run "layer_run_grants_need_the_prepared_writer_roles" {
  command = plan

  variables {
    job_layer_runs_prepared = true
  }

  expect_failures = [var.job_layer_runs_prepared]
}

run "each_layer_reads_only_its_input_layer" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.job_writer_layer_input)) == toset(["silver", "gold", "features"]) &&
      aws_iam_role_policy.job_writer_layer_input["silver"].role == aws_iam_role.job_write_layer_task["silver"].name &&
      aws_iam_role_policy.job_writer_layer_input["gold"].role == aws_iam_role.job_write_layer_task["gold"].name &&
      aws_iam_role_policy.job_writer_layer_input["features"].role == aws_iam_role.job_write_layer_task["features"].name &&
      alltrue([for layer, source in { silver = "bronze", gold = "silver", features = "gold" } : (
        length(jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement) == 3 &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[0].Action == ["s3:GetObject"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/${source}/*"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[1].Action == ["s3:ListBucket"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[1].Condition.StringLike["s3:prefix"] == ["lakehouse/${source}/*"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[2].Effect == "Allow" &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[2].Action == ["s3:ListBucketVersions"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[2].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
        toset(jsondecode(aws_iam_role_policy.job_writer_layer_input[layer].policy).Statement[2].Condition.StringLike["s3:prefix"]) == toset(layer == "features" ? ["lakehouse/gold/*", "lakehouse/features/*"] : ["lakehouse/${source}/*"])
      )])
    )
    error_message = "Silver reads bronze, gold reads silver and features reads gold. Each gets object reads, a scoped listing and a scoped version listing of that one prefix and nothing else, except that features also lists the versions of its own prefix, which a model set pins."
  }
}

run "only_silver_and_gold_upload_and_only_under_their_own_run_prefix" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.job_writer_layer_upload)) == toset(["silver", "gold"]) &&
      alltrue([for layer in ["silver", "gold"] : (
        aws_iam_role_policy.job_writer_layer_upload[layer].role == aws_iam_role.job_write_layer_task[layer].name &&
        length(jsondecode(aws_iam_role_policy.job_writer_layer_upload[layer].policy).Statement) == 1 &&
        jsondecode(aws_iam_role_policy.job_writer_layer_upload[layer].policy).Statement[0].Action == ["s3:PutObject"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_upload[layer].policy).Statement[0].Resource == concat(
          ["arn:aws:s3:::example-project-ci-bucket/lakehouse/layer_runs/${layer}/*"],
          [for name in ["manifest.json", "catalog.json", "run_results.json"] : "arn:aws:s3:::example-project-ci-bucket/metadata/dbt/${name}" if layer == "gold"]
        )
      )])
    )
    error_message = "Silver and gold may put objects under lakehouse/layer_runs/<layer>/ and nowhere else. Gold may also put the three fixed dbt keys the catalog reads today, and no other metadata/dbt/ key. Features uploads nothing through this grant."
  }
}

run "features_may_write_only_its_run_state" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.job_writer_layer_state)) == toset(["features"]) &&
      aws_iam_role_policy.job_writer_layer_state["features"].role == aws_iam_role.job_write_layer_task["features"].name &&
      length(jsondecode(aws_iam_role_policy.job_writer_layer_state["features"].policy).Statement) == 1 &&
      jsondecode(aws_iam_role_policy.job_writer_layer_state["features"].policy).Statement[0].Action == ["s3:PutObject"] &&
      jsondecode(aws_iam_role_policy.job_writer_layer_state["features"].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/layer_runs/features/*/state/*"]
    )
    error_message = "Features puts only the claim object of its run: lakehouse/layer_runs/features/<run>/state/. It uploads nothing else."
  }
}

run "only_features_may_apply_feast" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.job_writer_features_feast)) == toset(["features"]) &&
      aws_iam_role_policy.job_writer_features_feast["features"].role == aws_iam_role.job_write_layer_task["features"].name &&
      length(jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement) == 7 &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[0].Action == ["s3:GetObject", "s3:PutObject"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/lakehouse/feast/registry.pb"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[1].Action == ["ssm:GetParameter"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[1].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/redis_password"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[2].Action == ["s3:GetObject"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[2].Resource == ["arn:aws:s3:::example-project-ci-bucket/promote/production/*"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[3].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[3].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[3].Condition.StringLike["s3:prefix"] == ["promote/production/*"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[4].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[4].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[4].Condition.StringLike["s3:prefix"] == ["promote/models/*/"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[5].Action == ["s3:GetObject"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[5].Resource == [for name in ["online_hold.json", "online_release.json", "feast_materialize.json"] : "arn:aws:s3:::example-project-ci-bucket/lakehouse/layer_runs/features/*/${name}"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[6].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[6].Resource == "arn:aws:s3:::example-project-ci-bucket" &&
      jsondecode(aws_iam_role_policy.job_writer_features_feast["features"].policy).Statement[6].Condition.StringLike["s3:prefix"] == [for name in ["online_hold.json", "online_release.json", "feast_materialize.json"] : "lakehouse/layer_runs/features/*/${name}"]
    )
    error_message = "The features role reads and writes the one Feast registry object, reads the one Redis password parameter, reads and lists the Production records under promote/production/ lists the promotion lease records of each model under promote/models/<model>/ and, for the release phase, reads and lists the hold, release and load records of a features run (online_hold.json, online_release.json and feast_materialize.json under lakehouse/layer_runs/features/) only. No other layer receives any of them."
  }
}

run "layer_run_contract_names_the_grants" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      output.job_layer_runs_contract.prepared &&
      jsonencode(output.job_layer_runs_contract.inputs) == jsonencode({ features = "gold", gold = "silver", silver = "bronze" }) &&
      jsonencode(output.job_layer_runs_contract.uploads) == jsonencode({ gold = "lakehouse/layer_runs/gold/", silver = "lakehouse/layer_runs/silver/" }) &&
      output.job_layer_runs_contract.production_activation == false
    )
    error_message = "The contract output names the input layer and the upload prefix of each layer and activates nothing."
  }
}

run "each_layer_copies_its_dbt_files_only_under_its_own_prefix" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = (
      toset(keys(aws_iam_role_policy.job_writer_layer_dbt)) == toset(["silver", "gold", "features"]) &&
      alltrue([for layer in ["silver", "gold", "features"] : (
        aws_iam_role_policy.job_writer_layer_dbt[layer].role == aws_iam_role.job_write_layer_task[layer].name &&
        length(jsondecode(aws_iam_role_policy.job_writer_layer_dbt[layer].policy).Statement) == 1 &&
        jsondecode(aws_iam_role_policy.job_writer_layer_dbt[layer].policy).Statement[0].Effect == "Allow" &&
        jsondecode(aws_iam_role_policy.job_writer_layer_dbt[layer].policy).Statement[0].Action == ["s3:GetObject", "s3:PutObject"] &&
        jsondecode(aws_iam_role_policy.job_writer_layer_dbt[layer].policy).Statement[0].Resource == ["arn:aws:s3:::example-project-ci-bucket/metadata/dbt/${layer}/*"]
      )])
    )
    error_message = "Each layer writer may put and read back only its own dbt copies under metadata/dbt/<layer>/, and no other layer's."
  }
}

run "each_layer_writer_reaches_only_its_own_objects" {
  command = plan

  variables {
    job_writer_layers_prepared = true
    job_layer_runs_prepared    = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      toset(flatten([
        [for resource in try(jsondecode(aws_iam_role_policy.job_writer_layer_upload[layer].policy).Statement[0].Resource, []) : resource if layer != "features"],
        jsondecode(aws_iam_role_policy.job_writer_layer_dbt[layer].policy).Statement[0].Resource,
        [for resource in jsondecode(aws_iam_role_policy.job_writer_layer_state["features"].policy).Statement[0].Resource : resource if layer == "features"],
        ])) == toset(concat(
        [for prefix in ["lakehouse/layer_runs/${layer}/*"] : "arn:aws:s3:::example-project-ci-bucket/${prefix}" if layer != "features"],
        ["arn:aws:s3:::example-project-ci-bucket/metadata/dbt/${layer}/*"],
        [for name in ["manifest.json", "catalog.json", "run_results.json"] : "arn:aws:s3:::example-project-ci-bucket/metadata/dbt/${name}" if layer == "gold"],
        [for rest in ["*/state/*"] : "arn:aws:s3:::example-project-ci-bucket/lakehouse/layer_runs/features/${rest}" if layer == "features"],
      ))
    )])
    error_message = "The combined upload, dbt copy and claim grants of each layer writer name only its own run folder (silver and gold), its own dbt prefix, the features claim objects and, for gold, the three fixed dbt keys: no wildcard reaches another layer's objects."
  }
}

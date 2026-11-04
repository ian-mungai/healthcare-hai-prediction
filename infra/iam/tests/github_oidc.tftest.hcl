mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = {
      insecure_value = "ami-0123456789abcdef0"
    }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = {
      locations = ["us-west-2a", "us-west-2b", "us-west-2c"]
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:us-west-2:111111111111:example_project_budget_alerts_dev"
    }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = {
      arn = "arn:aws:autoscaling:us-west-2:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev"
    }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values = {
    id             = "lt-0123456789abcdef0"
    latest_version = 1
  }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values = {
    arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev"
  }
}

override_resource {
  target          = aws_iam_openid_connect_provider.github
  override_during = plan
  values = {
    arn = "arn:aws:iam::111111111111:oidc-provider/token.actions.githubusercontent.com"
  }
}

variables {
  aws_region          = "us-west-2"
  admin_profile       = "example_admin"
  aws_profile         = "example_project_dev"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-data-bucket"
}

run "delivery_roles_are_off_by_default" {
  command = plan

  assert {
    condition = (
      length(aws_iam_openid_connect_provider.github) == 0 &&
      length(aws_iam_role.github) == 0 &&
      length(aws_iam_role_policy.github) == 0 &&
      output.github_oidc_contract.enabled == false
    )
    error_message = "With the flag off the stack must plan no provider, role, policy or attachment for GitHub delivery."
  }
}

run "repository_must_be_one_exact_owner_and_name" {
  command = plan

  variables {
    github_oidc_enabled = true
    github_repository   = "ian-mungai/*"
  }

  expect_failures = [var.github_repository]
}

run "repository_without_a_slash_is_refused" {
  command = plan

  variables {
    github_repository = "healthcare-hai-prediction"
  }

  expect_failures = [var.github_repository]
}

run "enabled_provider_and_three_separate_roles" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      aws_iam_openid_connect_provider.github[0].url == "https://token.actions.githubusercontent.com" &&
      join(",", aws_iam_openid_connect_provider.github[0].client_id_list) == "sts.amazonaws.com"
    )
    error_message = "The provider must be GitHub's issuer with the sts.amazonaws.com audience only."
  }

  assert {
    condition = (
      join(",", sort(keys(aws_iam_role.github))) == "deploy,image_push,terraform_plan" &&
      aws_iam_role.github["image_push"].name == "example_project_gh_image_push_dev" &&
      aws_iam_role.github["deploy"].name == "example_project_gh_deploy_dev" &&
      aws_iam_role.github["terraform_plan"].name == "example_project_gh_terraform_plan_dev"
    )
    error_message = "Each purpose must have its own role with the project prefix."
  }
}

run "trust_binds_this_repository_and_named_environments" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = alltrue([
      for key, role in aws_iam_role.github :
      jsondecode(role.assume_role_policy).Statement[0].Principal.Federated == "arn:aws:iam::111111111111:oidc-provider/token.actions.githubusercontent.com" &&
      jsondecode(role.assume_role_policy).Statement[0].Action == "sts:AssumeRoleWithWebIdentity" &&
      jsondecode(role.assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:aud"] == "sts.amazonaws.com" &&
      !contains(keys(jsondecode(role.assume_role_policy).Statement[0].Condition), "StringLike") &&
      length(jsondecode(role.assume_role_policy).Statement) == 1
    ])
    error_message = "Every trust policy must be one exact web identity statement for the audience, with no pattern match."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.github["image_push"].assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:sub"] == ["repo:ian-mungai/healthcare-hai-prediction:environment:image-push"] &&
      jsondecode(aws_iam_role.github["deploy"].assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:sub"] == ["repo:ian-mungai/healthcare-hai-prediction:environment:model-api-deploy"]
    )
    error_message = "Push and deploy roles must trust one protected environment of this repository each."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.github["terraform_plan"].assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:sub"] ==
      ["repo:ian-mungai/healthcare-hai-prediction:pull_request", "repo:ian-mungai/healthcare-hai-prediction:environment:terraform-plan"]
    )
    error_message = "Only the read-only plan role may trust pull requests."
  }

  assert {
    condition = alltrue([
      for key, role in aws_iam_role.github :
      !strcontains(role.assume_role_policy, "*") && !strcontains(role.assume_role_policy, "pull_request_target") && !strcontains(role.assume_role_policy, "refs/heads")
    ])
    error_message = "No trust policy may hold a wildcard, a target-event subject or a branch subject that a fork could not be told apart from."
  }
}

run "only_the_plan_role_trusts_pull_requests" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = alltrue([
      for key, role in aws_iam_role.github :
      key == "terraform_plan" || !strcontains(role.assume_role_policy, ":pull_request")
    ])
    error_message = "Pull request tokens must reach only the read-only plan role."
  }
}

run "custom_permissions_stay_inside_the_approved_list" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = length(output.github_oidc_actions_outside_approved_list) == 0
    error_message = "Every allowed action of the push, deploy and plan roles must appear in a project policy of infra/iam/policies."
  }
}

run "plan_role_holds_no_write_and_no_secret_read" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : [
        for action in(statement.Effect == "Allow" && statement.Sid != "SaveTerraformPlans" ? tolist(statement.Action) : []) :
        can(regex("^[a-z0-9]+:(Describe|List|Get|BatchGet)", action)) &&
        !contains(["secretsmanager:GetSecretValue", "ssm:GetParameter", "ssm:GetParameters", "logs:GetLogEvents", "logs:FilterLogEvents", "ecr:GetAuthorizationToken", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"], action)
      ]
    ]))
    error_message = "The plan role may only describe, list or get, and may not read secret values, log events or image layers."
  }

  assert {
    condition = (
      !contains(flatten([for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : tolist(statement.Action) if statement.Effect == "Allow"]), "s3:DeleteObject") &&
      length([for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : statement if statement.Effect == "Allow" && contains(tolist(statement.Action), "s3:PutObject")]) == 1
    )
    error_message = "The plan role reads state and never writes it, so it plans without a lock. Its one write is the saved plan."
  }
}

run "state_access_is_per_stack_and_leaves_out_iam_and_storage" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "arn:aws:s3:::example-data-bucket/terraform_state/platform/terraform.tfstate") &&
      strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "arn:aws:s3:::example-data-bucket/terraform_state/services/terraform.tfstate") &&
      strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "arn:aws:s3:::example-data-bucket/terraform_state/jobs/terraform.tfstate") &&
      !strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "iam/terraform.tfstate") &&
      !strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "storage/terraform.tfstate")
    )
    error_message = "CI may read the platform, services and jobs state only, never the IAM or storage state."
  }
}

run "no_delivery_role_can_apply_or_write_state" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_model_api"]
  }

  # D18: GitHub runs Terraform plans with the read-only plan role and the owner applies them. No role may write state or hold a lock.
  assert {
    condition = (
      !contains(keys(aws_iam_role.github), "terraform_apply") &&
      !contains(keys(output.github_oidc_contract.roles), "terraform_apply") &&
      alltrue([
        for key, role in aws_iam_role_policy.github :
        !contains(flatten([for statement in jsondecode(role.policy).Statement : tolist(statement.Action) if statement.Effect == "Allow"]), "s3:DeleteObject") &&
        (key == "terraform_plan" || !contains(flatten([for statement in jsondecode(role.policy).Statement : tolist(statement.Action) if statement.Effect == "Allow"]), "s3:PutObject"))
      ]) &&
      !contains(output.github_oidc_boundary_allowed_actions, "s3:DeleteObject")
    )
    error_message = "GitHub Terraform is plan-only: no apply role exists, no delivery role or boundary deletes an object, and only the plan role writes (the saved plan, see the saved plan runs)."
  }
}

run "deploy_role_is_limited_to_named_services" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_model_api"]
  }

  assert {
    condition = (
      strcontains(aws_iam_role_policy.github["deploy"].policy, "arn:aws:ecs:us-west-2:111111111111:service/example_project_cluster_dev/example_project_dev_model_api") &&
      strcontains(aws_iam_role_policy.github["deploy"].policy, "ecs:UpdateService") &&
      !strcontains(aws_iam_role_policy.github["deploy"].policy, "iam:PassRole")
    )
    error_message = "Deploy may update only the named services and passes no role."
  }
}

run "deploy_without_a_named_service_updates_nothing" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.github["deploy"].policy, "ecs:UpdateService") && !strcontains(aws_iam_role_policy.github["deploy"].policy, "iam:PassRole")
    error_message = "Until the model API names its service the deploy role must hold no update or pass-role grant."
  }
}

run "image_push_is_limited_to_project_repositories" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      strcontains(aws_iam_role_policy.github["image_push"].policy, "arn:aws:ecr:us-west-2:111111111111:repository/example_project_*_dev") &&
      strcontains(aws_iam_role_policy.github["image_push"].policy, "ecr:PutImage") &&
      !strcontains(aws_iam_role_policy.github["image_push"].policy, "ecr:DeleteRepository") &&
      !strcontains(aws_iam_role_policy.github["image_push"].policy, "ecr:CreateRepository")
    )
    error_message = "Image push may push to project repositories and may not create or delete them."
  }
}

run "plan_role_is_denied_every_object_read_except_its_state_keys" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      statement.Effect == "Deny" &&
      !contains(keys(statement), "Resource") &&
      join(",", sort(tolist(statement.Action))) == "s3:GetObject,s3:GetObjectAttributes,s3:GetObjectTagging,s3:GetObjectVersion" &&
      join(",", sort(tolist(statement.NotResource))) == join(",", sort([
        "arn:aws:s3:::example-data-bucket/terraform_state/jobs/terraform.tfstate",
        "arn:aws:s3:::example-data-bucket/terraform_state/platform/terraform.tfstate",
        "arn:aws:s3:::example-data-bucket/terraform_state/services/terraform.tfstate"
      ]))
    ])
    error_message = "The plan role needs an explicit Deny of every object read except its three exact state keys. Any pull request job can use it."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      !strcontains(jsonencode(statement.NotResource), "*") && !strcontains(jsonencode(statement.NotResource), "?")
      if statement.Effect == "Deny" && statement.Sid == "NeverReadObjectsExceptStateKeys"
    ])
    error_message = "The read deny's exceptions must be exact keys, never a pattern."
  }
}

run "plan_role_allows_object_reads_on_state_keys_only" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : [
        for resource in tolist(statement.Resource) :
        startswith(resource, "arn:aws:s3:::example-data-bucket/terraform_state/") && endswith(resource, "/terraform.tfstate")
      ]
      if statement.Effect == "Allow" && length([for action in tolist(statement.Action) : action if startswith(action, "s3:GetObject")]) > 0
    ]))
    error_message = "An allowed object read may name only a state key under terraform_state/."
  }

  assert {
    condition = (
      join(",", sort(flatten([for s in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : tolist(s.Resource) if s.Effect == "Allow" && s.Sid == "ReadStateObjects"]))) ==
      join(",", sort(flatten([for s in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : tolist(s.NotResource) if s.Effect == "Deny" && s.Sid == "NeverReadObjectsExceptStateKeys"])))
    )
    error_message = "The deny's exceptions must equal the allowed state keys exactly, so the state read stays possible and nothing else opens."
  }
}

run "plan_role_cannot_list_versions_or_uploads" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      !strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "s3:ListBucketVersions") &&
      !strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "s3:ListBucketMultipartUploads") &&
      !strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "s3:ListMultipartUploadParts")
    )
    error_message = "A plan refreshes bucket configuration and needs no version, upload or part listing of the data bucket."
  }
}

run "push_and_deploy_roles_hold_no_s3_action" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      !strcontains(aws_iam_role_policy.github["image_push"].policy, "s3:") &&
      !strcontains(aws_iam_role_policy.github["deploy"].policy, "s3:") &&
      !strcontains(aws_iam_role_policy.github["image_push"].policy, "secretsmanager:") &&
      !strcontains(aws_iam_role_policy.github["deploy"].policy, "ssm:")
    )
    error_message = "The push and deploy roles must not reach S3, the secret store or the parameter store."
  }
}

run "deny_leaves_the_storage_stack_refresh_working" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = length(output.github_oidc_storage_refresh_gaps) == 0
    error_message = "Every bucket configuration read that a refresh of the storage stack makes must stay allowed and must not fall under the object read deny."
  }
}

run "no_existing_policy_reaches_the_state_prefix" {
  command = plan

  assert {
    condition     = length(output.state_prefix_reach) == 0
    error_message = "A policy of the project user, a job, the lakehouse, Athena or a feature role must not allow an object under terraform_state/. The bucket policy denies them as well."
  }
}

run "state_prefix_matcher_catches_wide_grants" {
  command = plan

  assert {
    condition = (
      output.state_prefix_probe.bucket_wide && output.state_prefix_probe.state_wildcard && output.state_prefix_probe.stack_wildcard &&
      !output.state_prefix_probe.lakehouse && !output.state_prefix_probe.dvc
    )
    error_message = "The matcher must flag a bucket-wide grant, a terraform_state/* grant and a per-stack wildcard, and must not flag lakehouse/* or dvc/*."
  }
}

run "other_stacks_define_no_iam_resource_and_no_bucket_arn" {
  command = plan

  assert {
    condition     = length(output.state_prefix_reach_other_stacks) == 0
    error_message = "The platform, services and jobs stacks and the job modules take their roles from the IAM stack. A file there that defines an IAM resource or names an S3 ARN must be added to the reach check."
  }
}

run "plan_role_returns_configuration_never_content" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = alltrue([
      for action in ["ec2:GetConsoleOutput", "ec2:GetConsoleScreenshot", "ec2:GetPasswordData", "logs:GetLogEvents", "logs:FilterLogEvents", "logs:GetLogRecord", "logs:GetQueryResults", "secretsmanager:GetSecretValue", "ssm:GetParameter", "ssm:GetParameters", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"] :
      !strcontains(aws_iam_role_policy.github["terraform_plan"].policy, "\"${action}\"")
    ])
    error_message = "The plan role may describe, list and get configuration. It must hold no action that returns console output, logs, passwords, secret values or image layers."
  }
}

run "plan_role_keeps_the_resources_of_the_approved_statements" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  # ecr:ListImages and s3:ListBucket were allowed on every repository and bucket of the account. They must reach only the project's own.
  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : [
        for resource in tolist(statement.Resource) : resource != "*"
      ]
      if statement.Effect == "Allow" && anytrue([for action in tolist(statement.Action) : startswith(action, "ecr:") || startswith(action, "s3:")])
    ]))
    error_message = "No ECR or S3 read of the plan role may name every resource. A list reaches only the project repositories and buckets."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      contains(tolist(statement.Action), "ecr:ListImages") &&
      join(",", sort(tolist(statement.Resource))) == "arn:aws:ecr:us-west-2:111111111111:repository/example_project_*_dev"
      if statement.Effect == "Allow"
    ])
    error_message = "ecr:ListImages must reach only the project repositories (the prefix, any image and the environment) of this account and region."
  }

  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : [
        for resource in tolist(statement.Resource) : contains(["arn:aws:s3:::example-data-bucket", "arn:aws:s3:::example-data-bucket-logs"], resource)
      ]
      if statement.Effect == "Allow" && contains(tolist(statement.Action), "s3:ListBucket")
    ]))
    error_message = "s3:ListBucket may reach only the project data bucket and its logging bucket."
  }

  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : [
        for resource in tolist(statement.Resource) : !strcontains(resource, ":222222222222:") && !strcontains(resource, "other-bucket")
      ]
      if statement.Effect == "Allow"
    ]))
    error_message = "No allowed resource of the plan role may name another account or a bucket of the project's neighbours."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      statement.Condition.StringEquals["aws:RequestedRegion"] == "us-west-2"
      if statement.Effect == "Allow" && !contains(["ReadStateObjects", "ListStateKeys"], statement.Sid)
    ])
    error_message = "Every derived read of the plan role stays in the project region."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      contains(tolist(statement.Action), "dlm:GetLifecyclePolicy") && statement.Condition.StringEquals["aws:ResourceTag/Project"] == "example_project"
      if statement.Effect == "Allow"
    ])
    error_message = "A read that the project policy limits by resource tag must keep its tag condition."
  }

  assert {
    condition     = length(replace(aws_iam_role_policy.github["terraform_plan"].policy, " ", "")) < 10240
    error_message = "A role's inline policy may not pass the 10,240 character limit of IAM."
  }
}

run "plan_role_wildcard_reads_match_the_approved_wildcards" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = length(output.github_oidc_plan_wildcard_actions_outside_approved) == 0
    error_message = "An action the plan role may read on every resource must be one that a project policy already allows on every resource."
  }
}

run "every_delivery_role_carries_the_delivery_boundary" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      length(aws_iam_role.github) == 3 &&
      alltrue([for role in values(aws_iam_role.github) : role.permissions_boundary == "arn:aws:iam::111111111111:policy/example_project_delivery_boundary"])
    )
    error_message = "The push, deploy and plan roles must all carry the delivery boundary."
  }
}

run "the_delivery_boundary_allows_every_action_of_the_delivery_roles" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_model_api"]
  }

  assert {
    condition     = length(output.github_oidc_actions_outside_boundary) == 0
    error_message = "A delivery role allows an action that the delivery boundary does not allow, so the boundary would refuse it."
  }
}

run "the_delivery_boundary_allows_every_named_resource_of_the_delivery_roles" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_model_api"]
  }

  assert {
    condition     = length(output.github_oidc_grants_outside_boundary) == 0
    error_message = "A delivery role grants an action on a resource that the delivery boundary does not allow for that action."
  }
}

run "a_service_outside_the_boundary_names_fails_the_plan" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_model_api_dev", "other_project_dev_model_api"]
  }

  expect_failures = [aws_iam_role_policy.github]
}

run "the_delivery_boundary_holds_no_action_beyond_the_delivery_work" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = length(setintersection(toset(output.github_oidc_boundary_allowed_actions), toset([
      "ecr:CreateRepository", "ecr:DeleteRepository", "ecr:BatchDeleteImage", "ecr:PutLifecyclePolicy", "ecr:SetRepositoryPolicy",
      "ecs:CreateService", "ecs:DeleteService", "ecs:RunTask", "ecs:StopTask", "ecs:DeleteCluster", "ecs:DeregisterTaskDefinition",
      "s3:DeleteObjectVersion", "s3:PutBucketPolicy", "s3:PutLifecycleConfiguration", "s3:GetObjectVersion",
      "ssm:GetParameter", "ssm:GetParameters", "ssm:PutParameter", "secretsmanager:GetSecretValue", "logs:GetLogEvents", "logs:PutLogEvents",
      "ec2:RunInstances", "ec2:TerminateInstances", "iam:CreateRole", "iam:AttachRolePolicy", "iam:GetRole", "sts:AssumeRole", "budgets:ModifyBudget",
    ]))) == 0
    error_message = "The delivery boundary allows an action outside image push, deploy, plan reads and Terraform state."
  }
}

run "the_delivery_boundary_names_one_action_with_a_wildcard" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = [for action in output.github_oidc_boundary_allowed_actions : action if strcontains(action, "*")] == ["s3:GetBucket*"]
    error_message = "Only the bucket configuration reads may use a wildcard action, to keep the boundary under 6,144 characters."
  }
}

run "the_delivery_boundary_allows_no_iam_action" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = length([for action in output.github_oidc_boundary_allowed_actions : action if startswith(action, "iam:")]) == 0
    error_message = "No delivery role passes or manages a role, so the delivery boundary allows no IAM action."
  }
}

run "the_deploy_role_cannot_register_a_task_definition_or_pass_a_role" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_model_api"]
  }

  # A crafted task definition that names the shared execution role would pull the PostgreSQL and Polaris root secrets. The workflow only describes
  # definitions and updates services, so neither the role nor the boundary may register a definition or pass a role.
  assert {
    condition = (
      !strcontains(aws_iam_role_policy.github["deploy"].policy, "ecs:RegisterTaskDefinition") &&
      !strcontains(aws_iam_role_policy.github["deploy"].policy, "iam:PassRole") &&
      strcontains(aws_iam_role_policy.github["deploy"].policy, "ecs:DescribeTaskDefinition")
    )
    error_message = "The deploy role may describe task definitions and update the named services. It may not register a definition or pass a role."
  }

  assert {
    condition = (
      !contains(output.github_oidc_boundary_allowed_actions, "ecs:RegisterTaskDefinition") &&
      !contains(output.github_oidc_boundary_allowed_actions, "iam:PassRole")
    )
    error_message = "The delivery boundary may not allow task definition registration or iam:PassRole for any delivery role."
  }
}

run "the_deploy_role_updates_the_serving_services" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_model_api", "example_project_dev_streamlit"]
  }

  assert {
    condition     = length(output.github_oidc_grants_outside_boundary) == 0 && length(output.github_oidc_actions_outside_boundary) == 0
    error_message = "The model API and dashboard services must stay inside the delivery boundary."
  }
}

run "the_deploy_role_cannot_name_a_service_that_is_not_served" {
  command = plan

  variables {
    github_oidc_enabled         = true
    github_oidc_deploy_services = ["example_project_dev_airflow"]
  }

  expect_failures = [aws_iam_role_policy.github]
}

# IAM counts at most 6,144 characters, white space left out, in a managed policy. The delivery boundary holds the project, bucket, region, account and environment
# names many times, so it grows with them. The bounds are what the variable validations accept: a project name of at most 37 characters, so that the longest
# role name (<project>_job_openmetadata_task_prod) fits the 64 characters of an IAM role name, a 63 character bucket, a region code of at most 14 characters,
# a 12 digit account and the longer environment name.
run "the_delivery_boundary_fits_the_policy_limit_at_the_name_bounds" {
  command = plan

  override_data {
    target          = data.aws_ec2_instance_type_offerings.host
    override_during = plan
    values = {
      locations = ["ap-southeast-2a", "ap-southeast-2b", "ap-southeast-2c"]
    }
  }

  variables {
    aws_region          = "ap-southeast-2"
    expected_account_id = "999999999999"
    project_name        = "healthcare_hai_prediction_longest_nm0"
    aws_profile         = "healthcare_hai_prediction_longest_nm0_prod"
    environment         = "prod"
    data_bucket_name    = "healthcare-hai-prediction-data-bucket-0000000000000000000000000"
  }

  assert {
    condition     = output.github_oidc_boundary_size.actual == output.github_oidc_boundary_size.at_bounds
    error_message = "The names at the validation bounds must be the names the size check uses for its bound, or the check measures something else."
  }

  assert {
    condition     = output.github_oidc_boundary_size.at_bounds <= output.github_oidc_boundary_size.limit && output.github_oidc_boundary_size.limit == 6144
    error_message = "The delivery boundary must stay inside the 6,144 characters of a managed policy at the longest names the variables accept."
  }
}

run "the_delivery_boundary_size_follows_the_names" {
  command = plan

  assert {
    condition     = output.github_oidc_boundary_size.actual < output.github_oidc_boundary_size.at_bounds && output.github_oidc_boundary_size.actual > 3000
    error_message = "The size of the boundary for the real names must be measured, and shorter than the size at the bounds."
  }
}

run "a_project_name_over_the_bound_is_refused" {
  command = plan

  variables {
    project_name = "healthcare_hai_prediction_longest_nm01"
    aws_profile  = "healthcare_hai_prediction_longest_nm01_prod"
    environment  = "prod"
  }

  expect_failures = [var.project_name]
}

run "a_bucket_name_over_the_bound_is_refused" {
  command = plan

  variables {
    data_bucket_name = "healthcare-hai-prediction-data-bucket-00000000000000000000000000"
  }

  expect_failures = [var.data_bucket_name]
}

run "a_region_code_over_the_bound_is_refused" {
  command = plan

  variables {
    aws_region = "ap-southeast-22"
  }

  expect_failures = [var.aws_region]
}

run "the_delivery_boundary_names_the_shared_secrets_and_the_exact_state_keys" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      join(",", sort([for resource in output.github_oidc_boundary_allowed_resources : resource if strcontains(resource, ":secret:")])) == join(",", sort([
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:bls_api_key-??????",
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:census_api_key-??????",
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:hud_api_key-??????",
        "arn:aws:secretsmanager:us-west-2:111111111111:secret:jev_api_key-??????"
      ]))
    )
    error_message = "The boundary may describe the four shared API key secrets by name and no other secret. A pattern such as *_api_key would reach another project's key."
  }

  assert {
    condition = (
      join(",", sort([for resource in output.github_oidc_boundary_allowed_resources : resource if strcontains(resource, "terraform_state")])) == join(",", sort([
        "arn:aws:s3:::example-data-bucket/terraform_state/jobs/terraform.tfstate",
        "arn:aws:s3:::example-data-bucket/terraform_state/platform/terraform.tfstate",
        "arn:aws:s3:::example-data-bucket/terraform_state/services/terraform.tfstate"
      ]))
    )
    error_message = "The boundary may read the three exact state keys. terraform.tfstate.backup and the lock files stay out of reach."
  }

  assert {
    condition     = !anytrue([for resource in output.github_oidc_boundary_allowed_resources : strcontains(resource, "log-group:") && endswith(resource, "*") && !endswith(resource, ":*")])
    error_message = "A log group pattern must end at the environment name, so a log group with a suffix such as _devops stays out of reach."
  }
}

# D18 saved plans: CI saves each plan with its digest under one private prefix of the project bucket. The plan role may write there and nowhere else.
run "the_plan_role_writes_saved_plans_to_one_prefix_only" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      length([for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement : statement if statement.Sid == "SaveTerraformPlans"]) == 1 &&
      alltrue([
        for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
        statement.Effect == "Allow" &&
        join(",", tolist(statement.Action)) == "s3:PutObject" &&
        join(",", tolist(statement.Resource)) == "arn:aws:s3:::example-data-bucket/terraform_plans/*" &&
        statement.Condition.StringEquals["aws:RequestedRegion"] == "us-west-2"
        if statement.Sid == "SaveTerraformPlans"
      ])
    )
    error_message = "The plan role holds one Allow that puts objects under terraform_plans/ in the project bucket, with no other action and no other resource."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      !contains(tolist(statement.Action), "s3:PutObject") || statement.Sid == "SaveTerraformPlans" || statement.Effect == "Deny"
    ])
    error_message = "No other statement of the plan role may allow s3:PutObject."
  }

  assert {
    condition     = alltrue([for key, role in aws_iam_role_policy.github : !strcontains(role.policy, "terraform_plans") if key != "terraform_plan"])
    error_message = "The image push and deploy roles do not touch the saved plans."
  }
}

run "the_plan_role_cannot_read_list_or_delete_saved_plans" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      !strcontains(jsonencode(try(statement.Resource, [])), "terraform_plans")
      if statement.Effect == "Allow" && statement.Sid != "SaveTerraformPlans"
    ])
    error_message = "No other Allow of the plan role reaches the saved plans, so the role cannot read or list them."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      statement.Sid == "NeverReadObjectsExceptStateKeys" && !strcontains(jsonencode(statement.NotResource), "terraform_plans")
    ])
    error_message = "The explicit read Deny must still cover the saved plans, so a plan role cannot read another job's plan."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_iam_role_policy.github["terraform_plan"].policy).Statement :
      statement.Sid == "NeverWriteObjectsExceptSavedPlans" &&
      statement.Effect == "Deny" &&
      join(",", sort(tolist(statement.Action))) == "s3:DeleteObject,s3:DeleteObjectVersion,s3:PutObject,s3:PutObjectAcl" &&
      join(",", tolist(statement.NotResource)) == "arn:aws:s3:::example-data-bucket/terraform_plans/*"
    ])
    error_message = "An explicit Deny keeps every write and delete of the plan role under terraform_plans/, even if an allow above is widened later."
  }
}

run "the_delivery_boundary_writes_only_saved_plans" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition = (
      contains(output.github_oidc_boundary_allowed_actions, "s3:PutObject") &&
      join(",", [for resource in output.github_oidc_boundary_allowed_resources : resource if strcontains(resource, "terraform_plans")]) == "arn:aws:s3:::example-data-bucket/terraform_plans/*" &&
      length([for allow in output.github_oidc_boundary_write_resources : allow if !strcontains(allow, "terraform_plans")]) == 0
    )
    error_message = "The delivery boundary lets a delivery role put objects under terraform_plans/ and nowhere else."
  }
}

run "the_plan_save_stays_inside_the_boundary" {
  command = plan

  variables {
    github_oidc_enabled = true
  }

  assert {
    condition     = length(output.github_oidc_grants_outside_boundary) == 0 && length(output.github_oidc_actions_outside_boundary) == 0
    error_message = "The plan role's one write must be allowed by the delivery boundary, or the boundary refuses the save."
  }
}

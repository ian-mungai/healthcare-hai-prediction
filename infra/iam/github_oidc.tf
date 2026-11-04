# GitHub Actions delivery roles. The whole file is off by default (var.github_oidc_enabled): with the flag off every resource
# below has no instance, so the stack plans no change. The owner applies it with the administrator profile like every other
# role of this stack, because the project profile may not create roles.
#
# Three roles, one per purpose. Each trusts one exact web identity subject of the one repository, with no pattern:
#   image_push       environment image-push           pushes images to the project repositories
#   deploy           environment model-api-deploy     updates the named serving services
#   terraform_plan   pull requests, environment terraform-plan   read-only, plans without a lock
# No role applies Terraform (owner decision D18): GitHub plans with the read-only plan role and the owner applies the saved plan. Each role
# holds one inline policy and no attached policy, so the coverage checks (scripts/infrastructure/run_policy_e2e.py) see all it can do.
# A pull request subject reaches only the plan role. The environments carry the owner's approval and the main-only branch rule
# on the GitHub side; GitHub also issues no web identity token to a workflow that a fork pull request starts.
#
# The permissions stay inside the project profile's approved list (policies/*.json): every action of the roles is checked against the
# actions of the same files.
locals {
  github_issuer       = "token.actions.githubusercontent.com"
  github_subject_base = "repo:${var.github_repository}"
  # Every delivery role carries one permissions boundary that the administrator creates at bootstrap from infra/iam/boundary/. The
  # boundary policy must exist before the owner applies this file with the flag on. The delivery boundary holds exactly the grants of
  # the three roles below. The project role boundary cannot hold them: it has no room for them in the 6,144 characters IAM allows.
  github_boundary_file = "healthcare_hai_prediction_delivery_boundary_policy.json"
  github_boundary_arn  = "arn:aws:iam::${var.expected_account_id}:policy/${var.project_name}_delivery_boundary"

  # IAM counts at most 6,144 characters, white space left out, in a managed policy, and the boundary holds the names many times. The size is measured for
  # the real names and for the longest names the variable validations accept: a 37 character project name, a 63 character bucket, a 14 character region
  # code, a 12 digit account and the environment "prod". The test run "the_delivery_boundary_fits_the_policy_limit_at_the_name_bounds" sets exactly those.
  github_boundary_limit = 6144
  github_boundary_names = {
    actual = {
      AWS_REGION       = var.aws_region
      AWS_ACCOUNT_ID   = var.expected_account_id
      PROJECT_NAME     = var.project_name
      ENVIRONMENT      = var.environment
      DATA_BUCKET_NAME = var.data_bucket_name
    }
    at_bounds = {
      AWS_REGION       = join("", [for position in range(14) : "r"])
      AWS_ACCOUNT_ID   = join("", [for position in range(12) : "9"])
      PROJECT_NAME     = join("", [for position in range(37) : "p"])
      ENVIRONMENT      = "prod"
      DATA_BUCKET_NAME = join("", [for position in range(63) : "b"])
    }
  }
  github_boundary_size = {
    for case, names in local.github_boundary_names : case => length(jsonencode(jsondecode(templatefile("${path.module}/boundary/${local.github_boundary_file}", names))))
  }
  github_provider_arn = var.github_oidc_provider_arn != "" ? var.github_oidc_provider_arn : try(aws_iam_openid_connect_provider.github[0].arn, "")

  # The stacks that continuous delivery may plan. The IAM stack and the storage stack stay with the owner.
  github_state_stacks = ["platform", "services", "jobs"]
  github_state_prefix = "terraform_state"
  github_state_keys   = [for stack in local.github_state_stacks : "${local.github_state_prefix}/${stack}/terraform.tfstate"]
  github_state_bucket = "arn:aws:s3:::${var.data_bucket_name}"

  github_roles = var.github_oidc_enabled ? {
    image_push = {
      subjects    = ["${local.github_subject_base}:environment:image-push"]
      description = "GitHub Actions image push: pushes images to the project repositories from the image-push environment."
    }
    deploy = {
      subjects    = ["${local.github_subject_base}:environment:model-api-deploy"]
      description = "GitHub Actions deploy: updates the named model API service from the model-api-deploy environment."
    }
    terraform_plan = {
      subjects    = ["${local.github_subject_base}:pull_request", "${local.github_subject_base}:environment:terraform-plan"]
      description = "GitHub Actions Terraform plan: read-only, for pull requests of this repository. The owner applies the saved plan."
    }
  } : {}

  github_trust = {
    for key, role in local.github_roles : key => jsonencode({
      Version = "2012-10-17"
      Statement = [{
        Effect    = "Allow"
        Principal = { Federated = local.github_provider_arn }
        Action    = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${local.github_issuer}:aud" = "sts.amazonaws.com"
            "${local.github_issuer}:sub" = role.subjects
          }
        }
      }]
    })
  }

  # What the project profile's approved policies allow, read from the same files the policy scripts render.
  approved_policy_files = fileset("${path.module}/policies", "*.json")
  approved_actions = toset(flatten([
    for file in local.approved_policy_files : [
      for statement in jsondecode(file("${path.module}/policies/${file}")).Statement :
      [for action in flatten([statement.Action]) : action if statement.Effect == "Allow"]
    ]
  ]))

  # The plan role reads: describe, list and get actions of the approved list, less every action that returns a secret value, log
  # content, an image layer or a listing of the data bucket's versions and uploads. Object reads are excluded by prefix (s3:GetObject*).
  # Each read keeps the resources and the conditions of the approved statement that allows it, so a list or describe action reaches
  # only the project's own repositories, buckets, cluster and the like, and a statement that the project policies limit by tag keeps
  # its tag condition. Only the region condition is added. State is read by key below. A refresh of the storage stack reads bucket
  # configuration and heads the bucket (s3:ListBucket), and the stack has no object resource, so no object read needs an exception.
  github_plan_unreadable       = ["secretsmanager:GetSecretValue", "ssm:GetParameter", "ssm:GetParameters", "logs:GetLogEvents", "logs:FilterLogEvents", "ecr:GetAuthorizationToken", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ec2:GetConsoleOutput", "ec2:GetConsoleScreenshot", "ec2:GetPasswordData", "logs:GetLogRecord", "logs:GetQueryResults", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts"]
  github_policy_shared_secrets = [var.census_secret_name, var.bls_secret_name, var.hud_secret_name, var.jev_secret_name]
  github_plan_read_statements = flatten([
    for file in sort(tolist(local.approved_policy_files)) : [
      for statement in jsondecode(
        replace(
          replace(
            replace(
              replace(
                replace(file("${path.module}/policies/${file}"), "$${AWS_REGION}", var.aws_region),
                "$${AWS_ACCOUNT_ID}", var.expected_account_id
              ),
              "$${PROJECT_NAME}", var.project_name
            ),
            "$${ENVIRONMENT}", var.environment
          ),
          "$${DATA_BUCKET_NAME}", var.data_bucket_name
        )
        ).Statement : {
        Sid    = "Read${regex("_([a-z0-9]+)_policy\\.json$", file)[0]}${statement.Sid}"
        Effect = "Allow"
        Action = sort([
          for action in distinct(flatten([statement.Action])) : action
          if can(regex("^[a-z0-9]+:(Describe|List|Get|BatchGet)", action)) && !contains(local.github_plan_unreadable, action) && !startswith(action, "s3:GetObject")
        ])
        Resource = flatten([
          for resource in flatten([statement.Resource]) :
          strcontains(resource, "$${SHARED_SECRET_NAME}") ? [for name in local.github_policy_shared_secrets : replace(resource, "$${SHARED_SECRET_NAME}", name)] : [resource]
        ])
        Condition = merge(try(statement.Condition, {}), {
          StringEquals = merge(try(statement.Condition.StringEquals, {}), { "aws:RequestedRegion" = var.aws_region })
        })
      }
      if statement.Effect == "Allow" && !can(statement.NotResource) && length([
        for action in distinct(flatten([statement.Action])) : action
        if can(regex("^[a-z0-9]+:(Describe|List|Get|BatchGet)", action)) && !contains(local.github_plan_unreadable, action) && !startswith(action, "s3:GetObject")
      ]) > 0
    ]
  ])
  github_plan_actions = sort(distinct(flatten([for statement in local.github_plan_read_statements : statement.Action])))

  # Actions that the project policies allow on every resource. The plan role may read an action on every resource only if it is one of these.
  approved_wildcard_actions = toset(flatten([
    for file in local.approved_policy_files : [
      for statement in jsondecode(file("${path.module}/policies/${file}")).Statement :
      flatten([statement.Action]) if statement.Effect == "Allow" && contains(flatten([try(statement.Resource, [])]), "*")
    ]
  ]))

  github_state_read = [
    {
      Sid      = "ReadStateObjects"
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = [for key in local.github_state_keys : "${local.github_state_bucket}/${key}"]
    },
    {
      Sid       = "ListStateKeys"
      Effect    = "Allow"
      Action    = ["s3:ListBucket"]
      Resource  = [local.github_state_bucket]
      Condition = { StringLike = { "s3:prefix" = ["${local.github_state_prefix}/*"] } }
    }
  ]

  github_statements = {
    image_push = [
      {
        Sid      = "GetRegistryToken"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = ["*"]
      },
      {
        Sid    = "PushProjectImages"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload",
          "ecr:PutImage", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:DescribeRepositories"
        ]
        Resource = ["arn:aws:ecr:${var.aws_region}:${var.expected_account_id}:repository/${var.project_name}_*_${var.environment}"]
      }
    ]

    # The deploy role rolls a service to a task definition revision that already exists: it describes definitions and updates the named
    # services. It cannot register a definition or pass a role. The shared task execution role reads the PostgreSQL and Polaris root secrets for
    # the tasks that name them, so a role that registers a definition, passes that role and updates a service could pull those secrets into a
    # crafted task. Terraform registers definitions, and the owner applies it. Until the services stack names the serving services through the
    # variable, the role can only look at the project cluster.
    deploy = concat(
      [
        {
          Sid      = "ReadCluster"
          Effect   = "Allow"
          Action   = ["ecs:DescribeClusters", "ecs:DescribeServices", "ecs:DescribeTasks", "ecs:ListTasks"]
          Resource = ["*"]
        },
        {
          Sid      = "DescribeTaskDefinitions"
          Effect   = "Allow"
          Action   = ["ecs:DescribeTaskDefinition"]
          Resource = ["*"]
        }
      ],
      length(var.github_oidc_deploy_services) == 0 ? [] : [
        {
          Sid      = "UpdateNamedServices"
          Effect   = "Allow"
          Action   = ["ecs:UpdateService"]
          Resource = [for name in var.github_oidc_deploy_services : "arn:aws:ecs:${var.aws_region}:${var.expected_account_id}:service/${local.cluster_name}/${name}"]
        }
      ]
    )

    # Any pull request job of this repository can use the plan role, and the data bucket holds Confidential tables. The explicit Deny
    # covers every object read except the three exact state keys the plan reads, and holds even if an allow above is widened later.
    terraform_plan = concat(
      [
        {
          Sid         = "NeverReadObjectsExceptStateKeys"
          Effect      = "Deny"
          Action      = ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectAttributes", "s3:GetObjectTagging"]
          NotResource = [for key in local.github_state_keys : "${local.github_state_bucket}/${key}"]
        }
      ],
      local.github_plan_read_statements,
      local.github_state_read
    )
  }

  github_policies = {
    for key, statements in local.github_statements : key => jsonencode({ Version = "2012-10-17", Statement = statements })
    if var.github_oidc_enabled
  }

  # What a refresh of the storage stack calls on its bucket: the configuration reads of the project S3 policy. None may fall outside the
  # plan role's allowed actions or under its object read deny.
  github_storage_refresh_actions = toset(flatten([
    for statement in jsondecode(file("${path.module}/policies/${one([for file in local.approved_policy_files : file if endswith(file, "_s3_policy.json")])}")).Statement :
    flatten([statement.Action]) if try(statement.Sid, "") == "ReadApprovedBucketConfiguration"
  ]))
  github_plan_denied_actions = toset(flatten([
    for statement in local.github_statements.terraform_plan : flatten([statement.Action]) if statement.Effect == "Deny"
  ]))

  github_custom_actions = toset(flatten([
    for key, statements in local.github_statements : [
      for statement in statements : statement.Action if statement.Effect == "Allow"
    ]
    if var.github_oidc_enabled
  ]))

  # The boundary as the administrator renders it: the same file, the same values. A shared secret name repeats its resource once per name.
  github_boundary_document = jsondecode(templatefile("${path.module}/boundary/${local.github_boundary_file}", merge(local.owner_policy_values, {
    DATA_BUCKET_NAME   = var.data_bucket_name
    SHARED_SECRET_NAME = "%SHARED_SECRET_NAME%"
  })))
  github_boundary_allows = [
    for statement in local.github_boundary_document.Statement : {
      actions = flatten([statement.Action])
      resources = flatten([
        for resource in flatten([statement.Resource]) :
        strcontains(resource, "%SHARED_SECRET_NAME%") ? [for name in local.github_policy_shared_secrets : replace(resource, "%SHARED_SECRET_NAME%", name)] : [resource]
      ])
    } if statement.Effect == "Allow"
  ]
  github_boundary_actions = toset(flatten([for allow in local.github_boundary_allows : allow.actions]))

  # An action of a delivery role that no boundary statement allows is dead: the boundary refuses it whatever the role policy says.
  github_actions_outside_boundary = toset([
    for action in local.github_custom_actions : action
    if !anytrue([for pattern in local.github_boundary_actions : can(regex("^${replace(pattern, "*", ".*")}$", action))])
  ])
  # A named resource of a delivery role that no boundary statement allows for the same action. A resource of "*" is skipped: the
  # boundary names the resources AWS authorizes such an action on, and the evaluator cases check those requests.
  github_grants_outside_boundary = var.github_oidc_enabled ? toset(flatten([
    for key, statements in local.github_statements : [
      for statement in statements : [
        for pair in setproduct(flatten([statement.Action]), flatten([statement.Resource])) : "${key}: ${pair[0]} on ${pair[1]}"
        if pair[1] != "*" && !anytrue([
          for allow in local.github_boundary_allows :
          anytrue([for action in allow.actions : can(regex("^${replace(action, "*", ".*")}$", pair[0]))]) && anytrue([for pattern in allow.resources : can(regex("^${replace(replace(replace(pattern, ".", "\\."), "?", "."), "*", ".*")}$", pair[1]))])
        ])
      ] if statement.Effect == "Allow"
    ]
  ])) : toset([])
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.github_oidc_enabled && var.github_oidc_provider_arn == "" ? 1 : 0

  url            = "https://${local.github_issuer}"
  client_id_list = ["sts.amazonaws.com"]
}

resource "aws_iam_role" "github" {
  for_each = local.github_roles

  name                 = format(local.platform_name, "gh_${each.key}")
  description          = each.value.description
  max_session_duration = 3600
  permissions_boundary = local.github_boundary_arn
  assume_role_policy   = local.github_trust[each.key]
}

resource "aws_iam_role_policy" "github" {
  for_each = local.github_roles

  name   = "${format(local.platform_name, "gh_${each.key}")}_access"
  role   = aws_iam_role.github[each.key].name
  policy = local.github_policies[each.key]

  lifecycle {
    precondition {
      condition     = length(setsubtract(local.github_custom_actions, local.approved_actions)) == 0
      error_message = "A delivery role allows an action that no project policy in infra/iam/policies allows. Keep the roles inside the approved list."
    }

    precondition {
      condition     = length(local.github_actions_outside_boundary) == 0 && length(local.github_grants_outside_boundary) == 0
      error_message = "A delivery role grants an action or a resource that the delivery boundary in infra/iam/boundary does not allow. The boundary would refuse it."
    }
  }
}

output "github_oidc_contract" {
  description = "What the GitHub delivery roles are, for the workflow settings. Names only."
  value = {
    enabled     = var.github_oidc_enabled
    repository  = var.github_repository
    roles       = { for key, role in aws_iam_role.github : key => role.name }
    subjects    = { for key, role in local.github_roles : key => role.subjects }
    state_stack = local.github_state_stacks
  }
}

output "github_oidc_actions_outside_approved_list" {
  description = "Actions that the push, deploy and plan roles allow beyond the approved project policies. Must be empty."
  value       = sort(tolist(setsubtract(local.github_custom_actions, local.approved_actions)))
}

output "github_oidc_storage_refresh_gaps" {
  description = "Bucket configuration reads of a storage stack refresh that the plan role cannot make. Must be empty."
  value = sort(tolist(setunion(
    setsubtract(local.github_storage_refresh_actions, toset(local.github_plan_actions)),
    setintersection(local.github_storage_refresh_actions, local.github_plan_denied_actions)
  )))
}

output "github_oidc_plan_wildcard_actions_outside_approved" {
  description = "Actions that the plan role may read on every resource but no project policy allows on every resource. Must be empty."
  value = sort(tolist(setsubtract(
    toset(flatten([for statement in local.github_plan_read_statements : statement.Action if contains(statement.Resource, "*")])),
    local.approved_wildcard_actions
  )))
}

output "github_oidc_actions_outside_boundary" {
  description = "Actions that a delivery role allows and the delivery boundary does not. Must be empty."
  value       = sort(tolist(local.github_actions_outside_boundary))
}

output "github_oidc_grants_outside_boundary" {
  description = "Named resources of a delivery role that the delivery boundary does not allow for the same action. Must be empty."
  value       = sort(tolist(local.github_grants_outside_boundary))
}

output "github_oidc_boundary_allowed_actions" {
  description = "Every action the delivery boundary allows, to check that nothing outside the delivery work is in it."
  value       = sort(tolist(local.github_boundary_actions))
}

output "github_oidc_boundary_size" {
  description = "Characters of the delivery boundary, white space left out, for the real names and for the longest names the validations accept, against the 6,144 that IAM allows."
  value = {
    actual    = local.github_boundary_size.actual
    at_bounds = local.github_boundary_size.at_bounds
    limit     = local.github_boundary_limit
  }

  precondition {
    condition     = local.github_boundary_size.actual <= local.github_boundary_limit && local.github_boundary_size.at_bounds <= local.github_boundary_limit
    error_message = "The delivery boundary is over the 6,144 characters of a managed policy, for the real names or for the longest names the variables accept. Shorten infra/iam/boundary/healthcare_hai_prediction_delivery_boundary_policy.json."
  }
}

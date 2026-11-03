# GitHub Actions delivery roles. The whole file is off by default (var.github_oidc_enabled): with the flag off every resource
# below has no instance, so the stack plans no change. The owner applies it with the administrator profile like every other
# role of this stack, because the project profile may not create roles.
#
# Four roles, one per purpose. Each trusts one exact web identity subject of the one repository, with no pattern:
#   image_push       environment image-push           pushes images to the project repositories
#   deploy           environment model-api-deploy     updates the named model API service
#   terraform_plan   pull requests, environment terraform-plan   read-only, plans without a lock
#   terraform_apply  environment terraform-apply      attaches the project's own service policies, nothing wider
# A pull request subject reaches only the plan role. The environments carry the owner's approval and the main-only branch rule
# on the GitHub side; GitHub also issues no web identity token to a workflow that a fork pull request starts.
#
# The permissions stay inside the project profile's approved list (policies/*.json): the apply role attaches those policies by
# ARN, and every action of the other roles is checked against the actions of the same files.
locals {
  github_issuer       = "token.actions.githubusercontent.com"
  github_subject_base = "repo:${var.github_repository}"
  github_provider_arn = var.github_oidc_provider_arn != "" ? var.github_oidc_provider_arn : try(aws_iam_openid_connect_provider.github[0].arn, "")

  # The stacks that continuous delivery may plan and apply. The IAM stack and the storage stack stay with the owner.
  github_state_stacks = ["platform", "services", "jobs"]
  github_state_prefix = "terraform_state"
  github_state_keys   = [for stack in local.github_state_stacks : "${local.github_state_prefix}/${stack}/terraform.tfstate"]
  github_lock_keys    = [for key in local.github_state_keys : "${key}.tflock"]
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
      description = "GitHub Actions Terraform plan: read-only, for pull requests of this repository and the plan step before an apply."
    }
    terraform_apply = {
      subjects    = ["${local.github_subject_base}:environment:terraform-apply"]
      description = "GitHub Actions Terraform apply: applies the platform, services and jobs stacks from the terraform-apply environment after the owner approves."
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
  approved_policy_files    = fileset("${path.module}/policies", "*.json")
  approved_policy_services = toset([for file in local.approved_policy_files : regex("_([a-z0-9]+)_policy\\.json$", file)[0]])
  approved_actions = toset(flatten([
    for file in local.approved_policy_files : [
      for statement in jsondecode(file("${path.module}/policies/${file}")).Statement :
      [for action in flatten([statement.Action]) : action if statement.Effect == "Allow"]
    ]
  ]))

  # The plan role reads: describe, list and get actions of the approved list, less every action that returns a secret value, log
  # content, an image layer or a listing of the data bucket's versions and uploads. Object reads are excluded by prefix (s3:GetObject*).
  # State is read by key below. A refresh of the storage stack reads bucket configuration and heads the bucket (s3:ListBucket), and
  # the stack has no object resource, so no object read needs an exception.
  github_plan_unreadable = ["secretsmanager:GetSecretValue", "ssm:GetParameter", "ssm:GetParameters", "logs:GetLogEvents", "logs:FilterLogEvents", "ecr:GetAuthorizationToken", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ec2:GetConsoleOutput", "ec2:GetConsoleScreenshot", "ec2:GetPasswordData", "logs:GetLogRecord", "logs:GetQueryResults", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts"]
  github_plan_actions = sort([
    for action in local.approved_actions : action
    if can(regex("^[a-z0-9]+:(Describe|List|Get|BatchGet)", action)) && !contains(local.github_plan_unreadable, action) && !startswith(action, "s3:GetObject")
  ])

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

    # The model API lives on another branch. Until it names its service and task roles through the variables, the role can only
    # look at the project cluster.
    deploy = concat(
      [{
        Sid      = "ReadCluster"
        Effect   = "Allow"
        Action   = ["ecs:DescribeClusters", "ecs:DescribeServices", "ecs:DescribeTasks", "ecs:ListTasks"]
        Resource = ["*"]
      }],
      length(var.github_oidc_deploy_services) == 0 ? [] : [
        {
          Sid      = "UpdateNamedServices"
          Effect   = "Allow"
          Action   = ["ecs:UpdateService"]
          Resource = [for name in var.github_oidc_deploy_services : "arn:aws:ecs:${var.aws_region}:${var.expected_account_id}:service/${local.cluster_name}/${name}"]
        },
        {
          Sid      = "RegisterTaskDefinitions"
          Effect   = "Allow"
          Action   = ["ecs:RegisterTaskDefinition", "ecs:DescribeTaskDefinition"]
          Resource = ["*"]
        }
      ],
      length(var.github_oidc_deploy_task_role_names) == 0 ? [] : [{
        Sid       = "PassNamedTaskRoles"
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = [for name in var.github_oidc_deploy_task_role_names : "arn:aws:iam::${var.expected_account_id}:role/${name}"]
        Condition = { StringEquals = { "iam:PassedToService" = "ecs-tasks.amazonaws.com" } }
      }]
    )

    # Any pull request job of this repository can use the plan role, and the data bucket holds Confidential tables. The explicit Deny
    # covers every object read except the three exact state keys the plan reads, and holds even if an allow above is widened later.
    terraform_plan = concat(
      [
        {
          Sid       = "ReadProjectRegion"
          Effect    = "Allow"
          Action    = local.github_plan_actions
          Resource  = ["*"]
          Condition = { StringEquals = { "aws:RequestedRegion" = var.aws_region } }
        },
        {
          Sid         = "NeverReadObjectsExceptStateKeys"
          Effect      = "Deny"
          Action      = ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectAttributes", "s3:GetObjectTagging"]
          NotResource = [for key in local.github_state_keys : "${local.github_state_bucket}/${key}"]
        }
      ],
      local.github_state_read
    )

    terraform_apply = concat(
      local.github_state_read,
      [
        {
          Sid      = "WriteStateObjects"
          Effect   = "Allow"
          Action   = ["s3:PutObject"]
          Resource = [for key in local.github_state_keys : "${local.github_state_bucket}/${key}"]
        },
        {
          Sid      = "HoldStateLocks"
          Effect   = "Allow"
          Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
          Resource = [for key in local.github_lock_keys : "${local.github_state_bucket}/${key}"]
        }
      ]
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
  }
}

resource "aws_iam_role_policy_attachment" "github_apply" {
  for_each = var.github_oidc_enabled ? toset(var.github_oidc_apply_policy_services) : toset([])

  role       = aws_iam_role.github["terraform_apply"].name
  policy_arn = "arn:aws:iam::${var.expected_account_id}:policy/${var.project_name}_${each.key}_policy"

  lifecycle {
    precondition {
      condition     = contains(local.approved_policy_services, each.key)
      error_message = "The apply role may attach only a service policy that exists in infra/iam/policies."
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
  description = "Actions that the push, deploy, plan and apply roles allow beyond the approved project policies. Must be empty."
  value       = sort(tolist(setsubtract(local.github_custom_actions, local.approved_actions)))
}

output "github_oidc_storage_refresh_gaps" {
  description = "Bucket configuration reads of a storage stack refresh that the plan role cannot make. Must be empty."
  value = sort(tolist(setunion(
    setsubtract(local.github_storage_refresh_actions, toset(local.github_plan_actions)),
    setintersection(local.github_storage_refresh_actions, local.github_plan_denied_actions)
  )))
}

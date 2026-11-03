# No policy of this repository may reach terraform_state/ in the data bucket except the Terraform roles, the project user and the
# administrator. The storage stack's bucket policy denies every other principal, and this check finds a grant that would need that deny.
# A glob of an IAM resource (* any run of characters, ? one character) is turned into an anchored pattern and tried on one key and one
# lock file per stack. The project policies, the owner policies and the two pipeline role templates are read.
locals {
  reach_stacks = ["iam", "jobs", "platform", "services", "storage"]
  reach_keys   = flatten([for stack in local.reach_stacks : ["terraform_state/${stack}/terraform.tfstate", "terraform_state/${stack}/terraform.tfstate.tflock"]])
  reach_bucket = "arn:aws:s3:::${var.data_bucket_name}/"

  reach_documents = merge(
    { for file in fileset(path.module, "policies/*.json") : file => try(jsondecode(file("${path.module}/${file}")), { Statement = [] }) },
    { for file in fileset(path.module, "owner_policies/*.json") : file => try(jsondecode(file("${path.module}/${file}")), { Statement = [] }) },
    {
      "role_policies/reader.json" = jsondecode(templatefile("${path.module}/role_policies/reader.json", local.role_policy_inputs))
      "role_policies/writer.json" = jsondecode(templatefile("${path.module}/role_policies/writer.json", local.role_policy_inputs))
    }
  )

  reach_hits = sort(distinct(flatten([
    for file, document in local.reach_documents : [
      for statement in flatten([document.Statement]) :
      "${file}:${try(statement.Sid, "no_sid")}"
      if try(statement.Effect, "") == "Allow" && anytrue([for action in flatten([try(statement.Action, [])]) : startswith(action, "s3:") || action == "*"]) && anytrue(flatten([
        for resource in flatten([try(statement.Resource, [])]) : [
          for key in local.reach_keys :
          can(regex("^${replace(replace(replace(replace(resource, "$${DATA_BUCKET_NAME}", var.data_bucket_name), ".", "\\."), "*", ".*"), "?", ".")}$", "${local.reach_bucket}${key}"))
        ]
      ]))
    ]
  ])))

  reach_probe_resources = {
    bucket_wide    = "${local.reach_bucket}*"
    state_wildcard = "${local.reach_bucket}terraform_state/*"
    stack_wildcard = "${local.reach_bucket}terraform_state/*/terraform.tfstate"
    lakehouse      = "${local.reach_bucket}lakehouse/*"
    dvc            = "${local.reach_bucket}dvc/*"
  }
}

output "state_prefix_reach" {
  description = "Policy statements, as file:Sid, that allow an object under terraform_state/. Must be empty."
  value       = local.reach_hits
}

output "state_prefix_probe" {
  description = "The matcher on known grants, for its test: which probe resources reach a state key."
  value = {
    for name, resource in local.reach_probe_resources : name => anytrue([
      for key in local.reach_keys :
      can(regex("^${replace(replace(replace(resource, ".", "\\."), "*", ".*"), "?", ".")}$", "${local.reach_bucket}${key}"))
    ])
  }
}

# The platform, services and jobs stacks and the job modules define no IAM policy today: they name the task roles that this stack creates. This
# lists any file there that defines an IAM resource or names an S3 ARN, so such a file joins the reach check above before it ships. The folder
# is read from the repository checkout, so a copy of this stack alone (the CI group copies one stack folder) scans nothing.
locals {
  other_stack_files = [
    for file in fileset("${path.module}/..", "{platform,services,jobs,modules}/**/*.tf") : file
    if !strcontains(file, "/tests/") && !strcontains(file, "/.terraform/")
  ]
  other_stack_hits = sort([
    for file in local.other_stack_files : file
    if strcontains(file("${path.module}/../${file}"), "resource \"aws_iam") || strcontains(file("${path.module}/../${file}"), "arn:aws:s3:::")
  ])
}

output "state_prefix_reach_other_stacks" {
  description = "Files of the other stacks that define an IAM resource or name an S3 ARN. Must be empty."
  value       = local.other_stack_hits
}

output "state_prefix_reach_other_stacks_scanned" {
  description = "How many files of the other stacks the check read, for the report."
  value       = length(local.other_stack_files)
}

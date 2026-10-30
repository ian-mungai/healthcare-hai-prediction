# TFLint for the lint_tools tflint check (scripts/quality/lint_tools.py): the Terraform recommended preset plus the AWS ruleset.
# The ruleset version matches config/quality_tools.json; install it once with `tflint --init`. Exceptions go in .linter_allowlist.
plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.50.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

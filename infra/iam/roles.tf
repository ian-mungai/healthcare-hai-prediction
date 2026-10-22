locals {
  lakehouse_read_prefixes  = ["lakehouse/bronze/", "lakehouse/silver/", "lakehouse/gold/"]
  lakehouse_write_prefixes = ["lakehouse/silver/", "lakehouse/silver_e2e/", "lakehouse/gold/", "lakehouse/gold_e2e/", "lakehouse/catalog_backups/"]
  role_policy_inputs = {
    AWS_ACCOUNT_ID   = var.expected_account_id
    PROJECT_USER     = var.aws_profile
    DATA_BUCKET_NAME = var.data_bucket_name
    READ_PREFIXES    = local.lakehouse_read_prefixes
    WRITE_PREFIXES   = local.lakehouse_write_prefixes
  }
}

resource "aws_iam_role" "lakehouse_reader" {
  name                 = var.lakehouse_reader_role_name
  description          = "Read-only access to the lakehouse tables for pipeline read jobs; assumed for one job by the project user."
  max_session_duration = var.lakehouse_session_max_seconds
  assume_role_policy   = templatefile("${path.module}/role_policies/trust.json", local.role_policy_inputs)

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role_policy" "lakehouse_reader" {
  name   = "${var.lakehouse_reader_role_name}_access"
  role   = aws_iam_role.lakehouse_reader.name
  policy = templatefile("${path.module}/role_policies/reader.json", local.role_policy_inputs)
}

resource "aws_iam_role" "lakehouse_writer" {
  name                 = var.lakehouse_writer_role_name
  description          = "Write access to the silver, gold and catalog backup prefixes for pipeline write jobs; assumed for one job by the project user."
  max_session_duration = var.lakehouse_session_max_seconds
  assume_role_policy   = templatefile("${path.module}/role_policies/trust.json", local.role_policy_inputs)

  lifecycle {
    prevent_destroy = true

    precondition {
      condition     = var.lakehouse_reader_role_name != var.lakehouse_writer_role_name
      error_message = "The reader and writer roles must have different names."
    }
  }
}

resource "aws_iam_role_policy" "lakehouse_writer" {
  name   = "${var.lakehouse_writer_role_name}_access"
  role   = aws_iam_role.lakehouse_writer.name
  policy = templatefile("${path.module}/role_policies/writer.json", local.role_policy_inputs)
}

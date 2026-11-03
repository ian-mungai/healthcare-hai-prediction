# Server access logs of the project bucket (approved: replaces the acquisition-stage logging deferral once the owner turns it on). The logs go to
# a small bucket of this stack, expire after 30 days and can be read only by the owner's administrator path. The log delivery service may
# write under data_bucket_logs/ for the project bucket of this account and nothing else. Server access logs support SSE-S3 only, not KMS.
# The project profile needs s3:CreateBucket and the bucket settings on the logging bucket and s3:PutBucketLogging on the project bucket
# before an apply (project guide: access logging). Listing stays open to the project profile, because a bucket head
# needs s3:ListBucket for every refresh. Log objects cannot be read by it.
locals {
  logs_bucket_name = "${var.data_bucket_name}-logs"
  logs_prefix      = "data_bucket_logs/"
  logs_reader_arns = [for name in var.access_logging_reader_principals : "arn:aws:iam::${var.expected_account_id}:${name}"]
}

resource "aws_s3_bucket" "logs" {
  #checkov:skip=CKV_AWS_18:The logging bucket does not log itself; its only writer is the log delivery service and its readers are named in its policy.
  #checkov:skip=CKV_AWS_21:Logs are written once and expire after 30 days, so versions add nothing.
  #checkov:skip=CKV2_AWS_62:No event consumer exists for log objects.
  #checkov:skip=CKV_AWS_144:Logs are short-lived and rebuilt by the next requests, so replication waits for the recovery design.
  #checkov:skip=CKV_AWS_145:A log destination supports SSE-S3 only. AWS does not deliver server access logs to a KMS-encrypted bucket.
  count = var.access_logging_enabled ? 1 : 0

  bucket        = local.logs_bucket_name
  force_destroy = false

  tags = {
    DataClassification = "Confidential"
  }

  lifecycle {
    precondition {
      condition     = length(local.logs_bucket_name) <= 63
      error_message = "The logging bucket name is the project bucket name plus -logs and must fit in 63 characters."
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  count = var.access_logging_enabled ? 1 : 0

  bucket = aws_s3_bucket.logs[0].id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  count = var.access_logging_enabled ? 1 : 0

  bucket                  = aws_s3_bucket.logs[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  count = var.access_logging_enabled ? 1 : 0

  bucket = aws_s3_bucket.logs[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  count = var.access_logging_enabled ? 1 : 0

  bucket = aws_s3_bucket.logs[0].id

  rule {
    id     = "abort_incomplete_uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire_access_logs"
    status = "Enabled"

    filter {
      prefix = local.logs_prefix
    }

    expiration {
      days = 30
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  count = var.access_logging_enabled ? 1 : 0

  bucket = aws_s3_bucket.logs[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [aws_s3_bucket.logs[0].arn, "${aws_s3_bucket.logs[0].arn}/*"]
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
      {
        Sid       = "AllowLogDeliveryFromTheProjectBucket"
        Effect    = "Allow"
        Principal = { Service = "logging.s3.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.logs[0].arn}/${local.logs_prefix}*"
        Condition = {
          ArnLike      = { "aws:SourceArn" = aws_s3_bucket.data.arn }
          StringEquals = { "aws:SourceAccount" = var.expected_account_id }
        }
      },
      {
        Sid       = "DenyLogReadsToEveryoneElse"
        Effect    = "Deny"
        Principal = "*"
        Action    = ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectAttributes", "s3:GetObjectTagging"]
        Resource  = [aws_s3_bucket.logs[0].arn, "${aws_s3_bucket.logs[0].arn}/*"]
        Condition = { ArnNotLike = { "aws:PrincipalArn" = local.logs_reader_arns } }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.logs]

  lifecycle {
    precondition {
      condition     = length(var.access_logging_reader_principals) > 0
      error_message = "Name the owner's administrator path in access_logging_reader_principals before turning access logging on."
    }
  }
}

resource "aws_s3_bucket_logging" "data" {
  count = var.access_logging_enabled ? 1 : 0

  bucket        = aws_s3_bucket.data.id
  target_bucket = aws_s3_bucket.logs[0].id
  target_prefix = local.logs_prefix

  depends_on = [aws_s3_bucket_policy.logs]
}

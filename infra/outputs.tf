output "bucket_name" {
  value       = aws_s3_bucket.data.id
  description = "Acquisition destination managed through Terraform."
}

output "acquisition_prefixes" {
  description = "Prefix conventions only; no placeholder objects or datasets are provisioned by Terraform."
  value = {
    raw       = "s3://${aws_s3_bucket.data.id}/raw/"
    reference = "s3://${aws_s3_bucket.data.id}/reference/"
    manifests = "s3://${aws_s3_bucket.data.id}/manifests/"
    audit     = "s3://${aws_s3_bucket.data.id}/audit/"
  }
}

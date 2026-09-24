output "bucket_name" {
  value       = aws_s3_bucket.data.id
  description = "Acquisition destination managed through Terraform."
}

output "acquisition_prefixes" {
  description = "Legacy prefixes retained for existing evidence; new captures use dataset_prefixes."
  value = {
    raw       = "s3://${aws_s3_bucket.data.id}/raw/"
    reference = "s3://${aws_s3_bucket.data.id}/reference/"
    manifests = "s3://${aws_s3_bucket.data.id}/manifests/"
    audit     = "s3://${aws_s3_bucket.data.id}/audit/"
  }
}

output "dataset_prefixes" {
  description = "Superseded dataset-first templates retained for legacy evidence; new writes use collection_prefixes."
  value = {
    data       = "s3://${aws_s3_bucket.data.id}/{dataset_id}/data/"
    references = "s3://${aws_s3_bucket.data.id}/{dataset_id}/references/"
    manifests  = "s3://${aws_s3_bucket.data.id}/{dataset_id}/manifests/"
    audit      = "s3://${aws_s3_bucket.data.id}/{dataset_id}/audit/"
  }
}

output "collection_prefixes" {
  description = "Publisher/collection templates with one shared references area per collection; table paths remain separate under datasets."
  value = {
    datasets   = "s3://${aws_s3_bucket.data.id}/{publisher}/{collection}/datasets/"
    references = "s3://${aws_s3_bucket.data.id}/{publisher}/{collection}/references/"
    manifests  = "s3://${aws_s3_bucket.data.id}/{publisher}/{collection}/manifests/"
    audit      = "s3://${aws_s3_bucket.data.id}/{publisher}/{collection}/audit/"
  }
}

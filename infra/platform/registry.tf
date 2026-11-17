# Images are code, not data. AES256 avoids a customer-managed key ($1 a month each) and the KMS grants the project user
# would need for an AWS-managed key.
#trivy:ignore:AWS-0033
resource "aws_ecr_repository" "image" {
  #checkov:skip=CKV_AWS_136:AES256 encryption; a KMS key adds cost and grant permissions for images that hold no data.
  for_each = local.ecr_repositories

  name                 = format(local.name, each.key)
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.allow_destructive_teardown

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  # The group of every image is checked on every plan, so a typo in the group of an image that a switch would leave out cannot keep or drop its repository unseen.
  # scripts/infrastructure/push_images.py checks the same list before an image is pushed.
  lifecycle {
    precondition {
      condition     = alltrue([for name, group in local.image_groups : contains(local.repository_groups, group)])
      error_message = "Every image of infra/services/images.json needs a repository_group of always, monitoring or serving. Fix: ${join(", ", [for name, group in local.image_groups : name if !contains(local.repository_groups, group)])}."
    }
  }
}

resource "aws_ecr_lifecycle_policy" "image" {
  for_each = aws_ecr_repository.image

  repository = each.value.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after one day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep the most recent images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = var.ecr_keep_images
        }
        action = { type = "expire" }
      }
    ]
  })
}

# =============================================================================
# S3 data lake — the permanent, cheap storage that is our no-data-loss safety net.
# =============================================================================

resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_bucket_name
}

# Block ALL public access — this data must never be reachable from the internet.
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Encrypt everything at rest (AWS-managed keys — free and automatic).
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Keep old versions of objects — extra protection against accidental overwrite.
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle rules: raw data ages into cheaper storage; staging is temporary.
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "raw-to-glacier"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = var.raw_retention_days_to_glacier
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "expire-staging"
    status = "Enabled"
    filter { prefix = "staging/" }
    expiration { days = 7 }
  }
}

# The "folders" (prefixes) our pipeline uses. S3 doesn't need real folders, but
# creating zero-byte markers makes the structure visible in the console.
resource "aws_s3_object" "zones" {
  for_each = toset(["raw/", "staging/", "curated/", "rejects/"])
  bucket   = aws_s3_bucket.data_lake.id
  key      = each.value
  content  = ""
}

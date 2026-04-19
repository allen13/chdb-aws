# Parallel Iceberg backend: regular S3 bucket + Glue Data Catalog. Each
# asset is registered as both an S3 Tables table (s3_tables.tf) and a Glue
# Iceberg table here. Same schema, same data — populated by the write Lambda.
# This second backend exists so chDB's native icebergS3() can read tables
# directly: regular S3 buckets allow GetBucketLocation / ListObjects, which
# the *--table-s3 buckets behind S3 Tables refuse.

resource "aws_s3_bucket" "iceberg" {
  bucket        = "${local.name_prefix}-iceberg"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "iceberg" {
  bucket                  = aws_s3_bucket.iceberg.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "iceberg" {
  bucket = aws_s3_bucket.iceberg.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_glue_catalog_database" "this" {
  name = replace(local.name_prefix, "-", "_")
}

# Glue catalog tables are created lazily by the write Lambda using
# pyiceberg, which preserves the full Iceberg type system (timestamptz,
# required, etc.) — Terraform's `aws_glue_catalog_table` with
# `metadata_operation=CREATE` collapses that down to Hive types and loses
# required flags, which then causes pyiceberg.append schema-mismatch errors.

resource "aws_s3_bucket" "data" {
  bucket = "${local.name_prefix}-data"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-archived"
    status = "Enabled"

    filter {
      tag {
        key   = "lifecycle"
        value = "archived"
      }
    }

    expiration {
      days = 30
    }
  }
}

resource "aws_s3_bucket_notification" "data" {
  count  = var.image_uri == null ? 0 : 1
  bucket = aws_s3_bucket.data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.write[0].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "assets/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}

resource "aws_lambda_permission" "allow_s3_invoke" {
  count         = var.image_uri == null ? 0 : 1
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.write[0].function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

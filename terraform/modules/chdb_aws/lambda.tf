resource "aws_lambda_function" "this" {
  count = var.image_uri == null ? 0 : 1

  function_name = local.name_prefix
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = var.image_uri

  memory_size = 2048
  timeout     = 60

  environment {
    variables = {
      TABLE_BUCKET_ARN = aws_s3tables_table_bucket.this.arn
      TABLE_NAMESPACE  = var.table_namespace
    }
  }
}

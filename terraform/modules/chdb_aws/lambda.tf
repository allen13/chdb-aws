locals {
  lambda_env = {
    DATA_BUCKET      = aws_s3_bucket.data.id
    TABLE_BUCKET_ARN = aws_s3tables_table_bucket.this.arn
    TABLE_NAMESPACE  = aws_s3tables_namespace.this.namespace
  }
}

resource "aws_lambda_function" "write" {
  count = var.image_uri == null ? 0 : 1

  function_name = "${local.name_prefix}-write"
  role          = aws_iam_role.write_exec.arn
  package_type  = "Image"
  image_uri     = var.image_uri

  memory_size = 1024
  timeout     = 300

  image_config {
    command = ["chdb_aws.write.handler.handler"]
  }

  environment {
    variables = local.lambda_env
  }
}

resource "aws_lambda_function" "read" {
  count = var.image_uri == null ? 0 : 1

  function_name = "${local.name_prefix}-read"
  role          = aws_iam_role.read_exec.arn
  package_type  = "Image"
  image_uri     = var.image_uri

  memory_size = 4096
  timeout     = 300

  image_config {
    command = ["chdb_aws.read.handler.handler"]
  }

  environment {
    variables = merge(local.lambda_env, {
      READ_RESULT_FORMAT = "JSONCompact"
    })
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# --- write role -------------------------------------------------------------

resource "aws_iam_role" "write_exec" {
  name               = "${local.name_prefix}-write-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "write_basic" {
  role       = aws_iam_role.write_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "write_inline" {
  statement {
    sid     = "DataBucketRW"
    actions = ["s3:GetObject", "s3:PutObject", "s3:PutObjectTagging", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/assets/*"]
  }

  statement {
    sid       = "DataBucketList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid = "S3TablesWrite"
    actions = [
      "s3tables:GetTableBucket",
      "s3tables:GetTableBucketMaintenanceConfiguration",
      "s3tables:GetNamespace",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
      "s3tables:GetTable",
      "s3tables:GetTableMetadataLocation",
      "s3tables:UpdateTableMetadataLocation",
      "s3tables:PutTableData",
      "s3tables:GetTableData",
    ]
    resources = [
      aws_s3tables_table_bucket.this.arn,
      "${aws_s3tables_table_bucket.this.arn}/*",
    ]
  }

  statement {
    sid     = "IcebergBucketRW"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.iceberg.arn,
      "${aws_s3_bucket.iceberg.arn}/*",
    ]
  }

  statement {
    sid = "GlueIcebergWrite"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.this.name}",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.this.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "write_inline" {
  name   = "${local.name_prefix}-write"
  role   = aws_iam_role.write_exec.id
  policy = data.aws_iam_policy_document.write_inline.json
}

# --- read role --------------------------------------------------------------

resource "aws_iam_role" "read_exec" {
  name               = "${local.name_prefix}-read-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "read_basic" {
  role       = aws_iam_role.read_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "read_inline" {
  statement {
    sid = "S3TablesRead"
    actions = [
      "s3tables:GetTableBucket",
      "s3tables:GetTableBucketMaintenanceConfiguration",
      "s3tables:GetNamespace",
      "s3tables:GetTable",
      "s3tables:GetTableMetadataLocation",
      "s3tables:GetTableData",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
    ]
    resources = [
      aws_s3tables_table_bucket.this.arn,
      "${aws_s3tables_table_bucket.this.arn}/*",
    ]
  }

  # chDB's icebergS3() reads metadata/data files using plain S3 API from the
  # underlying bucket S3 Tables uses to back each table (names match
  # `*--table-s3`). Grant direct S3 read access to those buckets — the bucket
  # *policy* refuses ListObjectsV2 / GetBucketLocation regardless, but plain
  # GetObject + HeadObject do work and that's what Iceberg readers need.
  statement {
    sid       = "UnderlyingTableBucketRead"
    actions   = ["s3:GetObject", "s3:GetBucketLocation", "s3:ListBucket"]
    resources = ["arn:aws:s3:::*--table-s3", "arn:aws:s3:::*--table-s3/*"]
  }

  statement {
    sid     = "IcebergBucketRead"
    actions = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.iceberg.arn,
      "${aws_s3_bucket.iceberg.arn}/*",
    ]
  }

  statement {
    sid = "GlueIcebergRead"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.this.name}",
      "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.this.name}/*",
    ]
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_iam_role_policy" "read_inline" {
  name   = "${local.name_prefix}-read"
  role   = aws_iam_role.read_exec.id
  policy = data.aws_iam_policy_document.read_inline.json
}

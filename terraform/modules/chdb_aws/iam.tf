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
  # `*--table-s3`). Grant direct S3 read access to those buckets.
  statement {
    sid       = "UnderlyingTableBucketRead"
    actions   = ["s3:GetObject", "s3:GetBucketLocation", "s3:ListBucket"]
    resources = ["arn:aws:s3:::*--table-s3", "arn:aws:s3:::*--table-s3/*"]
  }
}

resource "aws_iam_role_policy" "read_inline" {
  name   = "${local.name_prefix}-read"
  role   = aws_iam_role.read_exec.id
  policy = data.aws_iam_policy_document.read_inline.json
}

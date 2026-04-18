data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${local.name_prefix}-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "s3_tables_read" {
  statement {
    actions = [
      "s3tables:GetTable",
      "s3tables:GetTableBucket",
      "s3tables:GetTableData",
      "s3tables:GetTableMetadataLocation",
      "s3tables:ListNamespaces",
      "s3tables:ListTables",
    ]
    resources = [
      aws_s3tables_table_bucket.this.arn,
      "${aws_s3tables_table_bucket.this.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "s3_tables_read" {
  name   = "${local.name_prefix}-s3-tables-read"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.s3_tables_read.json
}

resource "aws_s3tables_table_bucket" "this" {
  name = var.table_bucket_name
}

resource "aws_s3tables_namespace" "this" {
  namespace        = var.table_namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
}

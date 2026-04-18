resource "aws_s3tables_table_bucket" "this" {
  name = var.table_bucket_name
}

resource "aws_s3tables_namespace" "this" {
  namespace        = var.table_namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
}

resource "aws_s3tables_table" "asset" {
  for_each = var.assets

  name             = each.key
  namespace        = aws_s3tables_namespace.this.namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  format           = "ICEBERG"

  metadata {
    iceberg {
      schema {
        dynamic "field" {
          for_each = each.value.schema
          content {
            name     = field.value.name
            type     = field.value.type
            required = try(field.value.required, false)
          }
        }
      }
    }
  }
}

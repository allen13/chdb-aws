output "data_bucket_name" {
  value = aws_s3_bucket.data.id
}

output "table_bucket_arn" {
  value = aws_s3tables_table_bucket.this.arn
}

output "namespace" {
  value = aws_s3tables_namespace.this.namespace
}

output "table_arns" {
  value       = { for k, t in aws_s3tables_table.asset : k => t.arn }
  description = "Map of asset name → S3 Tables table ARN."
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.lambda.repository_url
  description = "Push the Lambda container image to this repository."
}

output "write_lambda_function_name" {
  value = try(aws_lambda_function.write[0].function_name, null)
}

output "read_lambda_function_name" {
  value = try(aws_lambda_function.read[0].function_name, null)
}

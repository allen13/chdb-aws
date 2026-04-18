output "ecr_repository_url" {
  value       = aws_ecr_repository.lambda.repository_url
  description = "Push the Lambda container image to this repository."
}

output "lambda_function_name" {
  value       = try(aws_lambda_function.this[0].function_name, null)
  description = "Lambda function name (null until image_uri is set)."
}

output "table_bucket_arn" {
  value       = aws_s3tables_table_bucket.this.arn
  description = "ARN of the S3 Tables bucket."
}

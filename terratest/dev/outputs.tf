output "ecr_repository_url" {
  value = module.chdb_aws.ecr_repository_url
}

output "lambda_function_name" {
  value = module.chdb_aws.lambda_function_name
}

output "table_bucket_arn" {
  value = module.chdb_aws.table_bucket_arn
}

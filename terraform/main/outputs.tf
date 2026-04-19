output "data_bucket_name" {
  value = module.chdb_aws.data_bucket_name
}

output "table_bucket_arn" {
  value = module.chdb_aws.table_bucket_arn
}

output "table_arns" {
  value = module.chdb_aws.table_arns
}

output "ecr_repository_url" {
  value = module.chdb_aws.ecr_repository_url
}

output "write_lambda_function_name" {
  value = module.chdb_aws.write_lambda_function_name
}

output "read_lambda_function_name" {
  value = module.chdb_aws.read_lambda_function_name
}

output "glue_database_name" {
  value = module.chdb_aws.glue_database_name
}

output "iceberg_bucket_name" {
  value = module.chdb_aws.iceberg_bucket_name
}

output "glue_table_names" {
  value = module.chdb_aws.glue_table_names
}

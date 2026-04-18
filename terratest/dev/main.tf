module "chdb_aws" {
  source = "../../terraform/modules/chdb_aws"

  environment       = var.environment
  project_name      = var.project_name
  image_uri         = var.image_uri
  table_bucket_name = var.table_bucket_name
  table_namespace   = var.table_namespace
}

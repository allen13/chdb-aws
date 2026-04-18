variable "aws_region" {
  type        = string
  description = "AWS region for all resources."
}

variable "environment" {
  type        = string
  description = "Deployment environment."
  default     = "prod"
}

variable "project_name" {
  type        = string
  description = "Short project identifier used in resource names."
  default     = "chdb-aws"
}

variable "image_uri" {
  type        = string
  description = "ECR image URI for the Lambda container."
  default     = null
}

variable "table_bucket_name" {
  type        = string
  description = "Name of the S3 Tables bucket to create."
}

variable "table_namespace" {
  type        = string
  description = "Namespace within the S3 Tables bucket."
}

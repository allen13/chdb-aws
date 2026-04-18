variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "project_name" {
  type    = string
  default = "chdb-aws"
}

variable "image_uri" {
  type    = string
  default = null
}

variable "table_bucket_name" {
  type = string
}

variable "table_namespace" {
  type    = string
  default = "analytics_dev"
}

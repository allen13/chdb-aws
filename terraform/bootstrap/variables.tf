variable "aws_region" {
  type        = string
  description = "Region in which the state bucket is created."
}

variable "state_bucket_name" {
  type        = string
  description = "Name of the S3 bucket that will hold Terraform state."
}

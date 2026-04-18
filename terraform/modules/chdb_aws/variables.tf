variable "environment" {
  type        = string
  description = "Deployment environment (e.g. prod, dev)."
}

variable "project_name" {
  type        = string
  description = "Short project identifier used in resource names."
  default     = "chdb-aws"
}

variable "image_uri" {
  type        = string
  description = "ECR image URI (repo:tag or repo@digest) for both Lambda containers."
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

variable "assets" {
  type = map(object({
    schema = list(object({
      name     = string
      type     = string
      required = optional(bool, false)
    }))
  }))
  description = "Map of asset name to its Iceberg schema. Each key becomes a table and its dropzone/archive prefix."
}

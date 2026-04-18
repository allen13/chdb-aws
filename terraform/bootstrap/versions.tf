terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }

  # Local state by design: this config creates the S3 bucket that holds the
  # main config's remote state, so it cannot itself use that bucket.
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "chdb-aws"
      Component = "tfstate-bootstrap"
      ManagedBy = "terraform"
    }
  }
}

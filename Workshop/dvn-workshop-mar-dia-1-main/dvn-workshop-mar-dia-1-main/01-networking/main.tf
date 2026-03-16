terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  backend "s3" {
    bucket       = "workshop-dev-terraform"
    key          = "networking/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true

  }
}

provider "aws" {
  region = var.assume_role.region

  default_tags {
    tags = var.tags
  }

  assume_role {
    role_arn = var.assume_role.arn
  }
}

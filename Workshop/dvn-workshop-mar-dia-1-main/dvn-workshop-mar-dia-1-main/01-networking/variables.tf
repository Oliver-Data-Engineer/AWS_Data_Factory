variable "tags" {
  type = map(string)
  default = {
    Environment = "production"
    Project     = "workshop-devops-na-nuvem"
  }
}

variable "assume_role" {
  type = object({
    arn    = string
    region = string
  })

 default = {
    arn    = "arn:aws:iam::903146277540:role/glue_analytics_access"
    region = "us-east-1"
  }
}


variable "s3" {
   type = object({
    s3_bucket_name = string})

    default = {
      s3_bucket_name = "workshop-dev-terraform"
    }
}
# Which versions of Terraform + the AWS provider this project needs.
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # OPTIONAL but recommended once you're deploying for real: store Terraform's
  # "state file" in S3 instead of on your laptop, so it's safe and shareable.
  # Uncomment and fill in after you've created a state bucket (see NEXT-STEPS.md).
  #
  # backend "s3" {
  #   bucket = "aws-ledger-tfstate-<your-unique-suffix>"
  #   key    = "datamoon/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

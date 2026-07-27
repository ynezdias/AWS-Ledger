# Tells Terraform to talk to AWS in the region you choose, and tags every
# resource it creates so you can find (and bill) them easily.
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "aws-ledger-datamoon"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}

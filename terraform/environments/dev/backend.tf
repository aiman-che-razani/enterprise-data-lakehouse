# Remote state. Create the bucket and lock table once per account (see docs/deployment/DEPLOYMENT.md),
# then run: terraform init -backend-config="key=lakehouse/<env>/terraform.tfstate"
terraform {
  backend "s3" {
    bucket         = "retailnexus-terraform-state"
    key            = "lakehouse/dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "retailnexus-terraform-locks"
    encrypt        = true
  }
}

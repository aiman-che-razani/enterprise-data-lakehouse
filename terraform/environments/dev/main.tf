terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.tags
  }
}

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
  secrets_arn_prefix = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.project}/${var.environment}/"
}

# Optional customer-managed key shared by S3, MSK and Redshift.
resource "aws_kms_key" "data" {
  count                   = var.enable_kms ? 1 : 0
  description             = "${local.name_prefix} data encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = 14
}

resource "aws_kms_alias" "data" {
  count         = var.enable_kms ? 1 : 0
  name          = "alias/${local.name_prefix}-data"
  target_key_id = aws_kms_key.data[0].key_id
}

locals {
  kms_key_arn = var.enable_kms ? aws_kms_key.data[0].arn : ""
}

module "networking" {
  source             = "../../modules/networking"
  name_prefix        = local.name_prefix
  vpc_cidr           = var.vpc_cidr
  az_count           = var.az_count
  single_nat_gateway = var.single_nat_gateway
}

module "s3" {
  source        = "../../modules/s3"
  name_prefix   = local.name_prefix
  force_destroy = var.force_destroy_buckets
  kms_key_arn   = local.kms_key_arn
}

module "iam" {
  source               = "../../modules/iam"
  name_prefix          = local.name_prefix
  lakehouse_bucket_arn = module.s3.bucket_arns["lakehouse"]
  artifacts_bucket_arn = module.s3.bucket_arns["artifacts"]
  logs_bucket_arn      = module.s3.bucket_arns["logs"]
  secrets_arn_prefix   = local.secrets_arn_prefix
  kms_key_arn          = local.kms_key_arn
}

module "kafka" {
  source             = "../../modules/kafka"
  name_prefix        = local.name_prefix
  private_subnet_ids = module.networking.private_subnet_ids
  security_group_ids = [module.networking.msk_sg_id]
  instance_type      = var.msk_instance_type
  broker_count       = var.msk_broker_count
  ebs_volume_gb      = var.msk_ebs_volume_gb
  kms_key_arn        = local.kms_key_arn
  log_bucket         = module.s3.logs_bucket
}

module "emr" {
  source             = "../../modules/emr"
  name_prefix        = local.name_prefix
  private_subnet_ids = module.networking.private_subnet_ids
  security_group_ids = [module.networking.compute_sg_id]
  max_cpu            = var.emr_max_cpu
  max_memory         = var.emr_max_memory
}

module "redshift" {
  source              = "../../modules/redshift"
  name_prefix         = local.name_prefix
  private_subnet_ids  = module.networking.private_subnet_ids
  security_group_ids  = [module.networking.redshift_sg_id]
  iam_role_arns       = [module.iam.redshift_role_arn]
  node_type           = var.redshift_node_type
  number_of_nodes     = var.redshift_nodes
  kms_key_arn         = local.kms_key_arn
  log_bucket          = module.s3.logs_bucket
  skip_final_snapshot = var.environment != "prod"
}

terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

resource "aws_redshift_subnet_group" "this" {
  name       = "${var.name_prefix}-redshift"
  subnet_ids = var.private_subnet_ids
  tags       = var.tags
}

resource "aws_redshift_parameter_group" "this" {
  name   = "${var.name_prefix}-redshift"
  family = "redshift-1.0"

  parameter {
    name  = "require_ssl"
    value = "true"
  }
  parameter {
    name  = "enable_user_activity_logging"
    value = "true"
  }
  tags = var.tags
}

resource "aws_redshift_cluster" "this" {
  cluster_identifier = "${var.name_prefix}-dwh"
  database_name      = var.database_name
  master_username    = var.master_username

  # Password is generated and rotated in Secrets Manager; it never enters state or tfvars.
  manage_master_password = true

  node_type       = var.node_type
  number_of_nodes = var.number_of_nodes
  cluster_type    = var.number_of_nodes > 1 ? "multi-node" : "single-node"

  cluster_subnet_group_name    = aws_redshift_subnet_group.this.name
  cluster_parameter_group_name = aws_redshift_parameter_group.this.name
  vpc_security_group_ids       = var.security_group_ids
  iam_roles                    = var.iam_role_arns

  publicly_accessible  = false
  encrypted            = true
  kms_key_id           = var.kms_key_arn == "" ? null : var.kms_key_arn
  enhanced_vpc_routing = true

  automated_snapshot_retention_period = 7
  skip_final_snapshot                 = var.skip_final_snapshot
  final_snapshot_identifier           = var.skip_final_snapshot ? null : "${var.name_prefix}-dwh-final"

  logging {
    enable               = true
    log_destination_type = "s3"
    bucket_name          = var.log_bucket
    s3_key_prefix        = "redshift/"
  }

  tags = var.tags
}

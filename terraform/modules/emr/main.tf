terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.name_prefix}-spark"
  release_label = var.release_label
  type          = "spark"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = var.max_cpu
    memory = var.max_memory
  }

  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = var.tags
}

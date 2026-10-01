variable "project" {
  type    = string
  default = "retailnexus"
}

variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "enable_kms" {
  description = "Create a customer-managed KMS key (recommended for staging/prod)"
  type        = bool
  default     = false
}

variable "force_destroy_buckets" {
  type    = bool
  default = false
}

# ── Networking ────────────────────────────────────────────────────────────────
variable "vpc_cidr" {
  type = string
}

variable "az_count" {
  type    = number
  default = 2
}

variable "single_nat_gateway" {
  type    = bool
  default = true
}

# ── MSK ───────────────────────────────────────────────────────────────────────
variable "msk_instance_type" {
  type = string
}

variable "msk_broker_count" {
  type = number
}

variable "msk_ebs_volume_gb" {
  type    = number
  default = 100
}

# ── EMR Serverless ────────────────────────────────────────────────────────────
variable "emr_max_cpu" {
  type = string
}

variable "emr_max_memory" {
  type = string
}

# ── Redshift ──────────────────────────────────────────────────────────────────
variable "redshift_node_type" {
  type = string
}

variable "redshift_nodes" {
  type = number
}

variable "image_tag" {
  description = "API container image tag (passed by CD; consumed by the ECS service definition)"
  type        = string
  default     = "latest"
}

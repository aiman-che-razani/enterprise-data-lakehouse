variable "name_prefix" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "kafka_version" {
  type    = string
  default = "3.7.x"
}

variable "instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "broker_count" {
  description = "Total brokers; must be a multiple of the number of subnets"
  type        = number
  default     = 2
}

variable "ebs_volume_gb" {
  type    = number
  default = 100
}

variable "kms_key_arn" {
  type    = string
  default = ""
}

variable "log_bucket" {
  description = "S3 bucket receiving broker logs"
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

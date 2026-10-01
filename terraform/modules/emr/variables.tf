variable "name_prefix" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "release_label" {
  description = "EMR release bundling Spark 3.5"
  type        = string
  default     = "emr-7.2.0"
}

variable "max_cpu" {
  type    = string
  default = "64 vCPU"
}

variable "max_memory" {
  type    = string
  default = "256 GB"
}

variable "idle_timeout_minutes" {
  type    = number
  default = 15
}

variable "tags" {
  type    = map(string)
  default = {}
}

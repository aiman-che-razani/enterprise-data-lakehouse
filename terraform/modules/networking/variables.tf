variable "name_prefix" {
  type = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones (MSK needs >= 2, prod uses 3)"
  type        = number
  default     = 2
}

variable "single_nat_gateway" {
  description = "Share one NAT gateway (cheaper; dev/staging) instead of one per AZ"
  type        = bool
  default     = true
}

variable "tags" {
  type    = map(string)
  default = {}
}

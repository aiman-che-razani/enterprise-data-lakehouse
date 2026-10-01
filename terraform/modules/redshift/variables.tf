variable "name_prefix" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "iam_role_arns" {
  type = list(string)
}

variable "node_type" {
  type    = string
  default = "ra3.xlplus"
}

variable "number_of_nodes" {
  type    = number
  default = 2
}

variable "database_name" {
  type    = string
  default = "retailnexus"
}

variable "master_username" {
  type    = string
  default = "rn_admin"
}

variable "kms_key_arn" {
  type    = string
  default = ""
}

variable "log_bucket" {
  type = string
}

variable "skip_final_snapshot" {
  type    = bool
  default = false
}

variable "tags" {
  type    = map(string)
  default = {}
}

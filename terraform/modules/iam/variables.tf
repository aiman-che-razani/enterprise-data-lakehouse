variable "name_prefix" {
  type = string
}

variable "lakehouse_bucket_arn" {
  type = string
}

variable "artifacts_bucket_arn" {
  type = string
}

variable "logs_bucket_arn" {
  type = string
}

variable "secrets_arn_prefix" {
  description = "ARN prefix of Secrets Manager secrets readable by the pipeline"
  type        = string
}

variable "kms_key_arn" {
  type    = string
  default = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

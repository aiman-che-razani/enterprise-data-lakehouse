variable "name_prefix" {
  description = "Prefix for all bucket names, e.g. retailnexus-dev"
  type        = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete non-empty buckets (dev only)"
  type        = bool
  default     = false
}

variable "kms_key_arn" {
  description = "KMS key used for SSE-KMS. Empty string falls back to SSE-S3 (AES256)."
  type        = string
  default     = ""
}

variable "bronze_glacier_days" {
  description = "Days before Bronze objects transition to Glacier"
  type        = number
  default     = 365
}

variable "bronze_expiration_days" {
  description = "Days before Bronze objects expire (2555 = 7-year retention)"
  type        = number
  default     = 2555
}

variable "tags" {
  type    = map(string)
  default = {}
}

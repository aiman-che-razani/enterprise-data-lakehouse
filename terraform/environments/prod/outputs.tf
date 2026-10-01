output "lakehouse_bucket" {
  value = module.s3.lakehouse_bucket
}

output "artifacts_bucket" {
  value = module.s3.artifacts_bucket
}

output "msk_bootstrap_brokers_iam" {
  value = module.kafka.bootstrap_brokers_iam
}

output "emr_application_id" {
  value = module.emr.application_id
}

output "emr_job_role_arn" {
  value = module.iam.emr_job_role_arn
}

output "redshift_endpoint" {
  value = module.redshift.endpoint
}

output "redshift_master_password_secret_arn" {
  value = module.redshift.master_password_secret_arn
}

output "api_task_role_arn" {
  value = module.iam.api_task_role_arn
}

output "api_image_tag" {
  value = var.image_tag
}

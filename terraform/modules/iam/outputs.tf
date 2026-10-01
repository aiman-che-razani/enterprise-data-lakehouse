output "emr_job_role_arn" {
  value = aws_iam_role.emr_job.arn
}

output "redshift_role_arn" {
  value = aws_iam_role.redshift.arn
}

output "api_task_role_arn" {
  value = aws_iam_role.api_task.arn
}

output "data_access_policy_arn" {
  value = aws_iam_policy.data_access.arn
}

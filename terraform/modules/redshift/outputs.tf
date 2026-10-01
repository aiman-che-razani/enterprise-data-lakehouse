output "endpoint" {
  value = aws_redshift_cluster.this.endpoint
}

output "database_name" {
  value = aws_redshift_cluster.this.database_name
}

output "master_password_secret_arn" {
  value = aws_redshift_cluster.this.master_password_secret_arn
}

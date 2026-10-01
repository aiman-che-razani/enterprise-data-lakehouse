output "lakehouse_bucket" {
  value = aws_s3_bucket.this["lakehouse"].id
}

output "artifacts_bucket" {
  value = aws_s3_bucket.this["artifacts"].id
}

output "logs_bucket" {
  value = aws_s3_bucket.this["logs"].id
}

output "bucket_arns" {
  value = { for k, b in aws_s3_bucket.this : k => b.arn }
}

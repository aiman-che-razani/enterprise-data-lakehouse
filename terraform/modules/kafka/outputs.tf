output "cluster_arn" {
  value = aws_msk_cluster.this.arn
}

output "bootstrap_brokers_tls" {
  value = aws_msk_cluster.this.bootstrap_brokers_tls
}

output "bootstrap_brokers_iam" {
  value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

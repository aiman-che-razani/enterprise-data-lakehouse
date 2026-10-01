output "vpc_id" {
  value = aws_vpc.this.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "compute_sg_id" {
  value = aws_security_group.compute.id
}

output "msk_sg_id" {
  value = aws_security_group.msk.id
}

output "redshift_sg_id" {
  value = aws_security_group.redshift.id
}

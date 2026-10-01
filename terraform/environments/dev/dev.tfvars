environment           = "dev"
aws_region            = "us-east-1"
enable_kms            = false
force_destroy_buckets = true

vpc_cidr           = "10.20.0.0/16"
az_count           = 2
single_nat_gateway = true

msk_instance_type = "kafka.t3.small"
msk_broker_count  = 2
msk_ebs_volume_gb = 50

emr_max_cpu    = "16 vCPU"
emr_max_memory = "64 GB"

redshift_node_type = "ra3.xlplus"
redshift_nodes     = 1

environment           = "staging"
aws_region            = "us-east-1"
enable_kms            = true
force_destroy_buckets = false

vpc_cidr           = "10.30.0.0/16"
az_count           = 2
single_nat_gateway = true

msk_instance_type = "kafka.m5.large"
msk_broker_count  = 2
msk_ebs_volume_gb = 200

emr_max_cpu    = "32 vCPU"
emr_max_memory = "128 GB"

redshift_node_type = "ra3.xlplus"
redshift_nodes     = 2

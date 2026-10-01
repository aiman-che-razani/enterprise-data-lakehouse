environment           = "prod"
aws_region            = "us-east-1"
enable_kms            = true
force_destroy_buckets = false

vpc_cidr           = "10.40.0.0/16"
az_count           = 3
single_nat_gateway = false

msk_instance_type = "kafka.m5.xlarge"
msk_broker_count  = 3
msk_ebs_volume_gb = 1000

emr_max_cpu    = "200 vCPU"
emr_max_memory = "800 GB"

redshift_node_type = "ra3.4xlarge"
redshift_nodes     = 3

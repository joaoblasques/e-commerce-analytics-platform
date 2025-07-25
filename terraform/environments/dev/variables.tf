# Development Environment Variables

variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-west-2"
}

variable "project_owner" {
  description = "Project owner for tagging"
  type        = string
  default     = "DataEngineering"
}

variable "cost_center" {
  description = "Cost center for billing"
  type        = string
  default     = "Engineering"
}

# VPC Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_count" {
  description = "Number of public subnets"
  type        = number
  default     = 2
}

variable "private_subnet_count" {
  description = "Number of private subnets"
  type        = number
  default     = 2
}

variable "database_subnet_count" {
  description = "Number of database subnets"
  type        = number
  default     = 2
}

variable "single_nat_gateway_dev" {
  description = "Use single NAT gateway for cost savings in dev"
  type        = bool
  default     = true
}

variable "enable_flow_logs" {
  description = "Enable VPC flow logs"
  type        = bool
  default     = true
}

variable "flow_log_retention_days" {
  description = "VPC flow log retention period"
  type        = number
  default     = 7
}

# Bastion Configuration
variable "enable_bastion" {
  description = "Enable bastion host"
  type        = bool
  default     = false
}

variable "bastion_allowed_cidrs" {
  description = "CIDR blocks allowed to access bastion"
  type        = list(string)
  default     = []
}

# RDS Configuration
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "RDS max allocated storage in GB"
  type        = number
  default     = 100
}

variable "db_backup_retention_period" {
  description = "RDS backup retention period in days"
  type        = number
  default     = 3
}

variable "db_backup_window" {
  description = "RDS backup window"
  type        = string
  default     = "03:00-04:00"
}

variable "db_maintenance_window" {
  description = "RDS maintenance window"
  type        = string
  default     = "sun:04:00-sun:05:00"
}

variable "db_multi_az" {
  description = "Enable RDS Multi-AZ"
  type        = bool
  default     = false
}

variable "db_deletion_protection" {
  description = "Enable RDS deletion protection"
  type        = bool
  default     = false
}

variable "db_skip_final_snapshot" {
  description = "Skip final snapshot when deleting RDS"
  type        = bool
  default     = true
}

variable "create_read_replica" {
  description = "Create RDS read replica"
  type        = bool
  default     = false
}

variable "read_replica_instance_class" {
  description = "RDS read replica instance class"
  type        = string
  default     = "db.t3.micro"
}

# MSK Configuration
variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "2.8.1"
}

variable "kafka_broker_nodes" {
  description = "Number of Kafka broker nodes"
  type        = number
  default     = 2
}

variable "kafka_instance_type" {
  description = "Kafka broker instance type"
  type        = string
  default     = "kafka.t3.small"
}

variable "kafka_ebs_volume_size" {
  description = "Kafka EBS volume size in GB"
  type        = number
  default     = 50
}

# EKS Configuration
variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "eks_public_access_cidrs" {
  description = "CIDR blocks that can access EKS API"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "eks_node_instance_types" {
  description = "EKS node instance types"
  type        = list(string)
  default     = ["t3.medium"]
}

variable "eks_node_desired_size" {
  description = "EKS node desired size"
  type        = number
  default     = 2
}

variable "eks_node_max_size" {
  description = "EKS node max size"
  type        = number
  default     = 4
}

variable "eks_node_min_size" {
  description = "EKS node min size"
  type        = number
  default     = 1
}

variable "eks_node_disk_size" {
  description = "EKS node disk size in GB"
  type        = number
  default     = 30
}

variable "eks_cluster_log_types" {
  description = "EKS cluster log types to enable"
  type        = list(string)
  default     = ["api", "audit", "authenticator"]
}

# Spot Instances Configuration
variable "enable_spot_instances" {
  description = "Enable spot instances for cost savings"
  type        = bool
  default     = true
}

variable "spot_instance_types" {
  description = "Spot instance types"
  type        = list(string)
  default     = ["t3.medium", "t3.large", "m5.large"]
}

variable "spot_desired_size" {
  description = "Spot instances desired size"
  type        = number
  default     = 1
}

variable "spot_max_size" {
  description = "Spot instances max size"
  type        = number
  default     = 3
}

variable "spot_min_size" {
  description = "Spot instances min size"
  type        = number
  default     = 0
}

# Redis Configuration
variable "redis_node_type" {
  description = "Redis node type"
  type        = string
  default     = "cache.t3.micro"
}

variable "redis_num_cache_clusters" {
  description = "Number of Redis cache clusters"
  type        = number
  default     = 1
}

variable "redis_auth_token" {
  description = "Redis auth token"
  type        = string
  default     = null
  sensitive   = true
}

variable "redis_snapshot_retention_limit" {
  description = "Redis snapshot retention limit"
  type        = number
  default     = 1
}

variable "redis_snapshot_window" {
  description = "Redis snapshot window"
  type        = string
  default     = "03:00-05:00"
}

variable "redis_maintenance_window" {
  description = "Redis maintenance window"
  type        = string
  default     = "sun:05:00-sun:06:00"
}

variable "redis_automatic_failover_enabled" {
  description = "Enable Redis automatic failover"
  type        = bool
  default     = false
}

variable "redis_multi_az_enabled" {
  description = "Enable Redis Multi-AZ"
  type        = bool
  default     = false
}

# Application Load Balancer
variable "alb_deletion_protection" {
  description = "Enable ALB deletion protection"
  type        = bool
  default     = false
}

# Logging Configuration
variable "application_log_retention_days" {
  description = "Application log retention period in days"
  type        = number
  default     = 7
}

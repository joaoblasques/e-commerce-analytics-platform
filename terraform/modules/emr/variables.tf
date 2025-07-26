# EMR Cluster Module Variables

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "ecap"
}

variable "vpc_id" {
  description = "VPC ID where EMR cluster will be deployed"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for EMR cluster"
  type        = list(string)
}

variable "emr_security_group_id" {
  description = "Security group ID for EMR cluster"
  type        = string
}

# EMR Cluster Configuration
variable "release_label" {
  description = "EMR release version"
  type        = string
  default     = "emr-7.0.0"
}

variable "applications" {
  description = "List of applications to install on EMR cluster"
  type        = list(string)
  default     = ["Spark", "Hadoop", "Hive", "Livy", "JupyterHub", "Zeppelin"]
}

# Instance Configuration
variable "master_instance_type" {
  description = "EC2 instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "m5.large"
}

variable "core_instance_count" {
  description = "Number of core instances in EMR cluster"
  type        = number
  default     = 2
}

variable "task_instance_type" {
  description = "EC2 instance type for EMR task nodes (spot instances)"
  type        = string
  default     = "m5.large"
}

variable "task_instance_count" {
  description = "Initial number of task instances (spot)"
  type        = number
  default     = 1
}

variable "max_task_instances" {
  description = "Maximum number of task instances for auto-scaling"
  type        = number
  default     = 10
}

# Auto Scaling Configuration
variable "auto_scaling_enabled" {
  description = "Enable EMR auto-scaling"
  type        = bool
  default     = true
}

variable "auto_scaling_min_capacity" {
  description = "Minimum capacity for auto-scaling"
  type        = number
  default     = 1
}

variable "auto_scaling_max_capacity" {
  description = "Maximum capacity for auto-scaling"
  type        = number
  default     = 20
}

variable "scale_up_cooldown" {
  description = "Scale up cooldown period in seconds"
  type        = number
  default     = 300
}

variable "scale_down_cooldown" {
  description = "Scale down cooldown period in seconds"
  type        = number
  default     = 300
}

# Spot Instance Configuration
variable "spot_instance_enabled" {
  description = "Enable spot instances for task nodes"
  type        = bool
  default     = true
}

variable "spot_bid_percentage" {
  description = "Percentage of on-demand price to bid for spot instances"
  type        = number
  default     = 60
}

variable "spot_timeout_action" {
  description = "Action to take when spot instance is terminated"
  type        = string
  default     = "TERMINATE_CLUSTER"
  validation {
    condition     = contains(["TERMINATE_CLUSTER", "SWITCH_TO_ON_DEMAND"], var.spot_timeout_action)
    error_message = "Spot timeout action must be either TERMINATE_CLUSTER or SWITCH_TO_ON_DEMAND."
  }
}

# Storage Configuration
variable "master_ebs_size" {
  description = "EBS volume size for master node (GB)"
  type        = number
  default     = 50
}

variable "core_ebs_size" {
  description = "EBS volume size for core nodes (GB)"
  type        = number
  default     = 100
}

variable "task_ebs_size" {
  description = "EBS volume size for task nodes (GB)"
  type        = number
  default     = 50
}

variable "ebs_volume_type" {
  description = "EBS volume type"
  type        = string
  default     = "gp3"
}

# Logging and Monitoring
variable "log_bucket" {
  description = "S3 bucket for EMR logs"
  type        = string
}

variable "enable_debugging" {
  description = "Enable EMR debugging"
  type        = bool
  default     = true
}

variable "enable_cloudwatch_monitoring" {
  description = "Enable CloudWatch monitoring for EMR"
  type        = bool
  default     = true
}

# Bootstrap Actions
variable "bootstrap_actions" {
  description = "List of bootstrap actions to run on cluster nodes"
  type = list(object({
    name = string
    path = string
    args = list(string)
  }))
  default = []
}

# Security Configuration
variable "security_configuration_name" {
  description = "Name of the EMR security configuration"
  type        = string
  default     = null
}

variable "encryption_at_rest_enabled" {
  description = "Enable encryption at rest"
  type        = bool
  default     = true
}

variable "encryption_in_transit_enabled" {
  description = "Enable encryption in transit"
  type        = bool
  default     = true
}

variable "kms_key_id" {
  description = "KMS key ID for encryption"
  type        = string
}

# Network Configuration
variable "additional_security_group_ids" {
  description = "Additional security group IDs for EMR cluster"
  type        = list(string)
  default     = []
}

variable "emr_managed_master_security_group_id" {
  description = "EMR managed security group for master node"
  type        = string
  default     = null
}

variable "emr_managed_slave_security_group_id" {
  description = "EMR managed security group for slave nodes"
  type        = string
  default     = null
}

# Cost Optimization
variable "terminate_unhealthy_nodes" {
  description = "Terminate unhealthy nodes automatically"
  type        = bool
  default     = true
}

variable "idle_timeout" {
  description = "Idle timeout for auto-termination (seconds)"
  type        = number
  default     = 3600  # 1 hour
}

variable "scale_down_behavior" {
  description = "Scale down behavior configuration"
  type        = string
  default     = "TERMINATE_AT_TASK_COMPLETION"
  validation {
    condition = contains([
      "TERMINATE_AT_INSTANCE_HOUR",
      "TERMINATE_AT_TASK_COMPLETION"
    ], var.scale_down_behavior)
    error_message = "Scale down behavior must be TERMINATE_AT_INSTANCE_HOUR or TERMINATE_AT_TASK_COMPLETION."
  }
}

# Tagging
variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "additional_tags" {
  description = "Additional tags specific to EMR cluster"
  type        = map(string)
  default     = {}
}

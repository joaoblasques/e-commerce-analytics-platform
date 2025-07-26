# Apache Airflow Module Variables

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
  description = "VPC ID where Airflow will be deployed"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for Airflow deployment"
  type        = list(string)
}

# RDS Configuration for Airflow Metadata
variable "airflow_db_instance_class" {
  description = "RDS instance class for Airflow metadata database"
  type        = string
  default     = "db.t3.micro"
}

variable "airflow_db_allocated_storage" {
  description = "Allocated storage for Airflow metadata database (GB)"
  type        = number
  default     = 20
}

variable "airflow_db_username" {
  description = "Username for Airflow metadata database"
  type        = string
  default     = "airflow"
}

variable "airflow_db_password" {
  description = "Password for Airflow metadata database"
  type        = string
  sensitive   = true
}

# Redis Configuration for Celery Backend
variable "redis_node_type" {
  description = "ElastiCache Redis node type"
  type        = string
  default     = "cache.t3.micro"
}

variable "redis_num_cache_nodes" {
  description = "Number of cache nodes in Redis cluster"
  type        = number
  default     = 1
}

variable "redis_parameter_group_name" {
  description = "Parameter group name for Redis"
  type        = string
  default     = "default.redis7"
}

variable "redis_port" {
  description = "Port for Redis"
  type        = number
  default     = 6379
}

# S3 Configuration
variable "airflow_s3_bucket" {
  description = "S3 bucket for Airflow logs and DAGs"
  type        = string
}

variable "kms_key_id" {
  description = "KMS key ID for encryption"
  type        = string
}

# Security Groups
variable "airflow_security_group_id" {
  description = "Security group ID for Airflow components"
  type        = string
  default     = null
}

variable "rds_security_group_id" {
  description = "Security group ID for RDS instance"
  type        = string
  default     = null
}

variable "redis_security_group_id" {
  description = "Security group ID for Redis cluster"
  type        = string
  default     = null
}

# Kubernetes Configuration
variable "kubernetes_namespace" {
  description = "Kubernetes namespace for Airflow deployment"
  type        = string
  default     = "airflow"
}

variable "airflow_image_tag" {
  description = "Airflow Docker image tag"
  type        = string
  default     = "2.8.1-python3.9"
}

variable "airflow_executor" {
  description = "Airflow executor type"
  type        = string
  default     = "CeleryExecutor"
  validation {
    condition = contains([
      "LocalExecutor",
      "CeleryExecutor",
      "KubernetesExecutor",
      "CeleryKubernetesExecutor"
    ], var.airflow_executor)
    error_message = "Executor must be one of: LocalExecutor, CeleryExecutor, KubernetesExecutor, CeleryKubernetesExecutor."
  }
}

# Airflow Configuration
variable "airflow_admin_username" {
  description = "Airflow admin username"
  type        = string
  default     = "admin"
}

variable "airflow_admin_password" {
  description = "Airflow admin password"
  type        = string
  sensitive   = true
}

variable "airflow_admin_email" {
  description = "Airflow admin email"
  type        = string
  default     = "admin@example.com"
}

variable "airflow_admin_firstname" {
  description = "Airflow admin first name"
  type        = string
  default     = "Admin"
}

variable "airflow_admin_lastname" {
  description = "Airflow admin last name"
  type        = string
  default     = "User"
}

# Scaling Configuration
variable "webserver_replicas" {
  description = "Number of Airflow webserver replicas"
  type        = number
  default     = 2
}

variable "scheduler_replicas" {
  description = "Number of Airflow scheduler replicas"
  type        = number
  default     = 2
}

variable "worker_replicas" {
  description = "Number of Airflow worker replicas"
  type        = number
  default     = 3
}

variable "worker_max_replicas" {
  description = "Maximum number of worker replicas for autoscaling"
  type        = number
  default     = 10
}

# Resource Configuration
variable "webserver_resources" {
  description = "Resource requests and limits for webserver"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "100m"
      memory = "256Mi"
    }
    limits = {
      cpu    = "500m"
      memory = "1Gi"
    }
  }
}

variable "scheduler_resources" {
  description = "Resource requests and limits for scheduler"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "100m"
      memory = "256Mi"
    }
    limits = {
      cpu    = "500m"
      memory = "1Gi"
    }
  }
}

variable "worker_resources" {
  description = "Resource requests and limits for workers"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "200m"
      memory = "512Mi"
    }
    limits = {
      cpu    = "1000m"
      memory = "2Gi"
    }
  }
}

# Storage Configuration
variable "logs_volume_size" {
  description = "Size of persistent volume for Airflow logs"
  type        = string
  default     = "10Gi"
}

variable "dags_volume_size" {
  description = "Size of persistent volume for Airflow DAGs"
  type        = string
  default     = "5Gi"
}

# Monitoring Configuration
variable "enable_prometheus_monitoring" {
  description = "Enable Prometheus monitoring for Airflow"
  type        = bool
  default     = true
}

variable "enable_flower" {
  description = "Enable Flower for Celery monitoring"
  type        = bool
  default     = true
}

variable "flower_port" {
  description = "Port for Flower web interface"
  type        = number
  default     = 5555
}

# Load Balancer Configuration
variable "enable_load_balancer" {
  description = "Enable load balancer for Airflow webserver"
  type        = bool
  default     = true
}

variable "load_balancer_type" {
  description = "Type of load balancer (application or network)"
  type        = string
  default     = "application"
  validation {
    condition     = contains(["application", "network"], var.load_balancer_type)
    error_message = "Load balancer type must be either 'application' or 'network'."
  }
}

# EMR Integration
variable "emr_cluster_id" {
  description = "EMR cluster ID for Spark job execution"
  type        = string
  default     = null
}

variable "emr_service_role_arn" {
  description = "EMR service role ARN"
  type        = string
  default     = null
}

variable "emr_instance_profile_arn" {
  description = "EMR instance profile ARN"
  type        = string
  default     = null
}

# Tagging
variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "additional_tags" {
  description = "Additional tags specific to Airflow resources"
  type        = map(string)
  default     = {}
}

# Development Environment Infrastructure
# E-Commerce Analytics Platform - Cloud Infrastructure

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }

  # Uncomment for remote state management
  # backend "s3" {
  #   bucket  = "ecap-terraform-state-dev"
  #   key     = "dev/infrastructure.tfstate"
  #   region  = "us-west-2"
  #   encrypt = true
  # }
}

# AWS Provider Configuration
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

# Local values for consistent tagging and naming
locals {
  environment = "dev"
  name_prefix = "ecap-${local.environment}"

  common_tags = {
    Project     = "E-Commerce Analytics Platform"
    Environment = local.environment
    ManagedBy   = "Terraform"
    Owner       = var.project_owner
    CostCenter  = var.cost_center
    CreatedAt   = timestamp()
  }
}

# Random suffix for globally unique resource names
resource "random_id" "suffix" {
  byte_length = 4
}

# Data source for current AWS account and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# VPC Module
module "vpc" {
  source = "../../modules/vpc"

  name_prefix = local.name_prefix
  vpc_cidr    = var.vpc_cidr

  public_subnet_count   = var.public_subnet_count
  private_subnet_count  = var.private_subnet_count
  database_subnet_count = var.database_subnet_count

  # Cost optimization for dev environment
  single_nat_gateway = var.single_nat_gateway_dev

  cluster_name = "${local.name_prefix}-eks"

  enable_flow_logs           = var.enable_flow_logs
  flow_log_retention_days    = var.flow_log_retention_days

  tags = local.common_tags
}

# Security Groups Module
module "security_groups" {
  source = "../../modules/security-groups"

  name_prefix = local.name_prefix
  vpc_id      = module.vpc.vpc_id

  private_subnet_cidrs = module.vpc.private_subnet_cidr_blocks

  enable_bastion         = var.enable_bastion
  bastion_allowed_cidrs  = var.bastion_allowed_cidrs

  tags = local.common_tags
}

# S3 Data Lake Module
module "s3_data_lake" {
  source = "../../modules/s3"

  bucket_name = "${local.name_prefix}-data-lake-${random_id.suffix.hex}"

  # Development lifecycle policies (shorter retention)
  raw_data_ia_transition_days         = 7
  raw_data_glacier_transition_days    = 30
  raw_data_deep_archive_transition_days = 90
  processed_data_ia_transition_days   = 30
  processed_data_glacier_transition_days = 90
  temp_data_expiration_days          = 3
  logs_expiration_days               = 90

  # Enable cost optimization features
  enable_intelligent_tiering = true
  enable_inventory          = true
  enable_analytics          = false  # Disabled for dev to save costs

  # Development-specific settings
  versioning_enabled = true

  tags = local.common_tags
}

# RDS PostgreSQL Module
module "rds_postgres" {
  source = "../../modules/rds"

  name_prefix            = local.name_prefix
  db_instance_identifier = "${local.name_prefix}-postgres"

  # Development instance sizing
  instance_class    = var.db_instance_class
  allocated_storage = var.db_allocated_storage
  max_allocated_storage = var.db_max_allocated_storage

  # Network configuration
  db_subnet_group_name = module.vpc.database_subnet_group_name
  security_group_id    = module.security_groups.rds_postgres_security_group_id

  # Development backup configuration
  backup_retention_period = var.db_backup_retention_period
  backup_window          = var.db_backup_window
  maintenance_window     = var.db_maintenance_window

  # Development settings
  multi_az                = var.db_multi_az
  deletion_protection     = var.db_deletion_protection
  skip_final_snapshot     = var.db_skip_final_snapshot

  # Performance configuration for development
  performance_insights_enabled = true
  monitoring_interval          = 60

  # Development-specific PostgreSQL settings
  max_connections             = "100"
  work_mem_mb                = 4
  maintenance_work_mem_mb    = 32
  effective_cache_size_mb    = 512

  create_read_replica         = var.create_read_replica
  read_replica_instance_class = var.read_replica_instance_class

  tags = local.common_tags
}

# MSK (Managed Kafka) Module
module "msk_cluster" {
  source = "../../modules/msk"

  cluster_name           = "${local.name_prefix}-kafka"
  kafka_version         = var.kafka_version
  number_of_broker_nodes = var.kafka_broker_nodes

  # Development instance sizing
  broker_instance_type   = var.kafka_instance_type
  broker_ebs_volume_size = var.kafka_ebs_volume_size

  # Network configuration
  subnet_ids        = module.vpc.private_subnet_ids
  security_group_id = module.security_groups.msk_security_group_id

  # Development Kafka configuration
  auto_create_topics_enable = true
  default_replication_factor = min(var.kafka_broker_nodes, 3)
  min_insync_replicas       = min(var.kafka_broker_nodes - 1, 2)

  # Log retention for development
  log_retention_hours = 48  # 2 days for dev

  # Monitoring configuration
  enhanced_monitoring           = "PER_BROKER"
  enable_prometheus_monitoring  = true
  enable_cloudwatch_logs       = true
  cloudwatch_log_retention_days = 30

  # Topic configuration for development
  transactions_topic_partitions     = 3
  user_events_topic_partitions      = 3
  product_updates_topic_partitions  = 2
  fraud_alerts_topic_partitions     = 2

  tags = local.common_tags
}

# EKS Cluster Module
module "eks_cluster" {
  source = "../../modules/eks"

  cluster_name       = "${local.name_prefix}-eks"
  kubernetes_version = var.kubernetes_version

  # Network configuration
  private_subnet_ids = module.vpc.private_subnet_ids
  public_subnet_ids  = module.vpc.public_subnet_ids

  cluster_security_group_id      = module.security_groups.eks_cluster_security_group_id
  worker_nodes_security_group_id = module.security_groups.eks_worker_nodes_security_group_id

  # Development cluster configuration
  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = true
  cluster_endpoint_public_access_cidrs = var.eks_public_access_cidrs

  # Development node group configuration
  node_group_instance_types = var.eks_node_instance_types
  node_group_desired_size   = var.eks_node_desired_size
  node_group_max_size       = var.eks_node_max_size
  node_group_min_size       = var.eks_node_min_size
  node_group_disk_size      = var.eks_node_disk_size

  # Cost optimization with spot instances
  enable_spot_instances = var.enable_spot_instances
  spot_instance_types   = var.spot_instance_types
  spot_desired_size     = var.spot_desired_size
  spot_max_size         = var.spot_max_size
  spot_min_size         = var.spot_min_size

  # S3 access for applications
  s3_bucket_name = module.s3_data_lake.bucket_id

  # Logging configuration
  cluster_enabled_log_types     = var.eks_cluster_log_types
  cloudwatch_log_retention_days = 30

  tags = local.common_tags
}

# ElastiCache Redis Module (simplified for space)
resource "aws_elasticache_subnet_group" "main" {
  name       = "${local.name_prefix}-redis-subnet-group"
  subnet_ids = module.vpc.private_subnet_ids

  tags = local.common_tags
}

resource "aws_elasticache_replication_group" "main" {
  replication_group_id         = "${local.name_prefix}-redis"
  description                  = "Redis cluster for ${local.name_prefix}"

  port                        = 6379
  parameter_group_name        = "default.redis7"
  node_type                   = var.redis_node_type
  num_cache_clusters          = var.redis_num_cache_clusters

  subnet_group_name           = aws_elasticache_subnet_group.main.name
  security_group_ids          = [module.security_groups.elasticache_redis_security_group_id]

  at_rest_encryption_enabled  = true
  transit_encryption_enabled  = true
  auth_token                  = var.redis_auth_token

  snapshot_retention_limit    = var.redis_snapshot_retention_limit
  snapshot_window             = var.redis_snapshot_window
  maintenance_window          = var.redis_maintenance_window

  automatic_failover_enabled  = var.redis_automatic_failover_enabled
  multi_az_enabled           = var.redis_multi_az_enabled

  tags = local.common_tags
}

# Application Load Balancer for EKS services
resource "aws_lb" "main" {
  name               = "${local.name_prefix}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [module.security_groups.alb_security_group_id]
  subnets           = module.vpc.public_subnet_ids

  enable_deletion_protection = var.alb_deletion_protection

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-alb"
    Type = "ApplicationLoadBalancer"
  })
}

# CloudWatch Log Groups for applications
resource "aws_cloudwatch_log_group" "applications" {
  for_each = toset([
    "fastapi",
    "streamlit",
    "spark-jobs",
    "kafka-consumers",
    "data-processing"
  ])

  name              = "/aws/ecap/${local.environment}/${each.key}"
  retention_in_days = var.application_log_retention_days

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-${each.key}-logs"
    Type = "LogGroup"
    Application = each.key
  })
}

# IAM roles for service accounts (IRSA)
resource "aws_iam_role" "application_roles" {
  for_each = toset([
    "fastapi-service",
    "streamlit-service",
    "spark-operator",
    "kafka-consumer"
  ])

  name = "${local.name_prefix}-${each.key}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRoleWithWebIdentity"
        Effect = "Allow"
        Principal = {
          Federated = module.eks_cluster.oidc_provider_arn
        }
        Condition = {
          StringEquals = {
            "${replace(module.eks_cluster.cluster_oidc_issuer_url, "https://", "")}:sub" = "system:serviceaccount:default:${each.key}"
            "${replace(module.eks_cluster.cluster_oidc_issuer_url, "https://", "")}:aud" = "sts.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-${each.key}-role"
    Type = "IAMRole"
    ServiceAccount = each.key
  })
}

# IAM policies for application access to AWS services
resource "aws_iam_policy" "s3_access_policy" {
  name        = "${local.name_prefix}-s3-access-policy"
  description = "Policy for application access to S3 data lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload"
        ]
        Resource = [
          module.s3_data_lake.bucket_arn,
          "${module.s3_data_lake.bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = [module.s3_data_lake.kms_key_arn]
      }
    ]
  })

  tags = local.common_tags
}

# Attach S3 access policy to application roles
resource "aws_iam_role_policy_attachment" "s3_access" {
  for_each = aws_iam_role.application_roles

  role       = each.value.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

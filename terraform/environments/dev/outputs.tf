# Development Environment Outputs

# Network Outputs
output "vpc_id" {
  description = "ID of the VPC"
  value       = module.vpc.vpc_id
}

output "vpc_cidr_block" {
  description = "CIDR block of the VPC"
  value       = module.vpc.vpc_cidr_block
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = module.vpc.public_subnet_ids
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = module.vpc.private_subnet_ids
}

output "database_subnet_ids" {
  description = "IDs of the database subnets"
  value       = module.vpc.database_subnet_ids
}

# EKS Cluster Outputs
output "eks_cluster_endpoint" {
  description = "Endpoint for EKS control plane"
  value       = module.eks_cluster.cluster_endpoint
}

output "eks_cluster_security_group_id" {
  description = "Security group ID attached to the EKS cluster"
  value       = module.eks_cluster.cluster_security_group_id
}

output "eks_cluster_name" {
  description = "Name of the EKS cluster"
  value       = module.eks_cluster.cluster_id
}

output "eks_kubeconfig_update_command" {
  description = "Command to update kubeconfig"
  value       = module.eks_cluster.kubeconfig_update_command
}

output "eks_oidc_provider_arn" {
  description = "ARN of the OIDC Provider for service accounts"
  value       = module.eks_cluster.oidc_provider_arn
}

# RDS Outputs
output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = module.rds_postgres.db_instance_endpoint
}

output "rds_connection_string" {
  description = "PostgreSQL connection string"
  value       = module.rds_postgres.connection_string
  sensitive   = true
}

output "rds_secret_arn" {
  description = "ARN of the RDS password secret"
  value       = module.rds_postgres.db_password_secret_arn
}

# MSK Outputs
output "msk_bootstrap_brokers" {
  description = "MSK bootstrap brokers"
  value       = module.msk_cluster.bootstrap_brokers_tls
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN"
  value       = module.msk_cluster.cluster_arn
}

output "msk_zookeeper_connect_string" {
  description = "MSK Zookeeper connection string"
  value       = module.msk_cluster.zookeeper_connect_string
}

output "kafka_topics_configuration" {
  description = "Kafka topics configuration"
  value       = module.msk_cluster.topics_configuration
}

# S3 Data Lake Outputs
output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = module.s3_data_lake.bucket_id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = module.s3_data_lake.bucket_arn
}

output "s3_kms_key_arn" {
  description = "ARN of the S3 KMS encryption key"
  value       = module.s3_data_lake.kms_key_arn
}

output "data_lake_structure" {
  description = "Data lake folder structure"
  value       = module.s3_data_lake.data_lake_structure
}

# Redis Outputs
output "redis_endpoint" {
  description = "Redis cluster endpoint"
  value       = aws_elasticache_replication_group.main.primary_endpoint_address
}

output "redis_port" {
  description = "Redis cluster port"
  value       = aws_elasticache_replication_group.main.port
}

# Load Balancer Outputs
output "alb_dns_name" {
  description = "DNS name of the Application Load Balancer"
  value       = aws_lb.main.dns_name
}

output "alb_hosted_zone_id" {
  description = "Hosted zone ID of the Application Load Balancer"
  value       = aws_lb.main.zone_id
}

# Environment Configuration for Applications
output "environment_configuration" {
  description = "Environment configuration for applications"
  value = {
    # Infrastructure
    aws_region     = var.aws_region
    environment    = "dev"
    vpc_id         = module.vpc.vpc_id

    # EKS Configuration
    eks_cluster_name     = module.eks_cluster.cluster_id
    eks_cluster_endpoint = module.eks_cluster.cluster_endpoint

    # Database Configuration
    postgres_endpoint = module.rds_postgres.db_instance_endpoint
    postgres_port     = module.rds_postgres.db_instance_port
    postgres_database = module.rds_postgres.db_instance_name
    redis_endpoint    = aws_elasticache_replication_group.main.primary_endpoint_address
    redis_port        = aws_elasticache_replication_group.main.port

    # Kafka Configuration
    kafka_bootstrap_servers = module.msk_cluster.bootstrap_brokers_tls
    kafka_topics = {
      transactions     = "transactions"
      user_events      = "user-events"
      product_updates  = "product-updates"
      fraud_alerts     = "fraud-alerts"
    }

    # S3 Configuration
    s3_bucket_name   = module.s3_data_lake.bucket_id
    s3_bucket_region = module.s3_data_lake.bucket_region
    s3_kms_key_id    = module.s3_data_lake.kms_key_id

    # Load Balancer
    alb_dns_name = aws_lb.main.dns_name
  }
  sensitive = true
}

# Kubernetes Manifests Data
output "kubernetes_config" {
  description = "Kubernetes configuration data"
  value = {
    namespace = "default"

    # Service Account configurations
    service_accounts = {
      for role_name, role in aws_iam_role.application_roles : role_name => {
        name = role_name
        annotations = {
          "eks.amazonaws.com/role-arn" = role.arn
        }
      }
    }

    # ConfigMap data
    configmap_data = {
      "AWS_REGION"              = var.aws_region
      "ENVIRONMENT"             = "dev"
      "S3_BUCKET_NAME"          = module.s3_data_lake.bucket_id
      "KAFKA_BOOTSTRAP_SERVERS" = module.msk_cluster.bootstrap_brokers_tls
      "REDIS_ENDPOINT"          = aws_elasticache_replication_group.main.primary_endpoint_address
      "REDIS_PORT"              = tostring(aws_elasticache_replication_group.main.port)
    }

    # Secret data (references to AWS Secrets Manager)
    secret_refs = {
      postgres_credentials = module.rds_postgres.db_password_secret_arn
    }
  }
}

# Spark Configuration
output "spark_configuration" {
  description = "Spark configuration for data processing"
  value = merge(
    module.s3_data_lake.spark_configuration,
    {
      "spark.kubernetes.namespace"                     = "default"
      "spark.kubernetes.authenticate.driver.serviceAccountName" = "spark-operator"
      "spark.hadoop.fs.s3a.bucket.${module.s3_data_lake.bucket_id}.endpoint" = "s3.${var.aws_region}.amazonaws.com"
      "spark.sql.warehouse.dir"                       = "s3a://${module.s3_data_lake.bucket_id}/delta/warehouse/"
      "spark.sql.streaming.checkpointLocation"        = "s3a://${module.s3_data_lake.bucket_id}/temp/checkpoints/"
    }
  )
}

# Delta Lake Configuration
output "delta_lake_configuration" {
  description = "Delta Lake configuration"
  value = merge(
    module.s3_data_lake.delta_lake_configuration,
    {
      catalog_endpoint = "http://delta-catalog.default.svc.cluster.local:8080"
      metastore_uri    = "thrift://delta-metastore.default.svc.cluster.local:9083"
    }
  )
}

# Monitoring Configuration
output "monitoring_configuration" {
  description = "Monitoring and observability configuration"
  value = {
    cloudwatch_log_groups = {
      for name, log_group in aws_cloudwatch_log_group.applications : name => {
        name              = log_group.name
        retention_in_days = log_group.retention_in_days
      }
    }

    # Prometheus targets
    prometheus_targets = {
      kafka_metrics     = "${module.msk_cluster.cluster_name}:9092"
      postgres_metrics  = "${module.rds_postgres.db_instance_endpoint}:5432"
      redis_metrics     = "${aws_elasticache_replication_group.main.primary_endpoint_address}:6379"
    }

    # Grafana dashboard endpoints
    grafana_dashboards = {
      kafka      = "/d/kafka/kafka-cluster"
      postgres   = "/d/postgres/postgresql-database"
      redis      = "/d/redis/redis-cluster"
      kubernetes = "/d/k8s/kubernetes-cluster"
    }
  }
}

# Cost Optimization Summary
output "cost_optimization_summary" {
  description = "Summary of cost optimization features enabled"
  value = {
    vpc = {
      single_nat_gateway = var.single_nat_gateway_dev
      estimated_savings  = "~$45/month vs multi-NAT"
    }

    ec2 = {
      spot_instances_enabled = var.enable_spot_instances
      estimated_savings      = "~60-70% vs on-demand"
    }

    rds = {
      instance_class    = var.db_instance_class
      multi_az_disabled = !var.db_multi_az
      estimated_cost    = "~$15-20/month"
    }

    s3 = merge(
      module.s3_data_lake.cost_optimization_features,
      {
        estimated_storage_cost = "~$0.023/GB/month standard, reduces with lifecycle"
      }
    )

    kafka = {
      instance_type     = var.kafka_instance_type
      broker_count      = var.kafka_broker_nodes
      estimated_cost    = "~$40-60/month"
    }

    total_estimated_monthly_cost = "$100-150/month for development environment"
  }
}

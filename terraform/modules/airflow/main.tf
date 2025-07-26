# Apache Airflow Module - Main Configuration

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Random password for Airflow Fernet key
resource "random_password" "fernet_key" {
  length  = 32
  special = false
}

# KMS key for Airflow-specific encryption
resource "aws_kms_key" "airflow" {
  description             = "KMS key for ${var.project_name}-${var.environment} Airflow encryption"
  deletion_window_in_days = 7

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-kms"
    Service = "Airflow"
  })
}

resource "aws_kms_alias" "airflow" {
  name          = "alias/${var.project_name}-${var.environment}-airflow"
  target_key_id = aws_kms_key.airflow.key_id
}

# S3 bucket for Airflow DAGs and logs
resource "aws_s3_bucket" "airflow_bucket" {
  bucket = "${var.project_name}-${var.environment}-airflow-${random_id.bucket_suffix.hex}"

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow"
    Service = "Airflow"
  })
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket_versioning" "airflow_bucket_versioning" {
  bucket = aws_s3_bucket.airflow_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "airflow_bucket_encryption" {
  bucket = aws_s3_bucket.airflow_bucket.id

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.airflow.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }
}

resource "aws_s3_bucket_public_access_block" "airflow_bucket_pab" {
  bucket = aws_s3_bucket.airflow_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# RDS PostgreSQL instance for Airflow metadata
resource "aws_db_subnet_group" "airflow" {
  name       = "${var.project_name}-${var.environment}-airflow-db-subnet-group"
  subnet_ids = var.private_subnet_ids

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-db-subnet-group"
    Service = "Airflow"
  })
}

resource "aws_db_instance" "airflow" {
  identifier = "${var.project_name}-${var.environment}-airflow-db"

  allocated_storage     = var.airflow_db_allocated_storage
  max_allocated_storage = var.airflow_db_allocated_storage * 2
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id           = aws_kms_key.airflow.arn

  engine         = "postgres"
  engine_version = "15.4"
  instance_class = var.airflow_db_instance_class

  db_name  = "airflow"
  username = var.airflow_db_username
  password = var.airflow_db_password

  vpc_security_group_ids = var.rds_security_group_id != null ? [var.rds_security_group_id] : []
  db_subnet_group_name   = aws_db_subnet_group.airflow.name

  backup_retention_period = var.environment == "prod" ? 7 : 3
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"

  skip_final_snapshot = var.environment != "prod"
  deletion_protection = var.environment == "prod"

  performance_insights_enabled = var.environment == "prod"
  monitoring_interval         = var.environment == "prod" ? 60 : 0

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-db"
    Service = "Airflow"
  })
}

# ElastiCache Redis cluster for Celery backend
resource "aws_elasticache_subnet_group" "airflow" {
  name       = "${var.project_name}-${var.environment}-airflow-redis-subnet-group"
  subnet_ids = var.private_subnet_ids

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-redis-subnet-group"
    Service = "Airflow"
  })
}

resource "aws_elasticache_cluster" "airflow" {
  cluster_id           = "${var.project_name}-${var.environment}-airflow-redis"
  engine               = "redis"
  node_type           = var.redis_node_type
  num_cache_nodes     = var.redis_num_cache_nodes
  parameter_group_name = var.redis_parameter_group_name
  port                = var.redis_port

  subnet_group_name = aws_elasticache_subnet_group.airflow.name
  security_group_ids = var.redis_security_group_id != null ? [var.redis_security_group_id] : []

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                  = random_password.redis_auth_token.result

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-redis"
    Service = "Airflow"
  })
}

resource "random_password" "redis_auth_token" {
  length  = 32
  special = false
}

# IAM Role for Airflow to access AWS services
resource "aws_iam_role" "airflow_execution_role" {
  name = "${var.project_name}-${var.environment}-airflow-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRoleWithWebIdentity"
        Effect = "Allow"
        Principal = {
          Federated = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/oidc.eks.${data.aws_region.current.name}.amazonaws.com/id/*"
        }
      }
    ]
  })

  tags = merge(var.tags, var.additional_tags, {
    Name    = "${var.project_name}-${var.environment}-airflow-execution-role"
    Service = "Airflow"
  })
}

resource "aws_iam_role_policy" "airflow_execution_policy" {
  name = "${var.project_name}-${var.environment}-airflow-execution-policy"
  role = aws_iam_role.airflow_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.airflow_bucket.arn,
          "${aws_s3_bucket.airflow_bucket.arn}/*",
          "arn:aws:s3:::${var.airflow_s3_bucket}",
          "arn:aws:s3:::${var.airflow_s3_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "emr:RunJobFlow",
          "emr:DescribeCluster",
          "emr:DescribeStep",
          "emr:AddJobFlowSteps",
          "emr:ListClusters",
          "emr:ListSteps",
          "emr:TerminateJobFlows"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          var.emr_service_role_arn != null ? var.emr_service_role_arn : "",
          var.emr_instance_profile_arn != null ? var.emr_instance_profile_arn : ""
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          aws_kms_key.airflow.arn,
          var.kms_key_id
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics"
        ]
        Resource = "*"
      }
    ]
  })
}

# Kubernetes namespace for Airflow
resource "kubernetes_namespace" "airflow" {
  metadata {
    name = var.kubernetes_namespace

    labels = {
      name        = var.kubernetes_namespace
      environment = var.environment
      service     = "airflow"
    }
  }
}

# Kubernetes secret for Airflow configuration
resource "kubernetes_secret" "airflow_config" {
  metadata {
    name      = "airflow-config"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  data = {
    # Database connection
    database-host     = aws_db_instance.airflow.address
    database-port     = tostring(aws_db_instance.airflow.port)
    database-name     = aws_db_instance.airflow.db_name
    database-username = aws_db_instance.airflow.username
    database-password = var.airflow_db_password

    # Redis connection
    redis-host     = aws_elasticache_cluster.airflow.cluster_address
    redis-port     = tostring(aws_elasticache_cluster.airflow.port)
    redis-password = random_password.redis_auth_token.result

    # Airflow secrets
    fernet-key        = base64encode(random_password.fernet_key.result)
    webserver-secret  = random_password.webserver_secret.result
    admin-username    = var.airflow_admin_username
    admin-password    = var.airflow_admin_password
    admin-email       = var.airflow_admin_email
    admin-firstname   = var.airflow_admin_firstname
    admin-lastname    = var.airflow_admin_lastname
  }

  type = "Opaque"
}

resource "random_password" "webserver_secret" {
  length  = 32
  special = true
}

# Kubernetes secret for AWS credentials
resource "kubernetes_secret" "aws_credentials" {
  metadata {
    name      = "aws-credentials"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  data = {
    role-arn         = aws_iam_role.airflow_execution_role.arn
    s3-bucket        = aws_s3_bucket.airflow_bucket.id
    kms-key-id       = aws_kms_key.airflow.key_id
    emr-cluster-id   = var.emr_cluster_id != null ? var.emr_cluster_id : ""
  }

  type = "Opaque"
}

# Persistent Volume Claims for Airflow components
resource "kubernetes_persistent_volume_claim" "airflow_logs" {
  metadata {
    name      = "airflow-logs"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  spec {
    access_modes = ["ReadWriteMany"]
    resources {
      requests = {
        storage = var.logs_volume_size
      }
    }
    storage_class_name = "efs-sc"  # Assuming EFS storage class is available
  }
}

resource "kubernetes_persistent_volume_claim" "airflow_dags" {
  metadata {
    name      = "airflow-dags"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  spec {
    access_modes = ["ReadWriteMany"]
    resources {
      requests = {
        storage = var.dags_volume_size
      }
    }
    storage_class_name = "efs-sc"  # Assuming EFS storage class is available
  }
}

# Service Account for Airflow with IAM role binding
resource "kubernetes_service_account" "airflow" {
  metadata {
    name      = "airflow"
    namespace = kubernetes_namespace.airflow.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.airflow_execution_role.arn
    }
  }
}

# Helm release for Apache Airflow
resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  version    = "1.11.0"  # Use latest stable version
  namespace  = kubernetes_namespace.airflow.metadata[0].name

  # Core configuration
  set {
    name  = "images.airflow.repository"
    value = "apache/airflow"
  }

  set {
    name  = "images.airflow.tag"
    value = var.airflow_image_tag
  }

  set {
    name  = "executor"
    value = var.airflow_executor
  }

  # Database configuration
  set {
    name  = "postgresql.enabled"
    value = "false"
  }

  set {
    name  = "data.metadataConnection.host"
    value = aws_db_instance.airflow.address
  }

  set {
    name  = "data.metadataConnection.port"
    value = tostring(aws_db_instance.airflow.port)
  }

  set {
    name  = "data.metadataConnection.db"
    value = aws_db_instance.airflow.db_name
  }

  set {
    name  = "data.metadataConnection.user"
    value = aws_db_instance.airflow.username
  }

  set_sensitive {
    name  = "data.metadataConnection.pass"
    value = var.airflow_db_password
  }

  # Redis configuration
  set {
    name  = "redis.enabled"
    value = var.airflow_executor == "CeleryExecutor" ? "false" : "false"
  }

  set {
    name  = "data.brokerUrl"
    value = var.airflow_executor == "CeleryExecutor" ? "redis://:${random_password.redis_auth_token.result}@${aws_elasticache_cluster.airflow.cluster_address}:${aws_elasticache_cluster.airflow.port}/0" : ""
  }

  # Webserver configuration
  set {
    name  = "webserver.replicas"
    value = var.webserver_replicas
  }

  set {
    name  = "webserver.resources.requests.cpu"
    value = var.webserver_resources.requests.cpu
  }

  set {
    name  = "webserver.resources.requests.memory"
    value = var.webserver_resources.requests.memory
  }

  set {
    name  = "webserver.resources.limits.cpu"
    value = var.webserver_resources.limits.cpu
  }

  set {
    name  = "webserver.resources.limits.memory"
    value = var.webserver_resources.limits.memory
  }

  # Scheduler configuration
  set {
    name  = "scheduler.replicas"
    value = var.scheduler_replicas
  }

  set {
    name  = "scheduler.resources.requests.cpu"
    value = var.scheduler_resources.requests.cpu
  }

  set {
    name  = "scheduler.resources.requests.memory"
    value = var.scheduler_resources.requests.memory
  }

  set {
    name  = "scheduler.resources.limits.cpu"
    value = var.scheduler_resources.limits.cpu
  }

  set {
    name  = "scheduler.resources.limits.memory"
    value = var.scheduler_resources.limits.memory
  }

  # Worker configuration (for CeleryExecutor)
  dynamic "set" {
    for_each = var.airflow_executor == "CeleryExecutor" ? [1] : []
    content {
      name  = "workers.replicas"
      value = var.worker_replicas
    }
  }

  dynamic "set" {
    for_each = var.airflow_executor == "CeleryExecutor" ? [1] : []
    content {
      name  = "workers.resources.requests.cpu"
      value = var.worker_resources.requests.cpu
    }
  }

  dynamic "set" {
    for_each = var.airflow_executor == "CeleryExecutor" ? [1] : []
    content {
      name  = "workers.resources.requests.memory"
      value = var.worker_resources.requests.memory
    }
  }

  # Flower configuration (for CeleryExecutor)
  dynamic "set" {
    for_each = var.enable_flower && var.airflow_executor == "CeleryExecutor" ? [1] : []
    content {
      name  = "flower.enabled"
      value = "true"
    }
  }

  # Monitoring configuration
  set {
    name  = "statsd.enabled"
    value = var.enable_prometheus_monitoring ? "true" : "false"
  }

  # Service account
  set {
    name  = "serviceAccount.create"
    value = "false"
  }

  set {
    name  = "serviceAccount.name"
    value = kubernetes_service_account.airflow.metadata[0].name
  }

  # Storage configuration
  set {
    name  = "logs.persistence.enabled"
    value = "true"
  }

  set {
    name  = "logs.persistence.existingClaim"
    value = kubernetes_persistent_volume_claim.airflow_logs.metadata[0].name
  }

  set {
    name  = "dags.persistence.enabled"
    value = "true"
  }

  set {
    name  = "dags.persistence.existingClaim"
    value = kubernetes_persistent_volume_claim.airflow_dags.metadata[0].name
  }

  # Security
  set_sensitive {
    name  = "webserver.secretKey"
    value = random_password.webserver_secret.result
  }

  set_sensitive {
    name  = "fernetKey"
    value = base64encode(random_password.fernet_key.result)
  }

  depends_on = [
    kubernetes_namespace.airflow,
    kubernetes_secret.airflow_config,
    kubernetes_secret.aws_credentials,
    kubernetes_service_account.airflow,
    kubernetes_persistent_volume_claim.airflow_logs,
    kubernetes_persistent_volume_claim.airflow_dags,
    aws_db_instance.airflow,
    aws_elasticache_cluster.airflow
  ]
}

# Load Balancer service for Airflow webserver
resource "kubernetes_service" "airflow_webserver_lb" {
  count = var.enable_load_balancer ? 1 : 0

  metadata {
    name      = "airflow-webserver-lb"
    namespace = kubernetes_namespace.airflow.metadata[0].name
    annotations = {
      "service.beta.kubernetes.io/aws-load-balancer-type" = var.load_balancer_type
      "service.beta.kubernetes.io/aws-load-balancer-backend-protocol" = "http"
      "service.beta.kubernetes.io/aws-load-balancer-ssl-ports" = "https"
    }
  }

  spec {
    selector = {
      component = "webserver"
      app       = "airflow"
    }

    port {
      name        = "airflow-ui"
      port        = 80
      target_port = 8080
      protocol    = "TCP"
    }

    type = "LoadBalancer"
  }

  depends_on = [helm_release.airflow]
}

# HorizontalPodAutoscaler for workers (if using CeleryExecutor)
resource "kubernetes_horizontal_pod_autoscaler_v2" "airflow_workers" {
  count = var.airflow_executor == "CeleryExecutor" ? 1 : 0

  metadata {
    name      = "airflow-workers-hpa"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = "airflow-worker"
    }

    min_replicas = var.worker_replicas
    max_replicas = var.worker_max_replicas

    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = 70
        }
      }
    }

    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = 80
        }
      }
    }
  }

  depends_on = [helm_release.airflow]
}

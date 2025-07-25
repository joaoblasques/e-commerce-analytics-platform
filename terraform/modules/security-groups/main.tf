# Security Groups Module for E-Commerce Analytics Platform
# Provides comprehensive security group configuration with least privilege access

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Application Load Balancer Security Group
resource "aws_security_group" "alb" {
  name_prefix = "${var.name_prefix}-alb-"
  vpc_id      = var.vpc_id
  description = "Security group for Application Load Balancer"

  # HTTP access from anywhere
  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTPS access from anywhere
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-alb-sg"
    Type = "SecurityGroup"
    Component = "LoadBalancer"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# EKS Cluster Security Group
resource "aws_security_group" "eks_cluster" {
  name_prefix = "${var.name_prefix}-eks-cluster-"
  vpc_id      = var.vpc_id
  description = "Security group for EKS cluster control plane"

  # HTTPS API access from worker nodes
  ingress {
    description     = "HTTPS API access from worker nodes"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_worker_nodes.id]
  }

  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-eks-cluster-sg"
    Type = "SecurityGroup"
    Component = "EKS-Cluster"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# EKS Worker Nodes Security Group
resource "aws_security_group" "eks_worker_nodes" {
  name_prefix = "${var.name_prefix}-eks-worker-"
  vpc_id      = var.vpc_id
  description = "Security group for EKS worker nodes"

  # Access from ALB
  ingress {
    description     = "Access from ALB"
    from_port       = 30000
    to_port         = 65535
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # Inter-node communication
  ingress {
    description = "Inter-node communication"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  # Access from cluster control plane
  ingress {
    description     = "Access from cluster control plane"
    from_port       = 1025
    to_port         = 65535
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_cluster.id]
  }

  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-eks-worker-sg"
    Type = "SecurityGroup"
    Component = "EKS-Workers"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# RDS PostgreSQL Security Group
resource "aws_security_group" "rds_postgres" {
  name_prefix = "${var.name_prefix}-rds-postgres-"
  vpc_id      = var.vpc_id
  description = "Security group for RDS PostgreSQL database"

  # PostgreSQL access from EKS worker nodes
  ingress {
    description     = "PostgreSQL access from EKS workers"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_worker_nodes.id]
  }

  # PostgreSQL access from EMR
  ingress {
    description     = "PostgreSQL access from EMR"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  # PostgreSQL access from bastion (if enabled)
  dynamic "ingress" {
    for_each = var.enable_bastion ? [1] : []
    content {
      description     = "PostgreSQL access from bastion"
      from_port       = 5432
      to_port         = 5432
      protocol        = "tcp"
      security_groups = [aws_security_group.bastion[0].id]
    }
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-rds-postgres-sg"
    Type = "SecurityGroup"
    Component = "RDS-PostgreSQL"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ElastiCache Redis Security Group
resource "aws_security_group" "elasticache_redis" {
  name_prefix = "${var.name_prefix}-elasticache-redis-"
  vpc_id      = var.vpc_id
  description = "Security group for ElastiCache Redis cluster"

  # Redis access from EKS worker nodes
  ingress {
    description     = "Redis access from EKS workers"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_worker_nodes.id]
  }

  # Redis access from EMR
  ingress {
    description     = "Redis access from EMR"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-elasticache-redis-sg"
    Type = "SecurityGroup"
    Component = "ElastiCache-Redis"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# MSK (Managed Kafka) Security Group
resource "aws_security_group" "msk" {
  name_prefix = "${var.name_prefix}-msk-"
  vpc_id      = var.vpc_id
  description = "Security group for MSK (Managed Kafka) cluster"

  # Kafka broker access from EKS workers
  ingress {
    description     = "Kafka broker access from EKS workers"
    from_port       = 9092
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_worker_nodes.id]
  }

  # Kafka broker access from EMR
  ingress {
    description     = "Kafka broker access from EMR"
    from_port       = 9092
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  # Zookeeper access from EKS workers
  ingress {
    description     = "Zookeeper access from EKS workers"
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_worker_nodes.id]
  }

  # Zookeeper access from EMR
  ingress {
    description     = "Zookeeper access from EMR"
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  # Inter-broker communication
  ingress {
    description = "Inter-broker communication"
    from_port   = 9092
    to_port     = 9094
    protocol    = "tcp"
    self        = true
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-msk-sg"
    Type = "SecurityGroup"
    Component = "MSK-Kafka"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# EMR Security Group
resource "aws_security_group" "emr" {
  name_prefix = "${var.name_prefix}-emr-"
  vpc_id      = var.vpc_id
  description = "Security group for EMR cluster"

  # EMR master node access
  ingress {
    description = "EMR master node access"
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = var.private_subnet_cidrs
  }

  # Spark History Server
  ingress {
    description = "Spark History Server"
    from_port   = 18080
    to_port     = 18080
    protocol    = "tcp"
    cidr_blocks = var.private_subnet_cidrs
  }

  # Hadoop NameNode
  ingress {
    description = "Hadoop NameNode"
    from_port   = 9870
    to_port     = 9870
    protocol    = "tcp"
    cidr_blocks = var.private_subnet_cidrs
  }

  # Inter-cluster communication
  ingress {
    description = "Inter-cluster communication"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-emr-sg"
    Type = "SecurityGroup"
    Component = "EMR"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Bastion Host Security Group (optional)
resource "aws_security_group" "bastion" {
  count = var.enable_bastion ? 1 : 0

  name_prefix = "${var.name_prefix}-bastion-"
  vpc_id      = var.vpc_id
  description = "Security group for bastion host"

  # SSH access from allowed IPs
  ingress {
    description = "SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.bastion_allowed_cidrs
  }

  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-bastion-sg"
    Type = "SecurityGroup"
    Component = "Bastion"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Monitoring Security Group (for Prometheus/Grafana on EKS)
resource "aws_security_group" "monitoring" {
  name_prefix = "${var.name_prefix}-monitoring-"
  vpc_id      = var.vpc_id
  description = "Security group for monitoring services (Prometheus/Grafana)"

  # Prometheus access from ALB
  ingress {
    description     = "Prometheus access from ALB"
    from_port       = 9090
    to_port         = 9090
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # Grafana access from ALB
  ingress {
    description     = "Grafana access from ALB"
    from_port       = 3000
    to_port         = 3000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # Alertmanager access from ALB
  ingress {
    description     = "Alertmanager access from ALB"
    from_port       = 9093
    to_port         = 9093
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # Metrics collection from EKS workers
  ingress {
    description     = "Metrics collection from EKS workers"
    from_port       = 9100
    to_port         = 9200
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_worker_nodes.id]
  }

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-monitoring-sg"
    Type = "SecurityGroup"
    Component = "Monitoring"
  })

  lifecycle {
    create_before_destroy = true
  }
}

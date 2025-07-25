# E-Commerce Analytics Platform - Cloud Infrastructure

This directory contains Terraform infrastructure as code for deploying the E-Commerce Analytics Platform to AWS cloud.

## Architecture Overview

The infrastructure consists of the following components:

### Core Infrastructure
- **VPC**: Multi-AZ virtual private cloud with public, private, and database subnets
- **Security Groups**: Least-privilege security configuration for all services
- **Application Load Balancer**: For ingress traffic to applications

### Compute & Orchestration
- **EKS Cluster**: Managed Kubernetes for containerized applications
- **EC2 Spot Instances**: Cost-optimized compute for development workloads
- **Auto Scaling Groups**: Automatic scaling based on demand

### Data Services
- **MSK (Managed Kafka)**: Streaming data ingestion and processing
- **RDS PostgreSQL**: Operational database with Multi-AZ support
- **ElastiCache Redis**: In-memory caching layer
- **S3 Data Lake**: Object storage with lifecycle management

### Security & Monitoring
- **KMS Encryption**: Data encryption at rest and in transit
- **IAM Roles**: Service accounts with least-privilege access
- **CloudWatch**: Logging, monitoring, and alerting
- **VPC Flow Logs**: Network traffic monitoring

## Directory Structure

```
terraform/
├── modules/                    # Reusable Terraform modules
│   ├── vpc/                   # VPC and networking
│   ├── security-groups/       # Security group configurations
│   ├── eks/                   # EKS cluster and node groups
│   ├── rds/                   # PostgreSQL database
│   ├── msk/                   # Managed Kafka service
│   └── s3/                    # S3 data lake storage
├── environments/              # Environment-specific configurations
│   ├── dev/                   # Development environment
│   ├── staging/               # Staging environment (future)
│   └── prod/                  # Production environment (future)
└── README.md                  # This file
```

## Prerequisites

### Required Tools
1. **Terraform** >= 1.5.0
2. **AWS CLI** >= 2.0.0
3. **kubectl** >= 1.28.0
4. **Helm** >= 3.0.0

### AWS Configuration
1. Configure AWS credentials:
   ```bash
   aws configure
   ```

2. Ensure your AWS account has the following:
   - Sufficient service limits for EC2, EKS, RDS, MSK
   - IAM permissions for creating resources
   - S3 bucket for Terraform state (optional)

### Cost Considerations
- **Development Environment**: ~$100-150/month
- **Staging Environment**: ~$200-300/month
- **Production Environment**: ~$500-1000/month

Cost optimizations included:
- Spot instances for development
- Single NAT gateway for development
- Lifecycle policies for S3 storage
- Right-sized instances for each environment

## Quick Start - Development Environment

### 1. Deploy Infrastructure

```bash
# Navigate to dev environment
cd terraform/environments/dev

# Initialize Terraform
terraform init

# Review the planned changes
terraform plan

# Deploy infrastructure
terraform apply
```

### 2. Configure kubectl

```bash
# Update kubeconfig for EKS cluster
aws eks update-kubeconfig --region us-west-2 --name ecap-dev-eks

# Verify cluster access
kubectl get nodes
```

### 3. Deploy Applications

```bash
# Apply Kubernetes manifests (from project root)
kubectl apply -f k8s/dev/

# Verify deployments
kubectl get pods -A
```

### 4. Access Services

```bash
# Get ALB DNS name
terraform output alb_dns_name

# Access Grafana (if deployed)
kubectl port-forward svc/grafana 3000:3000

# Access Streamlit dashboard (if deployed)
kubectl port-forward svc/streamlit 8501:8501
```

## Module Documentation

### VPC Module
- **Purpose**: Provides networking foundation with multi-AZ subnets
- **Features**: NAT gateways, route tables, flow logs, EKS-ready tagging
- **Cost Optimization**: Single NAT gateway option for development

### Security Groups Module
- **Purpose**: Implements least-privilege network security
- **Components**: ALB, EKS, RDS, MSK, Redis, monitoring security groups
- **Features**: Minimal required access, documented ingress/egress rules

### EKS Module
- **Purpose**: Managed Kubernetes cluster for applications
- **Features**: OIDC provider, managed node groups, spot instances
- **Monitoring**: CloudWatch logs, enhanced monitoring
- **Security**: Encryption at rest, private API endpoint support

### RDS Module
- **Purpose**: Managed PostgreSQL for operational data
- **Features**: Multi-AZ, encryption, automated backups, monitoring
- **Performance**: Optimized parameters for analytics workloads
- **Security**: Secrets Manager integration, network isolation

### MSK Module
- **Purpose**: Managed Kafka for streaming data
- **Features**: Multi-AZ brokers, encryption, monitoring
- **Topics**: Pre-configured topics for e-commerce data
- **Monitoring**: CloudWatch metrics, Prometheus integration

### S3 Module
- **Purpose**: Data lake storage with lifecycle management
- **Features**: Intelligent tiering, encryption, versioning
- **Structure**: Organized folder structure for analytics
- **Cost Optimization**: Automated lifecycle transitions

## Environment Management

### Development Environment
- **Purpose**: Feature development and testing
- **Instance Sizes**: Small (t3.micro, t3.small)
- **High Availability**: Single AZ for cost savings
- **Data Retention**: Short retention periods
- **Cost**: ~$100-150/month

### Staging Environment (Future)
- **Purpose**: Pre-production testing
- **Instance Sizes**: Medium (t3.medium, m5.large)
- **High Availability**: Multi-AZ where critical
- **Data Retention**: Medium retention periods
- **Cost**: ~$200-300/month

### Production Environment (Future)
- **Purpose**: Live production workloads
- **Instance Sizes**: Large (m5.xlarge, c5.2xlarge)
- **High Availability**: Full Multi-AZ deployment
- **Data Retention**: Full retention with archival
- **Cost**: ~$500-1000/month

## Security Best Practices

### Network Security
- All resources deployed in private subnets
- Security groups with minimal required access
- No direct internet access to data services
- VPC Flow Logs enabled for monitoring

### Data Encryption
- All data encrypted at rest using KMS
- Encryption in transit for all services
- Separate KMS keys per service
- Key rotation enabled

### Access Control
- IAM roles with least-privilege access
- Service accounts using IRSA (IAM Roles for Service Accounts)
- No hardcoded credentials
- Secrets stored in AWS Secrets Manager

### Monitoring & Auditing
- CloudWatch logs for all services
- CloudTrail for API auditing
- VPC Flow Logs for network monitoring
- Automated alerting for security events

## Monitoring & Observability

### CloudWatch Integration
- Application logs centralized in CloudWatch
- Custom metrics for business KPIs
- Automated alerting for system health
- Cost monitoring and budget alerts

### Prometheus & Grafana
- Metrics collection from all services
- Pre-built dashboards for monitoring
- Custom alerting rules
- Integration with EKS cluster

### Application Monitoring
- Distributed tracing for microservices
- Performance monitoring for APIs
- Error tracking and alerting
- User experience monitoring

## Disaster Recovery

### Backup Strategy
- RDS automated backups with point-in-time recovery
- S3 versioning and cross-region replication
- EKS cluster state in git repositories
- Infrastructure as code for rapid rebuilding

### Recovery Procedures
- Database restore from backups
- Infrastructure rebuild using Terraform
- Application deployment using CI/CD
- Data recovery from S3 versioning

## Cost Optimization

### Implemented Optimizations
1. **Spot Instances**: 60-70% savings on compute
2. **S3 Lifecycle Policies**: Automatic storage tier transitions
3. **Right-Sized Instances**: Appropriate sizing per environment
4. **Reserved Instances**: For production predictable workloads
5. **Automated Scaling**: Scale down during off-hours

### Cost Monitoring
- Budget alerts for cost overruns
- Daily cost reports
- Resource tagging for cost allocation
- Regular cost optimization reviews

## Troubleshooting

### Common Issues

#### Terraform Apply Fails
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify service limits
aws service-quotas list-service-quotas --service-code ec2

# Clean up failed resources
terraform refresh
terraform plan
```

#### EKS Cluster Access Issues
```bash
# Update kubeconfig
aws eks update-kubeconfig --region us-west-2 --name ecap-dev-eks

# Check IAM permissions
aws iam get-user
aws eks describe-cluster --name ecap-dev-eks
```

#### RDS Connection Issues
```bash
# Test connectivity from EKS
kubectl run -it --rm debug --image=postgres:15 --restart=Never -- bash
psql -h <rds-endpoint> -U ecap_admin -d ecommerce_analytics
```

### Support
- Check Terraform documentation: https://registry.terraform.io/providers/hashicorp/aws
- AWS support for service-specific issues
- Kubernetes documentation for EKS issues
- Project team for application-specific problems

## Next Steps

1. **Complete Task 5.1.1**: Finish infrastructure module development
2. **Task 5.1.2**: Create Kubernetes deployment manifests
3. **Task 5.1.3**: Implement secrets and configuration management
4. **Task 5.2.x**: Add comprehensive monitoring and alerting
5. **Task 5.3.x**: Set up production data pipeline

## Contributing

1. Follow Terraform best practices
2. Test all changes in development environment
3. Update documentation for any changes
4. Use consistent naming conventions
5. Implement appropriate security controls

## License

This infrastructure code is part of the E-Commerce Analytics Platform project and follows the project's MIT license.

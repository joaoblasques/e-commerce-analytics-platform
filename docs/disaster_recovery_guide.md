# Disaster Recovery and Backup System - Operational Guide

## Overview

The E-Commerce Analytics Platform (ECAP) implements a comprehensive disaster recovery and backup system designed to ensure business continuity with strict RTO (Recovery Time Objective) of 15 minutes and RPO (Recovery Point Objective) of 5 minutes.

## System Components

### 1. BackupManager (`src/disaster_recovery/backup_manager.py`)
- **Purpose**: Centralized backup management for all data stores
- **Features**:
  - PostgreSQL database backups using pg_dump
  - S3 data lake backups with cross-region replication
  - Kafka topic configuration and message backups
  - AES-256-GCM encryption for backup security
  - Automated compression and integrity verification
  - Role-based access control and audit logging

### 2. DisasterRecoveryCoordinator (`src/disaster_recovery/dr_coordinator.py`)
- **Purpose**: Health monitoring and automated failover orchestration
- **Features**:
  - Continuous health monitoring of all critical services
  - Automated failover with DNS, database, storage, and compute transitions
  - RTO/RPO metrics tracking and compliance reporting
  - Cross-region infrastructure coordination
  - Event logging and audit trails

### 3. BackupAutomation (`src/disaster_recovery/backup_automation.py`)
- **Purpose**: Scheduled backup execution and monitoring
- **Features**:
  - Configurable backup schedules (daily, weekly, monthly)
  - Prometheus metrics collection and alerting
  - Multi-channel notifications (email, Slack, webhook)
  - Retention policy enforcement
  - Performance monitoring and alerting

### 4. DisasterRecoveryRunbook (`src/disaster_recovery/runbooks/dr_runbook.py`)
- **Purpose**: Step-by-step recovery procedures
- **Features**:
  - 7 comprehensive failure scenario runbooks
  - Automated command execution with verification
  - Dependency tracking and rollback procedures
  - Human-readable documentation generation

## Security Features

### Encryption
- **Algorithm**: AES-256-GCM for backup encryption
- **Key Management**: Environment variables or secure key files
- **Auto-Generation**: Automatic key generation when configured
- **Verification**: Integrity checks with checksum validation

### Access Control
- **Role-Based**: User and role-based permissions
- **Operations**: Fine-grained permissions (create, restore, verify, report)
- **Audit Trail**: Comprehensive logging of all security events
- **Session Tracking**: User session and source IP logging

### Compliance Reporting
- **Retention Compliance**: Automated retention policy verification
- **Security Compliance**: Encryption and access control status
- **Schedule Compliance**: Backup schedule adherence monitoring
- **Audit Reports**: Detailed compliance reports for governance

## Configuration

### Basic Configuration Example
```yaml
backup_manager:
  backup_dir: "/opt/ecap/backups"
  retention_policies:
    daily: 7
    weekly: 4
    monthly: 12

aws:
  region: "us-west-2"
  backup_region: "us-east-1"

postgresql:
  host: "localhost"
  port: 5432
  database: "ecommerce_analytics"
  username: "backup_user"
  password: "${DB_PASSWORD}"

encryption:
  enabled: true
  auto_generate_key: true
  key_file: "/opt/ecap/keys/backup.key"

access_control:
  enabled: true
  users:
    backup_admin: ["backup.*"]
    backup_operator: ["backup.create", "backup.list"]
  roles:
    admin: ["*"]
    operator: ["backup.create", "backup.list", "backup.verify"]

audit:
  log_file: "/var/log/ecap/backup-audit.log"

alerting:
  rules:
    max_backup_duration_minutes: 60
    max_backup_size_gb: 100
    max_backup_age_hours: 25
  notifications:
    email:
      enabled: true
      smtp_server: "smtp.company.com"
      from_address: "ecap-alerts@company.com"
      to_addresses: ["ops-team@company.com"]
    slack:
      enabled: true
      webhook_url: "${SLACK_WEBHOOK_URL}"
```

## Operations

### Manual Backup Operations

#### Create Full Backup
```bash
# Using the Python API
python -c "
import asyncio
from src.disaster_recovery.backup_manager import BackupManager

config = {...}  # Your configuration
manager = BackupManager(config)
result = asyncio.run(manager.create_full_backup('manual_backup_20241127'))
print(f'Backup Status: {result[\"status\"]}')
"
```

#### Restore from Backup
```bash
# Restore all components
python -c "
import asyncio
from src.disaster_recovery.backup_manager import BackupManager

config = {...}  # Your configuration
manager = BackupManager(config)
result = asyncio.run(manager.restore_backup('backup_id'))
print(f'Restore Status: {result[\"status\"]}')
"
```

#### Verify Backup Integrity
```bash
python -c "
import asyncio
from src.disaster_recovery.backup_manager import BackupManager

config = {...}  # Your configuration
manager = BackupManager(config)
result = asyncio.run(manager.verify_backup_integrity('backup_id'))
print(f'Verification: {result[\"verification_results\"][\"overall_status\"]}')
"
```

### Disaster Recovery Procedures

#### Manual Failover
```bash
python -c "
import asyncio
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator

config = {...}  # Your DR configuration
coordinator = DisasterRecoveryCoordinator(config)
result = asyncio.run(coordinator.manual_failover('Planned maintenance'))
print(f'Failover Status: {result[\"success\"]}')
"
```

#### Execute Recovery Runbook
```bash
python -c "
from src.disaster_recovery.runbooks.dr_runbook import DisasterRecoveryRunbook, FailureType

runbook = DisasterRecoveryRunbook()
result = runbook.execute_runbook(
    FailureType.DATABASE_FAILURE,
    auto_execute=False,  # Manual confirmation required
    skip_confirmations=False
)
print(f'Runbook Status: {result[\"execution_log\"][\"overall_status\"]}')
"
```

### Automated Operations

#### Backup Automation Service
```bash
# Start the backup automation service
python -c "
import asyncio
from src.disaster_recovery.backup_automation import BackupAutomation

config = {...}  # Your automation configuration
automation = BackupAutomation(config)
asyncio.run(automation.start())
"
```

#### Health Monitoring
```bash
# Start continuous health monitoring
python -c "
import asyncio
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator

config = {...}  # Your DR configuration
coordinator = DisasterRecoveryCoordinator(config)
asyncio.run(coordinator.start_monitoring())
"
```

## Monitoring and Alerting

### Prometheus Metrics
- `backups_total`: Total number of backups by type and status
- `backup_duration_seconds`: Backup execution time
- `backup_size_bytes`: Backup size metrics
- `backup_age_hours`: Time since last successful backup

### Health Check Endpoints
- Service health status monitoring
- Cross-region connectivity verification
- Database replication lag monitoring
- Storage accessibility checks

### Alert Conditions
- Backup failures or extended duration
- Service health degradation
- RTO/RPO threshold violations
- Storage space exhaustion
- Security access violations

## Runbook Scenarios

### 1. Database Failure
- **Estimated Time**: 68 minutes
- **Steps**: Assess failure → Stop services → Activate replica → Update DNS → Restart services → Verify integrity → Setup new replica → Update monitoring → Document recovery

### 2. Storage Failure
- **Estimated Time**: 70 minutes
- **Steps**: Assess storage → Switch to backup → Verify data → Resume processing → Setup replication

### 3. Compute Failure
- **Estimated Time**: 45 minutes
- **Steps**: Assess infrastructure → Scale services → Add capacity → Redistribute workloads → Verify health

### 4. Network Failure
- **Estimated Time**: 40 minutes
- **Steps**: Diagnose network → Check load balancer → Activate backup path → Verify connectivity

### 5. Complete Region Failure
- **Estimated Time**: 100 minutes
- **Steps**: Activate DR site → Promote backup database → Start backup cluster → Update global DNS → Verify functionality → Establish new replication

### 6. Data Corruption
- **Estimated Time**: 100 minutes
- **Steps**: Assess corruption → Stop writes → Identify clean backup → Restore from backup → Verify integrity → Resume operations

### 7. Security Breach
- **Estimated Time**: 150 minutes
- **Steps**: Immediate isolation → Change credentials → Collect forensics → Clean restoration → Enhanced security → Gradual restoration

## Testing and Validation

### Recovery Testing Schedule
- **Monthly**: Database failover testing
- **Quarterly**: Complete region failover simulation
- **Bi-annually**: Full disaster recovery drill
- **Weekly**: Backup integrity verification

### RTO/RPO Validation
- **Target RTO**: 15 minutes
- **Target RPO**: 5 minutes
- **Measurement**: Automated tracking during failover events
- **Reporting**: Monthly compliance reports

## Troubleshooting

### Common Issues

#### Backup Failures
```bash
# Check audit logs
tail -f /var/log/ecap/backup-audit.log

# Verify permissions
python -c "
import asyncio
from src.disaster_recovery.backup_manager import BackupManager
manager = BackupManager(config)
result = asyncio.run(manager._check_access_permissions('create'))
print(f'Permissions OK: {result}')
"

# Check disk space
df -h /opt/ecap/backups
```

#### Encryption Issues
```bash
# Verify encryption key
echo $ECAP_BACKUP_ENCRYPTION_KEY | base64 -d | wc -c  # Should be 32 bytes

# Test encryption functionality
python -c "
from src.disaster_recovery.backup_manager import BackupManager
manager = BackupManager(config)
key = manager._get_encryption_key()
print(f'Encryption key available: {key is not None}')
"
```

#### Failover Issues
```bash
# Check service health
python -c "
import asyncio
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator
coordinator = DisasterRecoveryCoordinator(config)
status = asyncio.run(coordinator.get_health_status())
print(f'Overall Status: {status[\"overall_status\"]}')
"

# Review event history
python -c "
import asyncio
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator
coordinator = DisasterRecoveryCoordinator(config)
events = asyncio.run(coordinator.get_event_history(limit=10))
for event in events:
    print(f'{event[\"timestamp\"]}: {event[\"event_type\"]} - {event[\"data\"]}')
"
```

## Security Best Practices

1. **Key Management**:
   - Store encryption keys securely outside the backup location
   - Rotate encryption keys quarterly
   - Use environment variables for key distribution

2. **Access Control**:
   - Implement least-privilege access principles
   - Regular access review and cleanup
   - Enable comprehensive audit logging

3. **Network Security**:
   - Use VPC peering for cross-region communication
   - Implement network segmentation for backup systems
   - Regular security scanning and updates

4. **Compliance**:
   - Monthly compliance reporting
   - Regular security audits
   - Document all configuration changes

## Emergency Contacts

- **Primary On-Call**: [Contact Information]
- **Backup Coordinator**: [Contact Information]
- **Database Administrator**: [Contact Information]
- **Security Team**: [Contact Information]
- **Management Escalation**: [Contact Information]

## Related Documentation

- [System Architecture Documentation](../README.md)
- [Database Administration Guide](./database_guide.md)
- [Security Policies](./security_policies.md)
- [Monitoring and Alerting Guide](./monitoring_guide.md)

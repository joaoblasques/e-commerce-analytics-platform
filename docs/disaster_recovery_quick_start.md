# Disaster Recovery Quick Start Guide

## Prerequisites

1. **Dependencies Installation**:
   ```bash
   poetry install
   poetry add schedule prometheus_client aiohttp cryptography
   ```

2. **Environment Setup**:
   ```bash
   # Set encryption key
   export ECAP_BACKUP_ENCRYPTION_KEY=$(openssl rand -base64 32)

   # Set AWS credentials
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-west-2
   ```

3. **Configuration File** (`config/disaster_recovery.yaml`):
   ```yaml
   backup_manager:
     backup_dir: "/opt/ecap/backups"

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
     auto_generate_key: true

   access_control:
     enabled: false  # Enable in production

   audit:
     log_file: "/var/log/ecap/backup-audit.log"
   ```

## Quick Operations

### 1. Create Manual Backup
```bash
python3 << 'EOF'
import asyncio
import yaml
from src.disaster_recovery.backup_manager import BackupManager

# Load configuration
with open('config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)

# Create backup
manager = BackupManager(config['backup_manager'])
result = asyncio.run(manager.create_full_backup())

print(f"Backup Status: {result['status']}")
print(f"Backup ID: {result['backup_id']}")
print(f"Size: {result.get('total_size_bytes', 0) / 1024 / 1024:.1f} MB")
EOF
```

### 2. List Available Backups
```bash
python3 << 'EOF'
import asyncio
import yaml
from src.disaster_recovery.backup_manager import BackupManager

with open('config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)

manager = BackupManager(config['backup_manager'])
backups = asyncio.run(manager.list_backups())

print(f"Found {len(backups)} backups:")
for backup in backups[:5]:  # Show last 5
    print(f"  {backup['backup_id']}: {backup['status']} ({backup['start_time']})")
EOF
```

### 3. Start Health Monitoring
```bash
python3 << 'EOF'
import asyncio
import yaml
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator

with open('config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)

# Configure monitoring services
config['monitored_services'] = {
    'api_service': {
        'type': 'http',
        'health_url': 'http://localhost:8000/health',
        'expected_status': 200
    }
}

coordinator = DisasterRecoveryCoordinator(config)
print("Starting health monitoring (Press Ctrl+C to stop)...")

try:
    asyncio.run(coordinator.start_monitoring())
except KeyboardInterrupt:
    print("Monitoring stopped.")
EOF
```

### 4. Generate Compliance Report
```bash
python3 << 'EOF'
import asyncio
import yaml
import json
from src.disaster_recovery.backup_manager import BackupManager

with open('config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)

manager = BackupManager(config['backup_manager'])
result = asyncio.run(manager.generate_compliance_report())

if result['success']:
    report = result['report']
    print("=== COMPLIANCE REPORT ===")
    print(f"Report Date: {report['report_date']}")
    print(f"Total Backups: {report['summary']['total_backups']}")
    print(f"Encryption Rate: {report['summary']['encryption_rate']}")
    print(f"Schedule Compliant: {report['backup_schedule_compliance']['schedule_compliant']}")
else:
    print(f"Report generation failed: {result['error']}")
EOF
```

### 5. Execute Disaster Recovery Runbook (Dry Run)
```bash
python3 << 'EOF'
from src.disaster_recovery.runbooks.dr_runbook import DisasterRecoveryRunbook, FailureType

runbook = DisasterRecoveryRunbook()

# Get runbook for database failure
db_runbook = runbook.get_runbook(FailureType.DATABASE_FAILURE)
print(f"Database Failure Runbook: {len(db_runbook)} steps")

# Generate documentation
doc = runbook.generate_runbook_documentation(FailureType.DATABASE_FAILURE)
print("\n=== RUNBOOK PREVIEW ===")
print(doc[:500] + "..." if len(doc) > 500 else doc)

# Get summary of all runbooks
summary = runbook.get_all_runbooks_summary()
print("\n=== ALL RUNBOOKS ===")
for failure_type, info in summary.items():
    print(f"{failure_type}: {info['total_steps']} steps, {info['estimated_time_minutes']} min")
EOF
```

## Production Deployment

### 1. Systemd Service for Backup Automation
Create `/etc/systemd/system/ecap-backup.service`:
```ini
[Unit]
Description=ECAP Backup Automation Service
After=network.target

[Service]
Type=simple
User=ecap
WorkingDirectory=/opt/ecap
Environment=PYTHONPATH=/opt/ecap
ExecStart=/opt/ecap/venv/bin/python -c "
import asyncio
import yaml
from src.disaster_recovery.backup_automation import BackupAutomation

with open('/opt/ecap/config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)

automation = BackupAutomation(config)
asyncio.run(automation.start())
"
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 2. Systemd Service for Health Monitoring
Create `/etc/systemd/system/ecap-dr-monitor.service`:
```ini
[Unit]
Description=ECAP Disaster Recovery Monitor
After=network.target

[Service]
Type=simple
User=ecap
WorkingDirectory=/opt/ecap
Environment=PYTHONPATH=/opt/ecap
ExecStart=/opt/ecap/venv/bin/python -c "
import asyncio
import yaml
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator

with open('/opt/ecap/config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)

coordinator = DisasterRecoveryCoordinator(config)
asyncio.run(coordinator.start_monitoring())
"
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 3. Enable and Start Services
```bash
sudo systemctl daemon-reload
sudo systemctl enable ecap-backup ecap-dr-monitor
sudo systemctl start ecap-backup ecap-dr-monitor
sudo systemctl status ecap-backup ecap-dr-monitor
```

## Monitoring Integration

### Prometheus Configuration
Add to `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'ecap-backup'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s
```

### Grafana Dashboard
Import dashboard with these key metrics:
- `backups_total` - Total backup count by status
- `backup_duration_seconds` - Backup execution time
- `backup_age_hours` - Time since last successful backup
- `backup_size_bytes` - Backup size trends

## Security Checklist

- [ ] Encryption keys stored securely
- [ ] Access control enabled in production
- [ ] Audit logging configured
- [ ] Network security groups configured
- [ ] Cross-region connectivity tested
- [ ] Backup verification scheduled
- [ ] Monitoring alerts configured
- [ ] Emergency contacts updated

## Troubleshooting

### Check Service Status
```bash
sudo systemctl status ecap-backup ecap-dr-monitor
sudo journalctl -u ecap-backup -f
```

### Verify Configuration
```bash
python3 -c "
import yaml
with open('config/disaster_recovery.yaml') as f:
    config = yaml.safe_load(f)
print('Configuration loaded successfully')
print(f'Backup directory: {config[\"backup_manager\"][\"backup_dir\"]}')
"
```

### Test Connectivity
```bash
# Test database connection
python3 -c "
import psycopg2
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='ecommerce_analytics',
    user='backup_user',
    password='your_password'
)
print('Database connection successful')
conn.close()
"

# Test AWS connectivity
aws s3 ls
```

## Emergency Recovery

### Immediate Actions
1. **Assess Situation**: Determine failure scope and impact
2. **Notify Team**: Alert on-call engineers and management
3. **Execute Runbook**: Follow appropriate failure scenario runbook
4. **Document Actions**: Log all recovery actions taken
5. **Validate Recovery**: Confirm system functionality
6. **Post-Mortem**: Schedule incident review within 24 hours

### Recovery Commands
```bash
# Quick health check
python3 -c "
import asyncio
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator
coordinator = DisasterRecoveryCoordinator(config)
status = asyncio.run(coordinator.get_health_status())
print(f'System Status: {status[\"overall_status\"]}')
"

# Manual failover (if needed)
python3 -c "
import asyncio
from src.disaster_recovery.dr_coordinator import DisasterRecoveryCoordinator
coordinator = DisasterRecoveryCoordinator(config)
result = asyncio.run(coordinator.manual_failover('Emergency recovery'))
print(f'Failover initiated: {result[\"success\"]}')
"
```

For detailed procedures, refer to the [Disaster Recovery Operational Guide](./disaster_recovery_guide.md).

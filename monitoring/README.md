# ECAP Application Performance Monitoring (APM)

## Overview

The ECAP Application Performance Monitoring system provides comprehensive observability for the E-Commerce Analytics Platform using industry-standard open-source tools. This implementation offers enterprise-grade monitoring capabilities without the costs associated with commercial APM solutions.

## Architecture

### Core Components

- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **Jaeger**: Distributed tracing
- **AlertManager**: Alert routing and management
- **Various Exporters**: Specialized metrics collection

### Monitoring Stack

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │   Exporters     │    │   Prometheus    │
│   (FastAPI)     │───▶│   (DB, Redis,   │───▶│   (Metrics)     │
│                 │    │   Kafka, etc.)  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────┐                            ┌─────────────────┐
│     Jaeger      │                            │    Grafana      │
│   (Tracing)     │                            │  (Dashboards)   │
└─────────────────┘                            └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │  AlertManager   │
                                               │   (Alerting)    │
                                               └─────────────────┘
```

## Features

### ✅ Application Performance Monitoring (APM)
- **Request Rate Monitoring**: Track API request throughput and patterns
- **Response Time Analysis**: Monitor latency percentiles (50th, 95th, 99th)
- **Error Rate Tracking**: Real-time error monitoring with alerting
- **Resource Usage**: CPU, memory, and disk utilization monitoring

### ✅ Custom Metrics Collection
- **Business Metrics**: User registrations, transactions, fraud alerts
- **Cache Performance**: Hit/miss ratios for Redis caching
- **Message Queue Metrics**: Kafka message production/consumption rates
- **Analytics Job Monitoring**: Track Spark job execution and performance

### ✅ Distributed Tracing
- **End-to-End Request Tracking**: Follow requests across all services
- **Database Query Tracing**: Monitor SQL query performance
- **Cache Operation Tracing**: Track Redis operations
- **Message Queue Tracing**: Kafka producer/consumer tracing

### ✅ SLA Monitoring and Reporting
- **Availability SLA**: 99.9% uptime target monitoring
- **Response Time SLA**: <1 second response time tracking
- **Error Budget Tracking**: Monitor remaining error budget
- **SLA Compliance Reporting**: Automated SLA violation detection

### ✅ Intelligent Alerting
- **Multi-Channel Alerts**: Email, Slack, webhook notifications
- **Alert Grouping**: Intelligent alert clustering and deduplication
- **Escalation Policies**: Automated alert escalation based on severity
- **Alert Inhibition**: Prevent alert storms with intelligent suppression

## Quick Start

### 1. Start Monitoring Stack

```bash
# Using the setup script (recommended)
python monitoring/setup_monitoring.py

# Or manually with Docker Compose
docker-compose -f monitoring/docker-compose.monitoring.yml up -d
```

### 2. Access Monitoring Interfaces

- **Grafana Dashboards**: http://localhost:3001 (admin/admin123)
- **Prometheus Metrics**: http://localhost:9090
- **Jaeger Tracing**: http://localhost:16686
- **AlertManager**: http://localhost:9093

### 3. Generate Test Data

```bash
# Start the monitoring demo
python examples/monitoring_integration_demo.py

# Generate load for testing dashboards
curl "http://localhost:8001/demo/load-test?requests=100"
```

## Configuration

### Environment Variables

```bash
# Jaeger Configuration
JAEGER_AGENT_HOST=localhost
JAEGER_AGENT_PORT=6831
JAEGER_ENDPOINT=http://localhost:14268/api/traces

# Monitoring Configuration
MONITORING_ENVIRONMENT=development
PROMETHEUS_RETENTION=30d
GRAFANA_ADMIN_PASSWORD=admin123
```

### Prometheus Configuration

The Prometheus configuration (`monitoring/prometheus/prometheus.yml`) defines:

- **Scrape Targets**: All monitored services and exporters
- **Scrape Intervals**: Optimized for each service type
- **Retention Policies**: 30-day metric retention
- **Alert Rules**: Comprehensive alerting configuration

### Grafana Dashboards

Pre-configured dashboards include:

1. **ECAP - Application Performance Overview**
   - API request rates and response times
   - Error rates and availability metrics
   - Resource utilization trends
   - Custom business metrics

2. **ECAP - SLA Monitoring Dashboard**
   - SLA compliance tracking
   - Error budget monitoring
   - Availability trends
   - Response time percentiles

### Alert Rules

Configured alerts include:

- **Critical Alerts**: API service down, high error rates
- **Warning Alerts**: High response times, resource usage
- **SLA Alerts**: Availability and response time SLA breaches
- **Business Alerts**: Fraud detection, unusual patterns

## Integration Guide

### Adding Custom Metrics

```python
from src.api.middleware.metrics_middleware import (
    record_user_registration,
    record_transaction,
    record_fraud_alert
)

# Record business events
record_user_registration()
record_transaction("completed", "credit_card")
record_fraud_alert("high_amount", "critical")
```

### Adding Distributed Tracing

```python
from src.monitoring import start_span, add_span_attributes

# Manual span creation
with start_span("custom_operation") as span:
    add_span_attributes(span, {"user_id": "12345"})
    # Your operation code here
```

### FastAPI Integration

```python
from src.api.middleware.metrics_middleware import PrometheusMetricsMiddleware
from src.monitoring import initialize_tracing, instrument_all

app = FastAPI()

# Add metrics middleware
app.add_middleware(PrometheusMetricsMiddleware)

# Initialize tracing
initialize_tracing(service_name="my-service")
instrument_all(app)
```

## Metrics Reference

### HTTP Metrics

- `http_requests_total`: Total HTTP requests by method, endpoint, status
- `http_request_duration_seconds`: Request duration histogram
- `http_requests_in_progress`: Current in-flight requests

### Business Metrics

- `user_registrations_total`: Total user registrations
- `transactions_total`: Total transactions by status and payment method
- `fraud_alerts_total`: Total fraud alerts by type and severity

### Infrastructure Metrics

- `database_connections_active`: Active database connections
- `cache_hits_total/cache_misses_total`: Cache performance
- `kafka_messages_produced_total`: Kafka message production

### Analytics Metrics

- `analytics_jobs_total`: Analytics job executions by type and status
- `analytics_job_duration_seconds`: Job execution time histogram

## Alert Configuration

### Alert Severity Levels

- **Critical**: Service down, SLA breaches, high error rates
- **Warning**: Performance degradation, resource usage
- **Info**: Maintenance notifications, configuration changes

### Notification Channels

1. **Email**: Critical alerts and SLA breaches
2. **Slack**: All alert types with channel-specific routing
3. **Webhook**: Integration with external systems
4. **PagerDuty**: On-call escalation (configurable)

### Alert Rules Examples

```yaml
# High API Response Time
- alert: HighAPIResponseTime
  expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2
  for: 2m
  labels:
    severity: warning

# SLA Availability Breach
- alert: APIAvailabilitySLABreach
  expr: (rate(http_requests_total{status!~"5.."}[5m]) / rate(http_requests_total[5m])) * 100 < 99.9
  for: 1m
  labels:
    severity: critical
```

## Troubleshooting

### Common Issues

1. **Metrics Not Appearing**
   - Check Prometheus targets: http://localhost:9090/targets
   - Verify application `/metrics` endpoint is accessible
   - Check firewall and network connectivity

2. **Tracing Not Working**
   - Verify Jaeger agent connectivity
   - Check JAEGER_ENDPOINT configuration
   - Ensure OpenTelemetry instrumentation is enabled

3. **Alerts Not Firing**
   - Check AlertManager configuration
   - Verify Prometheus alert rules syntax
   - Test notification channels

### Debugging Commands

```bash
# Check service health
docker-compose -f monitoring/docker-compose.monitoring.yml ps

# View service logs
docker-compose -f monitoring/docker-compose.monitoring.yml logs prometheus
docker-compose -f monitoring/docker-compose.monitoring.yml logs grafana

# Test metrics endpoint
curl http://localhost:8000/metrics

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets
```

## Performance Considerations

### Resource Requirements

- **Prometheus**: 2GB RAM, 100GB storage (for 30-day retention)
- **Grafana**: 512MB RAM, 1GB storage
- **Jaeger**: 1GB RAM, 50GB storage
- **Total**: ~4GB RAM, 150GB storage

### Optimization Tips

1. **Metric Cardinality**: Avoid high-cardinality labels
2. **Retention Policies**: Adjust based on storage capacity
3. **Scrape Intervals**: Balance between detail and performance
4. **Dashboard Queries**: Use efficient PromQL queries

## Security

### Access Control

- **Grafana**: User authentication and role-based access
- **Prometheus**: Network-level access control
- **AlertManager**: Secure notification channels

### Data Protection

- **Encryption**: TLS for all inter-service communication
- **Secrets**: Secure storage of credentials and API keys
- **Audit Logs**: Complete audit trail for access and changes

## Maintenance

### Regular Tasks

1. **Weekly**: Review alert rules and thresholds
2. **Monthly**: Analyze storage usage and retention
3. **Quarterly**: Update dashboards and metrics
4. **Annually**: Review and update monitoring strategy

### Backup and Recovery

```bash
# Backup Grafana dashboards
curl -H "Authorization: Bearer <token>" \
  http://localhost:3001/api/search?type=dash-db | \
  jq '.[] | .uri' | \
  xargs -I {} curl -H "Authorization: Bearer <token>" \
  "http://localhost:3001/api/dashboards/{}" > dashboards_backup.json

# Backup Prometheus data
docker run --rm -v prometheus_data:/data -v $(pwd):/backup \
  alpine tar czf /backup/prometheus_backup.tar.gz /data
```

## Support

For issues and questions:

1. Check the troubleshooting section above
2. Review service logs for error messages
3. Consult the Prometheus, Grafana, and Jaeger documentation
4. Create an issue in the project repository

## Contributing

To contribute to the monitoring system:

1. Follow the existing patterns for metrics and alerts
2. Test all changes with the monitoring demo
3. Update dashboards and documentation
4. Ensure backward compatibility with existing metrics

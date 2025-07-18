# Monitoring and Observability Setup

This document describes the comprehensive monitoring and observability stack implemented for the E-Commerce Analytics Platform (ECAP).

## Overview

The monitoring stack provides complete observability for all platform components including:
- **Metrics Collection**: Prometheus scrapes metrics from all services
- **Visualization**: Grafana dashboards for system and business metrics
- **Alerting**: Alertmanager handles alert routing and notifications
- **Service Metrics**: Dedicated exporters for each service type

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Applications  │    │   Infrastructure│    │   Monitoring    │
│                 │    │                 │    │                 │
│ • Spark Jobs    │    │ • Kafka Cluster │    │ • Prometheus    │
│ • API Services  │    │ • PostgreSQL    │    │ • Grafana       │
│ • Streaming     │    │ • Redis Cache   │    │ • Alertmanager  │
│                 │    │ • MinIO Storage │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                        │                        │
        └────────────────────────┼────────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Exporters     │
                    │                 │
                    │ • Node Exporter │
                    │ • Postgres Exp. │
                    │ • Redis Exp.    │
                    │ • Kafka JMX     │
                    └─────────────────┘
```

## Components

### 1. Prometheus (Port: 9090)
- **Purpose**: Metrics collection and storage
- **Configuration**: `config/prometheus/prometheus.yml`
- **Data Retention**: 200 hours
- **Targets**: All services and exporters

### 2. Grafana (Port: 3000)
- **Purpose**: Metrics visualization and dashboards
- **Default Login**: admin/admin
- **Dashboards**: Pre-configured dashboards for system overview, Kafka, and Spark
- **Configuration**: `config/grafana/provisioning/`

### 3. Alertmanager (Port: 9093)
- **Purpose**: Alert routing and notifications
- **Configuration**: `config/alertmanager/alertmanager.yml`
- **Features**: Email, Slack, and webhook notifications

### 4. Node Exporter (Port: 9100)
- **Purpose**: System-level metrics (CPU, memory, disk, network)
- **Metrics**: Hardware and OS metrics from the host system

### 5. Postgres Exporter (Port: 9187)
- **Purpose**: PostgreSQL database metrics
- **Metrics**: Connection counts, query performance, database size

### 6. Redis Exporter (Port: 9121)
- **Purpose**: Redis cache metrics
- **Metrics**: Memory usage, key counts, operation rates

### 7. Kafka JMX Exporter (Port: 5556)
- **Purpose**: Kafka broker metrics
- **Configuration**: `config/kafka/jmx_prometheus_javaagent.yml`
- **Metrics**: Topic throughput, consumer lag, broker performance

## Usage

### Starting the Monitoring Stack

1. **Start all services**:
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready**:
   ```bash
   ./scripts/test-monitoring.py
   ```

3. **Access monitoring interfaces**:
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (admin/admin)
   - Alertmanager: http://localhost:9093

### Grafana Dashboards

The following dashboards are automatically provisioned:

#### 1. System Overview Dashboard
- **URL**: http://localhost:3000/d/ecap-system-overview
- **Metrics**: Service status, CPU usage, memory usage, disk usage
- **Purpose**: High-level system health monitoring

#### 2. Kafka Monitoring Dashboard
- **URL**: http://localhost:3000/d/ecap-kafka-monitoring
- **Metrics**: Message rates, byte rates, partition counts, consumer lag
- **Purpose**: Kafka cluster performance monitoring

#### 3. Spark Monitoring Dashboard
- **URL**: http://localhost:3000/d/ecap-spark-monitoring
- **Metrics**: Executor counts, memory usage, job performance
- **Purpose**: Spark cluster performance monitoring

### Prometheus Targets

Prometheus automatically discovers and monitors these targets:

| Service | Endpoint | Metrics |
|---------|----------|---------|
| Prometheus | localhost:9090 | Internal Prometheus metrics |
| Kafka | kafka:9101 | JMX metrics via exporter |
| Spark Master | spark-master:8080 | Spark cluster metrics |
| Spark Workers | spark-worker-*:8081 | Worker node metrics |
| Spark History | spark-history:18080 | Historical job metrics |
| PostgreSQL | postgres-exporter:9187 | Database metrics |
| Redis | redis-exporter:9121 | Cache metrics |
| MinIO | minio:9000 | Object storage metrics |
| Node Exporter | node-exporter:9100 | System metrics |

### Alerting Rules

The following alerts are configured:

#### Critical Alerts
- **ServiceDown**: Any service becomes unavailable
- **PostgreSQLDown**: Database is unreachable
- **RedisDown**: Cache is unavailable

#### Warning Alerts
- **HighCPUUsage**: CPU usage > 80%
- **HighMemoryUsage**: Memory usage > 90%
- **DiskSpaceLow**: Disk space < 10%
- **KafkaOfflinePartitions**: Kafka has offline partitions
- **SparkHighMemoryUsage**: Spark worker memory > 90%

### Testing the Monitoring Stack

Use the provided test script to verify all monitoring services:

```bash
./scripts/test-monitoring.py
```

The script will:
- Test connectivity to all monitoring services
- Verify Prometheus targets are healthy
- Check Grafana datasources configuration
- Provide a comprehensive health report

## Configuration

### Adding New Metrics

1. **Add Prometheus scrape target** in `config/prometheus/prometheus.yml`:
   ```yaml
   - job_name: 'new-service'
     static_configs:
       - targets: ['new-service:port']
   ```

2. **Create Grafana dashboard** in `config/grafana/provisioning/dashboards/json/`

3. **Add alerting rules** in `config/prometheus/rules/alerts.yml`

### Customizing Alerts

Edit `config/alertmanager/alertmanager.yml` to configure:
- Email notifications
- Slack webhooks
- Alert grouping and routing
- Notification templates

### Dashboard Customization

Dashboards can be customized by:
1. Editing JSON files in `config/grafana/provisioning/dashboards/json/`
2. Using Grafana UI (changes will be lost on restart)
3. Exporting dashboards from Grafana UI and saving as JSON

## Troubleshooting

### Common Issues

1. **Prometheus targets down**:
   - Check if target services are running
   - Verify network connectivity
   - Check service health endpoints

2. **Grafana dashboard not loading**:
   - Verify Prometheus datasource configuration
   - Check Prometheus target health
   - Verify dashboard JSON syntax

3. **Alerts not firing**:
   - Check Prometheus alert rules syntax
   - Verify Alertmanager configuration
   - Check alert routing rules

### Monitoring Logs

View logs for monitoring services:
```bash
# Prometheus logs
docker logs ecap-prometheus

# Grafana logs
docker logs ecap-grafana

# Alertmanager logs
docker logs ecap-alertmanager
```

### Health Checks

All monitoring services include health checks:
- Prometheus: `http://localhost:9090/-/healthy`
- Grafana: `http://localhost:3000/api/health`
- Alertmanager: `http://localhost:9093/-/healthy`

## Best Practices

1. **Regular Monitoring**: Check monitoring dashboards regularly
2. **Alert Tuning**: Adjust alert thresholds based on normal system behavior
3. **Dashboard Maintenance**: Keep dashboards relevant and up-to-date
4. **Metric Retention**: Monitor storage usage and adjust retention policies
5. **Performance**: Monitor monitoring stack performance and resource usage

## Next Steps

1. **Custom Metrics**: Add application-specific metrics
2. **Distributed Tracing**: Implement Jaeger or Zipkin for request tracing
3. **Log Aggregation**: Add ELK stack for centralized logging
4. **Advanced Dashboards**: Create business-specific dashboards
5. **Automated Alerting**: Implement automatic remediation actions
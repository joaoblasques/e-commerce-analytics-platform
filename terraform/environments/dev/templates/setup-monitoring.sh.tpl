#!/bin/bash
# Setup Monitoring Integration for EMR Cluster
# Template for ${project_name} ${environment} environment

set -e

ENVIRONMENT="${environment}"
PROJECT_NAME="${project_name}"
PROMETHEUS_URL="${prometheus_url}"
GRAFANA_URL="${grafana_url}"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a /tmp/monitoring-setup.log
}

log "Setting up monitoring integration for $PROJECT_NAME-$ENVIRONMENT EMR cluster"

# Install monitoring tools
log "Installing monitoring packages..."
sudo python3 -m pip install \
    prometheus-client==0.16.0 \
    py4j==0.10.9.7 \
    psutil==5.9.5

# Create monitoring directory
sudo mkdir -p /opt/ecap/monitoring/{scripts,config,logs}
sudo chown -R hadoop:hadoop /opt/ecap/monitoring

# Create Prometheus metrics exporter for Spark
log "Creating Spark metrics exporter..."
cat <<'EOF' | sudo tee /opt/ecap/monitoring/scripts/spark_metrics_exporter.py
#!/usr/bin/env python3
"""
Spark Metrics Exporter for Prometheus
Exports Spark application metrics to Prometheus format
"""

import time
import logging
import psutil
from prometheus_client import start_http_server, Gauge, Counter, Histogram, Info
from pyspark.sql import SparkSession
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/ecap/monitoring/logs/spark_exporter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
SPARK_EXECUTORS = Gauge('spark_executors_total', 'Total number of Spark executors')
SPARK_EXECUTOR_CORES = Gauge('spark_executor_cores_total', 'Total executor cores')
SPARK_EXECUTOR_MEMORY = Gauge('spark_executor_memory_bytes', 'Total executor memory in bytes')
SPARK_ACTIVE_JOBS = Gauge('spark_active_jobs', 'Number of active Spark jobs')
SPARK_ACTIVE_STAGES = Gauge('spark_active_stages', 'Number of active Spark stages')
SPARK_COMPLETED_JOBS = Counter('spark_completed_jobs_total', 'Total completed Spark jobs')
SPARK_FAILED_JOBS = Counter('spark_failed_jobs_total', 'Total failed Spark jobs')

# System metrics
CPU_USAGE = Gauge('system_cpu_usage_percent', 'System CPU usage percentage')
MEMORY_USAGE = Gauge('system_memory_usage_percent', 'System memory usage percentage')
DISK_USAGE = Gauge('system_disk_usage_percent', 'System disk usage percentage')

# Application info
SPARK_APP_INFO = Info('spark_application_info', 'Spark application information')

class SparkMetricsCollector:
    def __init__(self):
        self.spark = None
        self.app_id = None

    def initialize_spark(self):
        """Initialize Spark session for metrics collection"""
        try:
            self.spark = SparkSession.builder \
                .appName(f"MetricsCollector-{os.environ.get('PROJECT_NAME', 'ECAP')}") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()

            self.app_id = self.spark.sparkContext.applicationId
            logger.info(f"Spark session initialized with application ID: {self.app_id}")

            # Set application info
            SPARK_APP_INFO.info({
                'application_id': self.app_id,
                'application_name': self.spark.sparkContext.appName,
                'spark_version': self.spark.version,
                'environment': os.environ.get('ENVIRONMENT', 'unknown'),
                'project': os.environ.get('PROJECT_NAME', 'unknown')
            })

            return True
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            return False

    def collect_spark_metrics(self):
        """Collect Spark application metrics"""
        if not self.spark:
            return

        try:
            # Get Spark context status
            sc = self.spark.sparkContext
            status = sc.statusTracker()

            # Executor information
            executor_infos = status.getExecutorInfos()
            total_executors = len(executor_infos)
            total_cores = sum(info.totalCores for info in executor_infos)
            total_memory = sum(info.maxMemory for info in executor_infos)

            SPARK_EXECUTORS.set(total_executors)
            SPARK_EXECUTOR_CORES.set(total_cores)
            SPARK_EXECUTOR_MEMORY.set(total_memory)

            # Job information
            active_jobs = len(status.getActiveJobIds())
            active_stages = len(status.getActiveStageIds())

            SPARK_ACTIVE_JOBS.set(active_jobs)
            SPARK_ACTIVE_STAGES.set(active_stages)

            logger.debug(f"Spark metrics collected: {total_executors} executors, {active_jobs} active jobs")

        except Exception as e:
            logger.error(f"Failed to collect Spark metrics: {e}")

    def collect_system_metrics(self):
        """Collect system metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            CPU_USAGE.set(cpu_percent)

            # Memory usage
            memory = psutil.virtual_memory()
            MEMORY_USAGE.set(memory.percent)

            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            DISK_USAGE.set(disk_percent)

            logger.debug(f"System metrics: CPU {cpu_percent}%, Memory {memory.percent}%, Disk {disk_percent}%")

        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")

    def run(self, port=8000, interval=30):
        """Run the metrics collector"""
        logger.info(f"Starting Spark metrics exporter on port {port}")

        # Start Prometheus HTTP server
        start_http_server(port)

        # Initialize Spark session
        if not self.initialize_spark():
            logger.error("Failed to initialize Spark session, exiting")
            return

        logger.info(f"Metrics collection started, interval: {interval}s")

        try:
            while True:
                self.collect_spark_metrics()
                self.collect_system_metrics()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Metrics collector stopped")
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    collector = SparkMetricsCollector()
    collector.run()
EOF

sudo chmod +x /opt/ecap/monitoring/scripts/spark_metrics_exporter.py

# Create system monitoring script
log "Creating system monitoring script..."
cat <<'EOF' | sudo tee /opt/ecap/monitoring/scripts/system_monitor.py
#!/usr/bin/env python3
"""
System Monitor for EMR Cluster
Monitors system resources and sends alerts
"""

import time
import psutil
import logging
import json
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/ecap/monitoring/logs/system_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SystemMonitor:
    def __init__(self, prometheus_url=None):
        self.prometheus_url = prometheus_url
        self.thresholds = {
            'cpu': 80,      # CPU usage percentage
            'memory': 85,   # Memory usage percentage
            'disk': 90,     # Disk usage percentage
            'load': 4.0     # Load average
        }

    def get_system_metrics(self):
        """Get current system metrics"""
        return {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'load_average': psutil.getloadavg()[0],  # 1-minute load average
            'network_io': psutil.net_io_counters()._asdict(),
            'disk_io': psutil.disk_io_counters()._asdict()
        }

    def check_thresholds(self, metrics):
        """Check if any metrics exceed thresholds"""
        alerts = []

        if metrics['cpu_percent'] > self.thresholds['cpu']:
            alerts.append({
                'metric': 'cpu',
                'value': metrics['cpu_percent'],
                'threshold': self.thresholds['cpu'],
                'severity': 'warning'
            })

        if metrics['memory_percent'] > self.thresholds['memory']:
            alerts.append({
                'metric': 'memory',
                'value': metrics['memory_percent'],
                'threshold': self.thresholds['memory'],
                'severity': 'critical'
            })

        if metrics['disk_percent'] > self.thresholds['disk']:
            alerts.append({
                'metric': 'disk',
                'value': metrics['disk_percent'],
                'threshold': self.thresholds['disk'],
                'severity': 'critical'
            })

        if metrics['load_average'] > self.thresholds['load']:
            alerts.append({
                'metric': 'load',
                'value': metrics['load_average'],
                'threshold': self.thresholds['load'],
                'severity': 'warning'
            })

        return alerts

    def send_alert(self, alert):
        """Send alert (placeholder for actual alerting mechanism)"""
        logger.warning(f"ALERT: {alert['metric']} at {alert['value']:.2f} exceeds threshold {alert['threshold']}")

    def log_metrics(self, metrics):
        """Log metrics to file"""
        metrics_file = '/opt/ecap/monitoring/logs/system_metrics.jsonl'
        with open(metrics_file, 'a') as f:
            f.write(json.dumps(metrics) + '\n')

    def run(self, interval=60):
        """Run the system monitor"""
        logger.info("Starting system monitor")

        try:
            while True:
                metrics = self.get_system_metrics()
                self.log_metrics(metrics)

                # Check for alerts
                alerts = self.check_thresholds(metrics)
                for alert in alerts:
                    self.send_alert(alert)

                logger.info(f"CPU: {metrics['cpu_percent']:.1f}%, "
                           f"Memory: {metrics['memory_percent']:.1f}%, "
                           f"Disk: {metrics['disk_percent']:.1f}%, "
                           f"Load: {metrics['load_average']:.2f}")

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("System monitor stopped")

if __name__ == "__main__":
    monitor = SystemMonitor(prometheus_url="${prometheus_url}")
    monitor.run()
EOF

sudo chmod +x /opt/ecap/monitoring/scripts/system_monitor.py

# Create Prometheus configuration for EMR
log "Creating Prometheus configuration..."
cat <<EOF | sudo tee /opt/ecap/monitoring/config/prometheus_emr.yml
# Prometheus configuration for EMR cluster monitoring
# Environment: $ENVIRONMENT
# Project: $PROJECT_NAME

global:
  scrape_interval: 30s
  evaluation_interval: 30s

rule_files:
  - "/opt/ecap/monitoring/config/alert_rules.yml"

scrape_configs:
  # Spark metrics exporter
  - job_name: 'spark-metrics'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 30s
    metrics_path: /metrics

  # Node exporter (if available)
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['localhost:9100']
    scrape_interval: 30s

  # EMR cluster metrics
  - job_name: 'emr-cluster'
    static_configs:
      - targets: ['localhost:8088']  # YARN ResourceManager
    metrics_path: /ws/v1/cluster/metrics
    scrape_interval: 60s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - "${prometheus_url}/alertmanager"
EOF

# Create alert rules
log "Creating alert rules..."
cat <<EOF | sudo tee /opt/ecap/monitoring/config/alert_rules.yml
groups:
  - name: emr_cluster_alerts
    rules:
      - alert: HighCPUUsage
        expr: system_cpu_usage_percent > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High CPU usage on EMR node"
          description: "CPU usage is above 80% for more than 5 minutes"

      - alert: HighMemoryUsage
        expr: system_memory_usage_percent > 85
        for: 3m
        labels:
          severity: critical
        annotations:
          summary: "High memory usage on EMR node"
          description: "Memory usage is above 85% for more than 3 minutes"

      - alert: SparkJobFailures
        expr: increase(spark_failed_jobs_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Spark job failures detected"
          description: "One or more Spark jobs have failed in the last 5 minutes"

      - alert: NoActiveExecutors
        expr: spark_executors_total == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "No active Spark executors"
          description: "No Spark executors are currently active"
EOF

# Create systemd service for Spark metrics exporter
log "Creating systemd service for metrics exporter..."
cat <<EOF | sudo tee /etc/systemd/system/spark-metrics-exporter.service
[Unit]
Description=Spark Metrics Exporter for Prometheus
After=network.target

[Service]
Type=simple
User=hadoop
Group=hadoop
Environment=PROJECT_NAME=$PROJECT_NAME
Environment=ENVIRONMENT=$ENVIRONMENT
Environment=PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-src.zip
ExecStart=/usr/bin/python3 /opt/ecap/monitoring/scripts/spark_metrics_exporter.py
Restart=always
RestartSec=30
StandardOutput=append:/opt/ecap/monitoring/logs/spark_exporter.log
StandardError=append:/opt/ecap/monitoring/logs/spark_exporter_error.log

[Install]
WantedBy=multi-user.target
EOF

# Create systemd service for system monitor
log "Creating systemd service for system monitor..."
cat <<EOF | sudo tee /etc/systemd/system/system-monitor.service
[Unit]
Description=System Monitor for EMR Cluster
After=network.target

[Service]
Type=simple
User=hadoop
Group=hadoop
Environment=PROJECT_NAME=$PROJECT_NAME
Environment=ENVIRONMENT=$ENVIRONMENT
ExecStart=/usr/bin/python3 /opt/ecap/monitoring/scripts/system_monitor.py
Restart=always
RestartSec=60
StandardOutput=append:/opt/ecap/monitoring/logs/system_monitor.log
StandardError=append:/opt/ecap/monitoring/logs/system_monitor_error.log

[Install]
WantedBy=multi-user.target
EOF

# Enable and start services
log "Enabling monitoring services..."
sudo systemctl daemon-reload
sudo systemctl enable spark-metrics-exporter.service
sudo systemctl enable system-monitor.service

# Create log rotation configuration
log "Setting up log rotation..."
cat <<EOF | sudo tee /etc/logrotate.d/ecap-monitoring
/opt/ecap/monitoring/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 hadoop hadoop
    postrotate
        systemctl reload spark-metrics-exporter || true
        systemctl reload system-monitor || true
    endscript
}
EOF

# Create monitoring startup script
log "Creating monitoring startup script..."
cat <<'EOF' | sudo tee /opt/ecap/monitoring/scripts/start_monitoring.sh
#!/bin/bash
# Start all monitoring services

echo "Starting EMR cluster monitoring services..."

# Start Spark metrics exporter
sudo systemctl start spark-metrics-exporter.service
echo "✓ Spark metrics exporter started"

# Start system monitor
sudo systemctl start system-monitor.service
echo "✓ System monitor started"

# Check service status
echo "Service status:"
sudo systemctl status spark-metrics-exporter.service --no-pager
sudo systemctl status system-monitor.service --no-pager

echo "Monitoring services started successfully"
echo "Metrics available at: http://$(hostname):8000/metrics"
EOF

sudo chmod +x /opt/ecap/monitoring/scripts/start_monitoring.sh

# Create monitoring status script
log "Creating monitoring status script..."
cat <<'EOF' | sudo tee /opt/ecap/monitoring/scripts/check_monitoring.sh
#!/bin/bash
# Check monitoring services status

echo "=== EMR Cluster Monitoring Status ==="
echo "Timestamp: $(date)"
echo

# Check service status
echo "Service Status:"
sudo systemctl is-active spark-metrics-exporter.service && echo "✓ Spark metrics exporter: Active" || echo "✗ Spark metrics exporter: Inactive"
sudo systemctl is-active system-monitor.service && echo "✓ System monitor: Active" || echo "✗ System monitor: Inactive"
echo

# Check if metrics endpoint is responding
echo "Metrics Endpoint Check:"
if curl -s http://localhost:8000/metrics > /dev/null; then
    echo "✓ Spark metrics endpoint: Responding"
else
    echo "✗ Spark metrics endpoint: Not responding"
fi
echo

# Show recent log entries
echo "Recent Log Entries:"
echo "--- Spark Metrics Exporter ---"
tail -n 5 /opt/ecap/monitoring/logs/spark_exporter.log 2>/dev/null || echo "No logs found"
echo
echo "--- System Monitor ---"
tail -n 5 /opt/ecap/monitoring/logs/system_monitor.log 2>/dev/null || echo "No logs found"
echo

echo "=== End Monitoring Status ==="
EOF

sudo chmod +x /opt/ecap/monitoring/scripts/check_monitoring.sh

# Start monitoring services
log "Starting monitoring services..."
/opt/ecap/monitoring/scripts/start_monitoring.sh

# Test the monitoring setup
log "Testing monitoring setup..."
sleep 10
/opt/ecap/monitoring/scripts/check_monitoring.sh

# Create completion marker
echo "$(date): Monitoring setup completed successfully" | sudo tee /opt/ecap/monitoring-complete

log "Monitoring setup completed successfully for $PROJECT_NAME-$ENVIRONMENT"
log "Metrics available at: http://$(hostname):8000/metrics"
log "Check status with: /opt/ecap/monitoring/scripts/check_monitoring.sh"

exit 0

#!/bin/bash
# EMR Bootstrap Script for ${project_name} ${environment} Environment
# This script configures the EMR cluster nodes for optimal Spark performance

set -e

# Variables
PROJECT_NAME="${project_name}"
ENVIRONMENT="${environment}"
LOG_BUCKET="${log_bucket}"
KMS_KEY_ID="${kms_key_id}"
PYTHON_VERSION="3.9"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a /tmp/bootstrap.log
}

log "Starting EMR bootstrap configuration for $PROJECT_NAME-$ENVIRONMENT"

# Update system packages
log "Updating system packages..."
sudo yum update -y

# Install additional Python packages
log "Installing Python packages..."
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install \
    pandas \
    numpy \
    scipy \
    scikit-learn \
    matplotlib \
    seaborn \
    plotly \
    boto3 \
    psycopg2-binary \
    kafka-python \
    redis \
    sqlalchemy \
    pyyaml \
    requests \
    jupyter \
    jupyterlab

# Install additional system tools
log "Installing system tools..."
sudo yum install -y \
    htop \
    iotop \
    tmux \
    git \
    wget \
    curl \
    unzip \
    jq

# Configure Spark settings for better performance
log "Configuring Spark settings..."

# Create Spark defaults configuration
cat <<EOF | sudo tee /etc/spark/conf/spark-defaults.conf
# E-Commerce Analytics Platform Spark Configuration
spark.master                     yarn
spark.submit.deployMode          cluster
spark.sql.warehouse.dir          s3://$LOG_BUCKET/spark-warehouse/
spark.sql.execution.arrow.pyspark.enabled   true
spark.sql.execution.arrow.maxRecordsPerBatch   10000

# Dynamic allocation
spark.dynamicAllocation.enabled             true
spark.dynamicAllocation.minExecutors        1
spark.dynamicAllocation.maxExecutors        20
spark.dynamicAllocation.initialExecutors    2
spark.dynamicAllocation.executorIdleTimeout 60s
spark.dynamicAllocation.cachedExecutorIdleTimeout 300s

# Memory configuration
spark.executor.memory            4g
spark.executor.cores             2
spark.driver.memory              2g
spark.driver.cores               1
spark.executor.memoryFraction    0.8
spark.executor.memoryStorageFraction 0.3

# Serialization
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.kryo.unsafe                true
spark.kryo.registrationRequired  false

# Networking
spark.network.timeout            600s
spark.executor.heartbeatInterval 20s

# Checkpointing
spark.sql.streaming.checkpointLocation  s3://$LOG_BUCKET/spark-checkpoints/

# Event logging
spark.eventLog.enabled           true
spark.eventLog.dir               s3://$LOG_BUCKET/spark-event-logs/
spark.history.fs.logDirectory    s3://$LOG_BUCKET/spark-event-logs/

# Optimization
spark.sql.adaptive.enabled                    true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.adaptive.skewJoin.enabled           true
spark.sql.adaptive.advisoryPartitionSizeInBytes 256MB

# S3 configuration
spark.hadoop.fs.s3a.impl                      org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider  com.amazonaws.auth.DefaultAWSCredentialsProviderChain
spark.hadoop.fs.s3a.server-side-encryption-algorithm   SSE-KMS
spark.hadoop.fs.s3a.server-side-encryption.key         $KMS_KEY_ID

# Delta Lake configuration (if using Delta Lake)
spark.sql.extensions                io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog     org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF

# Configure Hadoop settings
log "Configuring Hadoop settings..."
cat <<EOF | sudo tee /etc/hadoop/conf/core-site.xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3a.server-side-encryption-algorithm</name>
        <value>SSE-KMS</value>
    </property>
    <property>
        <name>fs.s3a.server-side-encryption.key</name>
        <value>$KMS_KEY_ID</value>
    </property>
    <property>
        <name>fs.s3a.multipart.size</name>
        <value>104857600</value>
    </property>
    <property>
        <name>fs.s3a.fast.upload</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.block.size</name>
        <value>134217728</value>
    </property>
</configuration>
EOF

# Install and configure CloudWatch agent for custom metrics
log "Setting up CloudWatch monitoring..."
wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm

# Create CloudWatch configuration
cat <<EOF | sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
{
    "metrics": {
        "namespace": "EMR/Custom",
        "metrics_collected": {
            "cpu": {
                "measurement": [
                    "cpu_usage_idle",
                    "cpu_usage_iowait",
                    "cpu_usage_user",
                    "cpu_usage_system"
                ],
                "metrics_collection_interval": 60,
                "totalcpu": false
            },
            "disk": {
                "measurement": [
                    "used_percent"
                ],
                "metrics_collection_interval": 60,
                "resources": [
                    "*"
                ]
            },
            "diskio": {
                "measurement": [
                    "io_time"
                ],
                "metrics_collection_interval": 60,
                "resources": [
                    "*"
                ]
            },
            "mem": {
                "measurement": [
                    "mem_used_percent"
                ],
                "metrics_collection_interval": 60
            },
            "netstat": {
                "measurement": [
                    "tcp_established",
                    "tcp_time_wait"
                ],
                "metrics_collection_interval": 60
            },
            "swap": {
                "measurement": [
                    "swap_used_percent"
                ],
                "metrics_collection_interval": 60
            }
        }
    },
    "logs": {
        "logs_collected": {
            "files": {
                "collect_list": [
                    {
                        "file_path": "/var/log/messages",
                        "log_group_name": "/aws/emr/$PROJECT_NAME-$ENVIRONMENT/system",
                        "log_stream_name": "{instance_id}/messages"
                    },
                    {
                        "file_path": "/var/log/spark/*.log",
                        "log_group_name": "/aws/emr/$PROJECT_NAME-$ENVIRONMENT/spark",
                        "log_stream_name": "{instance_id}/spark"
                    }
                ]
            }
        }
    }
}
EOF

# Start CloudWatch agent
sudo systemctl enable amazon-cloudwatch-agent
sudo systemctl start amazon-cloudwatch-agent

# Create directories for custom applications
log "Creating application directories..."
sudo mkdir -p /opt/ecap/{jobs,scripts,config,logs}
sudo chown -R hadoop:hadoop /opt/ecap

# Download and setup custom Spark applications
log "Setting up custom Spark applications..."
aws s3 sync s3://$LOG_BUCKET/spark-apps/ /opt/ecap/jobs/ || log "No custom Spark apps found"

# Install Java 11 for better performance (optional)
log "Installing Java 11..."
sudo yum install -y java-11-amazon-corretto-headless
sudo alternatives --set java /usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java

# Configure environment variables
log "Setting up environment variables..."
cat <<EOF | sudo tee /etc/environment
JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64
SPARK_HOME=/usr/lib/spark
HADOOP_HOME=/usr/lib/hadoop
HADOOP_CONF_DIR=/etc/hadoop/conf
YARN_CONF_DIR=/etc/hadoop/conf
PYSPARK_PYTHON=/usr/bin/python3
PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-src.zip:\$PYTHONPATH
EOF

# Setup log rotation for application logs
log "Configuring log rotation..."
cat <<EOF | sudo tee /etc/logrotate.d/ecap
/opt/ecap/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 hadoop hadoop
}
EOF

# Install Delta Lake (optional but recommended)
log "Installing Delta Lake..."
sudo mkdir -p /usr/lib/spark/jars
sudo wget -O /usr/lib/spark/jars/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar

# Create health check script
log "Creating health check script..."
cat <<'EOF' | sudo tee /opt/ecap/scripts/health_check.sh
#!/bin/bash
# Health check script for EMR cluster

check_spark() {
    if pgrep -f "spark" > /dev/null; then
        echo "✓ Spark processes running"
        return 0
    else
        echo "✗ Spark processes not found"
        return 1
    fi
}

check_yarn() {
    if pgrep -f "yarn" > /dev/null; then
        echo "✓ YARN processes running"
        return 0
    else
        echo "✗ YARN processes not found"
        return 1
    fi
}

check_hdfs() {
    if pgrep -f "hadoop" > /dev/null; then
        echo "✓ Hadoop processes running"
        return 0
    else
        echo "✗ Hadoop processes not found"
        return 1
    fi
}

check_disk_space() {
    USAGE=$(df / | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ $USAGE -lt 85 ]; then
        echo "✓ Disk space OK ($USAGE%)"
        return 0
    else
        echo "✗ Disk space critical ($USAGE%)"
        return 1
    fi
}

# Run all checks
echo "=== EMR Cluster Health Check ==="
echo "Timestamp: $(date)"
echo "Host: $(hostname)"
echo

check_spark
check_yarn
check_hdfs
check_disk_space

echo
echo "=== End Health Check ==="
EOF

sudo chmod +x /opt/ecap/scripts/health_check.sh

# Setup cron job for health checks
log "Setting up health check cron job..."
echo "*/5 * * * * /opt/ecap/scripts/health_check.sh >> /opt/ecap/logs/health_check.log 2>&1" | sudo crontab -u hadoop -

# Create performance tuning script
log "Creating performance tuning script..."
cat <<'EOF' | sudo tee /opt/ecap/scripts/tune_performance.sh
#!/bin/bash
# Performance tuning script for EMR cluster

# Increase file descriptor limits
echo "hadoop soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "hadoop hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Optimize kernel parameters
echo "vm.swappiness = 1" | sudo tee -a /etc/sysctl.conf
echo "vm.dirty_ratio = 15" | sudo tee -a /etc/sysctl.conf
echo "vm.dirty_background_ratio = 5" | sudo tee -a /etc/sysctl.conf
echo "net.core.rmem_max = 134217728" | sudo tee -a /etc/sysctl.conf
echo "net.core.wmem_max = 134217728" | sudo tee -a /etc/sysctl.conf

# Apply kernel parameters
sudo sysctl -p

# Disable transparent huge pages
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
EOF

sudo chmod +x /opt/ecap/scripts/tune_performance.sh
sudo /opt/ecap/scripts/tune_performance.sh

# Final cleanup and status
log "Cleaning up..."
sudo yum clean all
rm -f amazon-cloudwatch-agent.rpm

log "Bootstrap configuration completed successfully!"
log "Cluster ready for $PROJECT_NAME-$ENVIRONMENT analytics workloads"

# Create completion marker
echo "$(date): Bootstrap completed successfully" | sudo tee /opt/ecap/bootstrap-complete

# Display final status
log "=== Bootstrap Summary ==="
log "Project: $PROJECT_NAME"
log "Environment: $ENVIRONMENT"
log "Python version: $(python3 --version)"
log "Java version: $(java -version 2>&1 | head -1)"
log "Spark home: $SPARK_HOME"
log "Bootstrap completed at: $(date)"
log "========================"

exit 0

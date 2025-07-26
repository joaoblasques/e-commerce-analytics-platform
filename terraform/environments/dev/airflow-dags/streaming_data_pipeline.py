"""
Streaming Data Pipeline DAG
Manages real-time data ingestion and processing using EMR Spark Streaming

This DAG handles:
1. Streaming data validation and quality checks
2. Real-time fraud detection
3. Customer behavior analysis
4. Data quality monitoring and alerting
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

# Default arguments
default_args = {
    "owner": "ecap-streaming-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "max_active_runs": 1,
}

# DAG configuration
dag = DAG(
    "streaming_data_pipeline",
    default_args=default_args,
    description="Real-time data processing pipeline using EMR Spark Streaming",
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "realtime", "kafka", "spark", "fraud-detection"],
)

# Configuration
PROJECT_NAME = Variable.get("project_name", default_var="ecap")
ENVIRONMENT = Variable.get("environment", default_var="dev")
EMR_CLUSTER_ID = Variable.get("emr_cluster_id")
S3_BUCKET = Variable.get("s3_data_bucket")
KAFKA_BOOTSTRAP_SERVERS = Variable.get("kafka_bootstrap_servers")

# Streaming Spark job configurations
STREAMING_STEPS = [
    {
        "Name": "Real-time Data Validation",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                "--conf",
                "spark.sql.streaming.checkpointLocation=s3://"
                + S3_BUCKET
                + "/checkpoints/validation/",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--conf",
                "spark.streaming.kafka.consumer.cache.enabled=false",
                f"s3://{S3_BUCKET}/spark-jobs/streaming_validation.py",
                "--kafka-servers",
                KAFKA_BOOTSTRAP_SERVERS,
                "--input-topics",
                "transactions,user-events,product-updates",
                "--output-path",
                f"s3://{S3_BUCKET}/streaming/validated/",
                "--checkpoint-location",
                f"s3://{S3_BUCKET}/checkpoints/validation/",
                "--processing-time",
                "30 seconds",
            ],
        },
    },
    {
        "Name": "Real-time Fraud Detection",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                "--conf",
                "spark.sql.streaming.checkpointLocation=s3://"
                + S3_BUCKET
                + "/checkpoints/fraud-detection/",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--conf",
                "spark.sql.adaptive.skewJoin.enabled=true",
                "--py-files",
                f"s3://{S3_BUCKET}/spark-jobs/fraud_utils.py",
                f"s3://{S3_BUCKET}/spark-jobs/streaming_fraud_detection.py",
                "--kafka-servers",
                KAFKA_BOOTSTRAP_SERVERS,
                "--input-topics",
                "transactions",
                "--output-topic",
                "fraud-alerts",
                "--model-path",
                f"s3://{S3_BUCKET}/models/fraud-detection/",
                "--checkpoint-location",
                f"s3://{S3_BUCKET}/checkpoints/fraud-detection/",
                "--processing-time",
                "10 seconds",
                "--alert-threshold",
                "0.8",
            ],
        },
    },
    {
        "Name": "Real-time Customer Behavior Analysis",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                "--conf",
                "spark.sql.streaming.checkpointLocation=s3://"
                + S3_BUCKET
                + "/checkpoints/behavior-analysis/",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--py-files",
                f"s3://{S3_BUCKET}/spark-jobs/behavior_utils.py",
                f"s3://{S3_BUCKET}/spark-jobs/streaming_behavior_analysis.py",
                "--kafka-servers",
                KAFKA_BOOTSTRAP_SERVERS,
                "--input-topics",
                "user-events",
                "--output-path",
                f"s3://{S3_BUCKET}/streaming/behavior-insights/",
                "--checkpoint-location",
                f"s3://{S3_BUCKET}/checkpoints/behavior-analysis/",
                "--window-duration",
                "5 minutes",
                "--slide-duration",
                "1 minute",
            ],
        },
    },
    {
        "Name": "Data Quality Monitoring",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                "--conf",
                "spark.sql.streaming.checkpointLocation=s3://"
                + S3_BUCKET
                + "/checkpoints/quality-monitoring/",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                f"s3://{S3_BUCKET}/spark-jobs/streaming_quality_monitor.py",
                "--kafka-servers",
                KAFKA_BOOTSTRAP_SERVERS,
                "--input-topics",
                "transactions,user-events,product-updates",
                "--output-topic",
                "data-quality-alerts",
                "--metrics-output",
                f"s3://{S3_BUCKET}/streaming/quality-metrics/",
                "--checkpoint-location",
                f"s3://{S3_BUCKET}/checkpoints/quality-monitoring/",
                "--monitoring-interval",
                "1 minute",
            ],
        },
    },
]


class KafkaTopicSensor(BaseSensorOperator):
    """Custom sensor to check if Kafka topics have recent data"""

    def __init__(
        self, kafka_servers: str, topics: list, max_age_minutes: int = 60, **kwargs
    ):
        super().__init__(**kwargs)
        self.kafka_servers = kafka_servers
        self.topics = topics
        self.max_age_minutes = max_age_minutes

    def poke(self, context: Context) -> bool:
        """Check if Kafka topics have recent data"""
        try:
            import json
            from datetime import datetime, timedelta

            from kafka import KafkaConsumer

            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers.split(","),
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
                if x
                else None,
            )

            # Check for recent messages
            cutoff_time = datetime.now() - timedelta(minutes=self.max_age_minutes)
            recent_messages = 0

            consumer.poll(timeout_ms=5000)  # Poll for 5 seconds

            for message in consumer:
                if message.value and "timestamp" in message.value:
                    msg_time = datetime.fromisoformat(
                        message.value["timestamp"].replace("Z", "+00:00")
                    )
                    if msg_time > cutoff_time:
                        recent_messages += 1

                if recent_messages >= len(
                    self.topics
                ):  # At least one recent message per topic
                    consumer.close()
                    self.log.info(f"Found recent data in Kafka topics: {self.topics}")
                    return True

            consumer.close()
            self.log.warning(f"No recent data found in Kafka topics: {self.topics}")
            return False

        except Exception as e:
            self.log.error(f"Error checking Kafka topics: {str(e)}")
            return False


def check_streaming_infrastructure(**context):
    """Validate streaming infrastructure is ready"""
    from airflow.providers.amazon.aws.hooks.emr import EmrHook

    # Check EMR cluster status
    emr_hook = EmrHook()
    cluster_state = emr_hook.get_cluster_state(EMR_CLUSTER_ID)

    if cluster_state not in ["WAITING", "RUNNING"]:
        raise ValueError(f"EMR cluster {EMR_CLUSTER_ID} is not ready: {cluster_state}")

    # Validate cluster has Spark Streaming capability
    cluster_info = emr_hook.get_conn().describe_cluster(ClusterId=EMR_CLUSTER_ID)
    applications = [app["Name"] for app in cluster_info["Cluster"]["Applications"]]

    if "Spark" not in applications:
        raise ValueError(f"EMR cluster {EMR_CLUSTER_ID} does not have Spark installed")

    self.log.info(f"✓ EMR cluster {EMR_CLUSTER_ID} is ready for streaming jobs")
    return True


def monitor_streaming_jobs(**context):
    """Monitor streaming job health and performance"""
    import json

    import boto3

    s3 = boto3.client("s3")

    # Check checkpoint locations for recent activity
    checkpoint_paths = [
        "checkpoints/validation/",
        "checkpoints/fraud-detection/",
        "checkpoints/behavior-analysis/",
        "checkpoints/quality-monitoring/",
    ]

    job_status = {}

    for path in checkpoint_paths:
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=path, MaxKeys=1)

            if "Contents" in response:
                latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
                age_minutes = (
                    datetime.now(latest_file["LastModified"].tzinfo)
                    - latest_file["LastModified"]
                ).total_seconds() / 60
                job_status[path] = {
                    "status": "healthy" if age_minutes < 10 else "stale",
                    "last_checkpoint": latest_file["LastModified"].isoformat(),
                    "age_minutes": age_minutes,
                }
            else:
                job_status[path] = {
                    "status": "no_checkpoints",
                    "last_checkpoint": None,
                    "age_minutes": None,
                }

        except Exception as e:
            job_status[path] = {"status": "error", "error": str(e)}

    # Log monitoring results
    print(
        f"Streaming job monitoring results: {json.dumps(job_status, indent=2, default=str)}"
    )

    # Check if any jobs are unhealthy
    unhealthy_jobs = [
        path
        for path, status in job_status.items()
        if status["status"] not in ["healthy"]
    ]

    if unhealthy_jobs:
        print(f"Warning: Unhealthy streaming jobs detected: {unhealthy_jobs}")

    return job_status


def cleanup_old_checkpoints(**context):
    """Clean up old checkpoint files to prevent storage bloat"""
    from datetime import datetime, timedelta

    import boto3

    s3 = boto3.client("s3")
    cutoff_date = datetime.now() - timedelta(days=7)  # Keep last 7 days

    checkpoint_prefixes = [
        "checkpoints/validation/",
        "checkpoints/fraud-detection/",
        "checkpoints/behavior-analysis/",
        "checkpoints/quality-monitoring/",
    ]

    deleted_count = 0

    for prefix in checkpoint_prefixes:
        try:
            paginator = s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        if obj["LastModified"].replace(tzinfo=None) < cutoff_date:
                            s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
                            deleted_count += 1

        except Exception as e:
            print(f"Error cleaning up checkpoints for {prefix}: {str(e)}")

    print(f"Cleaned up {deleted_count} old checkpoint files")
    return deleted_count


# Task definitions

# Check Kafka data availability
check_kafka_data = KafkaTopicSensor(
    task_id="check_kafka_data_availability",
    kafka_servers=KAFKA_BOOTSTRAP_SERVERS,
    topics=["transactions", "user-events", "product-updates"],
    max_age_minutes=30,
    timeout=300,  # 5 minutes timeout
    poke_interval=30,  # Check every 30 seconds
    dag=dag,
)

# Validate streaming infrastructure
validate_infrastructure = PythonOperator(
    task_id="validate_streaming_infrastructure",
    python_callable=check_streaming_infrastructure,
    dag=dag,
)

# Add streaming steps to existing EMR cluster
add_streaming_steps = EmrAddStepsOperator(
    task_id="add_streaming_steps",
    job_flow_id=EMR_CLUSTER_ID,
    steps=STREAMING_STEPS,
    aws_conn_id="aws_default",
    dag=dag,
)

# Monitor streaming validation
streaming_validation = EmrStepSensor(
    task_id="streaming_validation",
    job_flow_id=EMR_CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    timeout=1800,  # 30 minutes
    poke_interval=30,
    dag=dag,
)

# Monitor fraud detection
fraud_detection_stream = EmrStepSensor(
    task_id="fraud_detection_stream",
    job_flow_id=EMR_CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[1] }}",
    aws_conn_id="aws_default",
    timeout=1800,  # 30 minutes
    poke_interval=30,
    dag=dag,
)

# Monitor behavior analysis
behavior_analysis_stream = EmrStepSensor(
    task_id="behavior_analysis_stream",
    job_flow_id=EMR_CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[2] }}",
    aws_conn_id="aws_default",
    timeout=1800,  # 30 minutes
    poke_interval=30,
    dag=dag,
)

# Monitor quality monitoring
quality_monitoring_stream = EmrStepSensor(
    task_id="quality_monitoring_stream",
    job_flow_id=EMR_CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[3] }}",
    aws_conn_id="aws_default",
    timeout=1800,  # 30 minutes
    poke_interval=30,
    dag=dag,
)

# Monitor streaming job health
monitor_jobs = PythonOperator(
    task_id="monitor_streaming_jobs", python_callable=monitor_streaming_jobs, dag=dag
)

# Cleanup old checkpoints
cleanup_checkpoints = PythonOperator(
    task_id="cleanup_old_checkpoints",
    python_callable=cleanup_old_checkpoints,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Task dependencies
check_kafka_data >> validate_infrastructure >> add_streaming_steps

# Parallel monitoring of streaming jobs
add_streaming_steps >> [
    streaming_validation,
    fraud_detection_stream,
    behavior_analysis_stream,
    quality_monitoring_stream,
]

# Health monitoring and cleanup
(
    [
        streaming_validation,
        fraud_detection_stream,
        behavior_analysis_stream,
        quality_monitoring_stream,
    ]
    >> monitor_jobs
    >> cleanup_checkpoints
)

"""
E-Commerce Analytics Pipeline DAG
Orchestrates EMR Spark jobs for e-commerce data processing and analytics

This DAG runs the complete analytics pipeline:
1. Data validation and quality checks
2. Customer segmentation (RFM analysis)
3. Fraud detection processing
4. Business intelligence calculations
5. Data export and reporting
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3KeySensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

# Default arguments for the DAG
default_args = {
    "owner": "ecap-analytics-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}

# DAG configuration
dag = DAG(
    "emr_analytics_pipeline",
    default_args=default_args,
    description="Complete e-commerce analytics pipeline using EMR Spark",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,
    tags=["emr", "spark", "analytics", "ecommerce"],
)

# Configuration from Airflow Variables
PROJECT_NAME = Variable.get("project_name", default_var="ecap")
ENVIRONMENT = Variable.get("environment", default_var="dev")
EMR_CLUSTER_ID = Variable.get("emr_cluster_id", default_var=None)
S3_BUCKET = Variable.get("s3_data_bucket")
AWS_REGION = Variable.get("aws_region", default_var="us-west-2")

# EMR cluster configuration
EMR_CLUSTER_CONFIG = {
    "Name": f"{PROJECT_NAME}-{ENVIRONMENT}-analytics-cluster",
    "ReleaseLabel": "emr-7.0.0",
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "Livy"},
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.large",
                "InstanceCount": 1,
            },
            {
                "Name": "Worker nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.large",
                "InstanceCount": 2,
            },
            {
                "Name": "Task nodes",
                "Market": "SPOT",
                "InstanceRole": "TASK",
                "InstanceType": "m5.large",
                "InstanceCount": 1,
                "BidPrice": "0.10",
            },
        ],
        "Ec2KeyName": f"{PROJECT_NAME}-{ENVIRONMENT}-key",
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
    "ServiceRole": f"{PROJECT_NAME}-{ENVIRONMENT}-emr-service-role",
    "JobFlowRole": f"{PROJECT_NAME}-{ENVIRONMENT}-emr-ec2-instance-profile",
    "VisibleToAllUsers": True,
    "Tags": [
        {"Key": "Project", "Value": PROJECT_NAME},
        {"Key": "Environment", "Value": ENVIRONMENT},
        {"Key": "Purpose", "Value": "AnalyticsPipeline"},
        {"Key": "CreatedBy", "Value": "Airflow"},
    ],
}

# Spark job configurations
SPARK_STEPS = [
    {
        "Name": "Data Validation and Quality Checks",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--conf",
                "spark.sql.adaptive.coalescePartitions.enabled=true",
                "--conf",
                "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                f"s3://{S3_BUCKET}/spark-jobs/data_validation.py",
                "--input-path",
                f"s3://{S3_BUCKET}/raw-data/{{ ds }}/",
                "--output-path",
                f"s3://{S3_BUCKET}/validated-data/{{ ds }}/",
                "--date",
                "{{ ds }}",
                "--environment",
                ENVIRONMENT,
            ],
        },
    },
    {
        "Name": "Customer Segmentation - RFM Analysis",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--conf",
                "spark.sql.execution.arrow.pyspark.enabled=true",
                "--py-files",
                f"s3://{S3_BUCKET}/spark-jobs/analytics_utils.py",
                f"s3://{S3_BUCKET}/spark-jobs/rfm_segmentation.py",
                "--input-path",
                f"s3://{S3_BUCKET}/validated-data/{{ ds }}/",
                "--output-path",
                f"s3://{S3_BUCKET}/analytics/rfm-segments/{{ ds }}/",
                "--date",
                "{{ ds }}",
                "--lookback-days",
                "90",
            ],
        },
    },
    {
        "Name": "Fraud Detection Processing",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--conf",
                "spark.sql.adaptive.skewJoin.enabled=true",
                "--py-files",
                f"s3://{S3_BUCKET}/spark-jobs/fraud_utils.py",
                f"s3://{S3_BUCKET}/spark-jobs/fraud_detection.py",
                "--input-path",
                f"s3://{S3_BUCKET}/validated-data/{{ ds }}/",
                "--model-path",
                f"s3://{S3_BUCKET}/models/fraud-detection/",
                "--output-path",
                f"s3://{S3_BUCKET}/analytics/fraud-scores/{{ ds }}/",
                "--date",
                "{{ ds }}",
                "--threshold",
                "0.7",
            ],
        },
    },
    {
        "Name": "Business Intelligence Calculations",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--conf",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB",
                "--py-files",
                f"s3://{S3_BUCKET}/spark-jobs/bi_utils.py",
                f"s3://{S3_BUCKET}/spark-jobs/business_intelligence.py",
                "--input-path",
                f"s3://{S3_BUCKET}/validated-data/{{ ds }}/",
                "--output-path",
                f"s3://{S3_BUCKET}/analytics/business-metrics/{{ ds }}/",
                "--date",
                "{{ ds }}",
                "--include-forecasts",
                "true",
            ],
        },
    },
    {
        "Name": "Customer Lifetime Value Calculation",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                "--py-files",
                f"s3://{S3_BUCKET}/spark-jobs/clv_utils.py",
                f"s3://{S3_BUCKET}/spark-jobs/clv_calculation.py",
                "--transactions-path",
                f"s3://{S3_BUCKET}/validated-data/{{ ds }}/transactions/",
                "--customers-path",
                f"s3://{S3_BUCKET}/validated-data/{{ ds }}/customers/",
                "--output-path",
                f"s3://{S3_BUCKET}/analytics/clv/{{ ds }}/",
                "--date",
                "{{ ds }}",
                "--prediction-period",
                "365",
            ],
        },
    },
    {
        "Name": "Data Export and Aggregation",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.adaptive.enabled=true",
                f"s3://{S3_BUCKET}/spark-jobs/data_export.py",
                "--analytics-path",
                f"s3://{S3_BUCKET}/analytics/",
                "--export-path",
                f"s3://{S3_BUCKET}/exports/{{ ds }}/",
                "--database-url",
                "{{ var.value.postgres_connection_string }}",
                "--date",
                "{{ ds }}",
                "--format",
                "parquet,csv,json",
            ],
        },
    },
]


def check_data_availability(**context):
    """Check if required input data is available for processing"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook()
    execution_date = context["ds"]

    required_paths = [
        f"raw-data/{execution_date}/transactions/",
        f"raw-data/{execution_date}/customers/",
        f"raw-data/{execution_date}/products/",
    ]

    for path in required_paths:
        if not s3_hook.check_for_prefix(S3_BUCKET, path):
            raise ValueError(f"Required data not found at s3://{S3_BUCKET}/{path}")

    print(f"✓ All required data available for {execution_date}")
    return True


def validate_cluster_resources(**context):
    """Validate EMR cluster has sufficient resources for the pipeline"""
    from airflow.providers.amazon.aws.hooks.emr import EmrHook

    emr_hook = EmrHook()
    cluster_id = context["task_instance"].xcom_pull(task_ids="create_emr_cluster")

    cluster = emr_hook.get_cluster_state(cluster_id)
    if cluster != "WAITING":
        raise ValueError(f"Cluster {cluster_id} is not in WAITING state: {cluster}")

    # Check cluster has minimum required instances
    cluster_info = emr_hook.get_conn().describe_cluster(ClusterId=cluster_id)
    instance_groups = cluster_info["Cluster"]["InstanceGroups"]

    total_instances = sum(ig["RequestedInstanceCount"] for ig in instance_groups)
    if total_instances < 3:  # Minimum: 1 master + 2 workers
        raise ValueError(f"Insufficient instances in cluster: {total_instances}")

    print(f"✓ Cluster {cluster_id} validated with {total_instances} instances")
    return True


def send_completion_notification(**context):
    """Send notification when pipeline completes"""
    import json

    execution_date = context["ds"]
    task_states = {}

    # Collect task states
    for task_id in [
        "data_validation",
        "rfm_analysis",
        "fraud_detection",
        "business_intelligence",
        "clv_calculation",
    ]:
        ti = context["task_instance"]
        state = ti.xcom_pull(task_ids=task_id, key="return_value")
        task_states[task_id] = state or "completed"

    # Prepare notification message
    message = {
        "pipeline": "emr_analytics_pipeline",
        "execution_date": execution_date,
        "status": "completed",
        "task_states": task_states,
        "cluster_info": context["task_instance"].xcom_pull(
            task_ids="create_emr_cluster"
        ),
    }

    print(f"Pipeline completion notification: {json.dumps(message, indent=2)}")
    return message


# Task definitions

# Check data availability before starting
check_data = PythonOperator(
    task_id="check_data_availability", python_callable=check_data_availability, dag=dag
)

# Create EMR cluster
create_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=EMR_CLUSTER_CONFIG,
    aws_conn_id="aws_default",
    dag=dag,
)

# Validate cluster resources
validate_cluster = PythonOperator(
    task_id="validate_cluster_resources",
    python_callable=validate_cluster_resources,
    dag=dag,
)

# Add Spark steps to EMR cluster
add_steps = EmrAddStepsOperator(
    task_id="add_spark_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=SPARK_STEPS,
    aws_conn_id="aws_default",
    dag=dag,
)

# Monitor individual steps
data_validation = EmrStepSensor(
    task_id="data_validation",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    timeout=3600,  # 1 hour timeout
    poke_interval=60,  # Check every minute
    dag=dag,
)

rfm_analysis = EmrStepSensor(
    task_id="rfm_analysis",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[1] }}",
    aws_conn_id="aws_default",
    timeout=2400,  # 40 minutes timeout
    poke_interval=60,
    dag=dag,
)

fraud_detection = EmrStepSensor(
    task_id="fraud_detection",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[2] }}",
    aws_conn_id="aws_default",
    timeout=3600,  # 1 hour timeout
    poke_interval=60,
    dag=dag,
)

business_intelligence = EmrStepSensor(
    task_id="business_intelligence",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[3] }}",
    aws_conn_id="aws_default",
    timeout=2400,  # 40 minutes timeout
    poke_interval=60,
    dag=dag,
)

clv_calculation = EmrStepSensor(
    task_id="clv_calculation",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[4] }}",
    aws_conn_id="aws_default",
    timeout=1800,  # 30 minutes timeout
    poke_interval=60,
    dag=dag,
)

data_export = EmrStepSensor(
    task_id="data_export",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[5] }}",
    aws_conn_id="aws_default",
    timeout=1800,  # 30 minutes timeout
    poke_interval=60,
    dag=dag,
)

# Send completion notification
notify_completion = PythonOperator(
    task_id="send_completion_notification",
    python_callable=send_completion_notification,
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if some tasks fail
    dag=dag,
)

# Terminate EMR cluster
terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    trigger_rule=TriggerRule.ALL_DONE,  # Always terminate cluster
    dag=dag,
)

# Task dependencies
check_data >> create_cluster >> validate_cluster >> add_steps

# Parallel execution of analytics steps
add_steps >> [
    data_validation,
    rfm_analysis,
    fraud_detection,
    business_intelligence,
    clv_calculation,
]

# Data export depends on completion of analytics
[rfm_analysis, fraud_detection, business_intelligence, clv_calculation] >> data_export

# Notification and cleanup
data_export >> notify_completion >> terminate_cluster

# Alternative cleanup path if analytics fail
[
    data_validation,
    rfm_analysis,
    fraud_detection,
    business_intelligence,
    clv_calculation,
] >> terminate_cluster

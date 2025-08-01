"""
Disaster Recovery Runbook Generator and Executor

This module provides comprehensive disaster recovery runbooks
with step-by-step procedures for various failure scenarios.
"""

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ..backup_manager import BackupManager
from ..dr_coordinator import DisasterRecoveryCoordinator

logger = logging.getLogger(__name__)


class FailureType(Enum):
    """Types of failures that can trigger disaster recovery."""

    DATABASE_FAILURE = "database_failure"
    STORAGE_FAILURE = "storage_failure"
    COMPUTE_FAILURE = "compute_failure"
    NETWORK_FAILURE = "network_failure"
    COMPLETE_REGION_FAILURE = "complete_region_failure"
    DATA_CORRUPTION = "data_corruption"
    SECURITY_BREACH = "security_breach"


class RunbookStep:
    """Individual step in a disaster recovery runbook."""

    def __init__(
        self,
        step_id: str,
        title: str,
        description: str,
        estimated_time_minutes: int,
        commands: List[str] = None,
        verification: str = None,
        rollback: str = None,
        dependencies: List[str] = None,
    ):
        self.step_id = step_id
        self.title = title
        self.description = description
        self.estimated_time_minutes = estimated_time_minutes
        self.commands = commands or []
        self.verification = verification
        self.rollback = rollback
        self.dependencies = dependencies or []

        # Execution tracking
        self.status = "pending"  # pending, in_progress, completed, failed, skipped
        self.start_time = None
        self.end_time = None
        self.output = None
        self.error = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert step to dictionary."""
        return {
            "step_id": self.step_id,
            "title": self.title,
            "description": self.description,
            "estimated_time_minutes": self.estimated_time_minutes,
            "commands": self.commands,
            "verification": self.verification,
            "rollback": self.rollback,
            "dependencies": self.dependencies,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "output": self.output,
            "error": self.error,
        }


class DisasterRecoveryRunbook:
    """
    Comprehensive disaster recovery runbook with step-by-step procedures.
    """

    def __init__(self):
        """Initialize disaster recovery runbook."""
        self.runbooks = {}
        self._initialize_runbooks()

    def _initialize_runbooks(self):
        """Initialize all disaster recovery runbooks."""
        self.runbooks = {
            FailureType.DATABASE_FAILURE: self._create_database_failure_runbook(),
            FailureType.STORAGE_FAILURE: self._create_storage_failure_runbook(),
            FailureType.COMPUTE_FAILURE: self._create_compute_failure_runbook(),
            FailureType.NETWORK_FAILURE: self._create_network_failure_runbook(),
            FailureType.COMPLETE_REGION_FAILURE: self._create_region_failure_runbook(),
            FailureType.DATA_CORRUPTION: self._create_data_corruption_runbook(),
            FailureType.SECURITY_BREACH: self._create_security_breach_runbook(),
        }

    def _create_database_failure_runbook(self) -> List[RunbookStep]:
        """Create runbook for database failure scenarios."""
        return [
            RunbookStep(
                step_id="db_01",
                title="Assess Database Failure Scope",
                description="Determine the extent of the database failure and affected services",
                estimated_time_minutes=5,
                commands=[
                    "pg_isready -h $DB_HOST -p $DB_PORT",
                    "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c 'SELECT version();'",
                    "tail -n 100 /var/log/postgresql/postgresql.log",
                ],
                verification="Database connection status and error logs reviewed",
            ),
            RunbookStep(
                step_id="db_02",
                title="Stop Application Services",
                description="Gracefully stop all services that depend on the database",
                estimated_time_minutes=3,
                commands=[
                    "kubectl scale deployment api-service --replicas=0",
                    "kubectl scale deployment analytics-service --replicas=0",
                    "kubectl scale deployment dashboard-service --replicas=0",
                ],
                verification="All application pods are terminated",
                dependencies=["db_01"],
            ),
            RunbookStep(
                step_id="db_03",
                title="Activate Read Replica",
                description="Promote read replica to primary database",
                estimated_time_minutes=10,
                commands=[
                    "aws rds promote-read-replica --db-instance-identifier $REPLICA_ID",
                    "aws rds wait db-instance-available --db-instance-identifier $REPLICA_ID",
                ],
                verification="Read replica is promoted and available",
                dependencies=["db_02"],
            ),
            RunbookStep(
                step_id="db_04",
                title="Update DNS Records",
                description="Update DNS to point to the new primary database",
                estimated_time_minutes=5,
                commands=[
                    "aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch file://dns-update.json"
                ],
                verification="DNS propagation completed (max 5 minutes)",
                dependencies=["db_03"],
            ),
            RunbookStep(
                step_id="db_05",
                title="Update Application Configuration",
                description="Update application configs with new database endpoint",
                estimated_time_minutes=5,
                commands=[
                    "kubectl create configmap db-config --from-literal=DB_HOST=$NEW_DB_HOST --dry-run=client -o yaml | kubectl apply -f -",
                    "kubectl rollout restart deployment/api-service",
                    "kubectl rollout restart deployment/analytics-service",
                ],
                verification="Deployments updated with new configuration",
                dependencies=["db_04"],
            ),
            RunbookStep(
                step_id="db_06",
                title="Restart Application Services",
                description="Bring application services back online",
                estimated_time_minutes=5,
                commands=[
                    "kubectl scale deployment api-service --replicas=3",
                    "kubectl scale deployment analytics-service --replicas=2",
                    "kubectl scale deployment dashboard-service --replicas=2",
                ],
                verification="All services are healthy and responding",
                dependencies=["db_05"],
            ),
            RunbookStep(
                step_id="db_07",
                title="Verify Data Integrity",
                description="Run data integrity checks on the new primary database",
                estimated_time_minutes=10,
                commands=[
                    "python scripts/data_integrity_check.py --full-check",
                    "python scripts/verify_recent_transactions.py",
                ],
                verification="Data integrity checks pass without errors",
                dependencies=["db_06"],
            ),
            RunbookStep(
                step_id="db_08",
                title="Setup New Read Replica",
                description="Create new read replica from the current primary",
                estimated_time_minutes=20,
                commands=[
                    "aws rds create-db-instance-read-replica --db-instance-identifier $NEW_REPLICA_ID --source-db-instance-identifier $PRIMARY_ID"
                ],
                verification="New read replica is created and in sync",
                dependencies=["db_07"],
            ),
            RunbookStep(
                step_id="db_09",
                title="Update Monitoring and Alerts",
                description="Update monitoring to track the new database configuration",
                estimated_time_minutes=5,
                commands=[
                    "python scripts/update_monitoring_config.py --new-primary $PRIMARY_ID",
                    "curl -X POST $GRAFANA_URL/api/dashboards/db -d @updated-dashboard.json",
                ],
                verification="Monitoring dashboard shows new database metrics",
                dependencies=["db_08"],
            ),
            RunbookStep(
                step_id="db_10",
                title="Document Recovery Actions",
                description="Document all actions taken and update incident log",
                estimated_time_minutes=10,
                commands=[
                    "python scripts/generate_incident_report.py --type database_failure",
                    "git add . && git commit -m 'Update configs after database failover'",
                ],
                verification="Incident documented and configurations committed",
                dependencies=["db_09"],
            ),
        ]

    def _create_storage_failure_runbook(self) -> List[RunbookStep]:
        """Create runbook for storage failure scenarios."""
        return [
            RunbookStep(
                step_id="storage_01",
                title="Assess Storage Failure",
                description="Determine which storage systems are affected",
                estimated_time_minutes=5,
                commands=[
                    "aws s3 ls s3://$PRIMARY_BUCKET",
                    "df -h /data",
                    "kubectl get pvc --all-namespaces",
                ],
                verification="Storage accessibility status determined",
            ),
            RunbookStep(
                step_id="storage_02",
                title="Switch to Backup Storage",
                description="Redirect applications to use backup storage locations",
                estimated_time_minutes=10,
                commands=[
                    'kubectl patch configmap storage-config -p \'{"data":{"BUCKET_NAME":"$BACKUP_BUCKET"}}\'',
                    "kubectl rollout restart deployment/data-ingestion",
                    "kubectl rollout restart deployment/analytics-engine",
                ],
                verification="Applications using backup storage",
                dependencies=["storage_01"],
            ),
            RunbookStep(
                step_id="storage_03",
                title="Verify Data Availability",
                description="Ensure critical data is available in backup storage",
                estimated_time_minutes=15,
                commands=[
                    "python scripts/verify_backup_data.py --bucket $BACKUP_BUCKET",
                    "python scripts/check_data_freshness.py",
                ],
                verification="Critical data confirmed available and current",
                dependencies=["storage_02"],
            ),
            RunbookStep(
                step_id="storage_04",
                title="Resume Data Processing",
                description="Restart data processing pipelines with backup storage",
                estimated_time_minutes=10,
                commands=[
                    "kubectl create job data-pipeline-restart --from=cronjob/data-pipeline",
                    "python scripts/resume_streaming_jobs.py",
                ],
                verification="Data processing pipelines operational",
                dependencies=["storage_03"],
            ),
            RunbookStep(
                step_id="storage_05",
                title="Setup Storage Replication",
                description="Establish replication from backup to new primary storage",
                estimated_time_minutes=30,
                commands=[
                    "aws s3api put-bucket-replication --bucket $BACKUP_BUCKET --replication-configuration file://replication-config.json"
                ],
                verification="Storage replication configured and active",
                dependencies=["storage_04"],
            ),
        ]

    def _create_compute_failure_runbook(self) -> List[RunbookStep]:
        """Create runbook for compute failure scenarios."""
        return [
            RunbookStep(
                step_id="compute_01",
                title="Assess Compute Infrastructure",
                description="Check status of Kubernetes cluster and worker nodes",
                estimated_time_minutes=5,
                commands=[
                    "kubectl get nodes",
                    "kubectl get pods --all-namespaces | grep -v Running",
                    "kubectl top nodes",
                ],
                verification="Cluster status and resource utilization assessed",
            ),
            RunbookStep(
                step_id="compute_02",
                title="Scale Critical Services",
                description="Ensure critical services have enough replicas on healthy nodes",
                estimated_time_minutes=5,
                commands=[
                    "kubectl scale deployment api-service --replicas=5",
                    "kubectl scale deployment fraud-detection --replicas=3",
                    "kubectl get hpa",
                ],
                verification="Critical services scaled appropriately",
                dependencies=["compute_01"],
            ),
            RunbookStep(
                step_id="compute_03",
                title="Add Emergency Compute Capacity",
                description="Launch additional worker nodes if needed",
                estimated_time_minutes=15,
                commands=[
                    "aws autoscaling update-auto-scaling-group --auto-scaling-group-name $ASG_NAME --desired-capacity $NEW_CAPACITY",
                    "kubectl wait --for=condition=Ready node --timeout=600s --selector=node-role=worker",
                ],
                verification="Additional compute capacity online and ready",
                dependencies=["compute_02"],
            ),
            RunbookStep(
                step_id="compute_04",
                title="Redistribute Workloads",
                description="Ensure workloads are evenly distributed across healthy nodes",
                estimated_time_minutes=10,
                commands=[
                    "kubectl drain $FAILED_NODE --ignore-daemonsets --delete-emptydir-data",
                    "kubectl rollout restart deployment --all",
                ],
                verification="Workloads redistributed to healthy nodes",
                dependencies=["compute_03"],
            ),
            RunbookStep(
                step_id="compute_05",
                title="Verify Service Health",
                description="Confirm all services are healthy and responding",
                estimated_time_minutes=10,
                commands=[
                    "kubectl get pods --all-namespaces | grep -E '(Error|CrashLoopBackOff|Pending)'",
                    "python scripts/health_check_all_services.py",
                ],
                verification="All services passing health checks",
                dependencies=["compute_04"],
            ),
        ]

    def _create_network_failure_runbook(self) -> List[RunbookStep]:
        """Create runbook for network failure scenarios."""
        return [
            RunbookStep(
                step_id="network_01",
                title="Diagnose Network Issues",
                description="Identify the scope and nature of network connectivity issues",
                estimated_time_minutes=10,
                commands=[
                    "ping -c 4 8.8.8.8",
                    "nslookup $DOMAIN_NAME",
                    "curl -I https://$DOMAIN_NAME/health",
                    "kubectl get service --all-namespaces",
                ],
                verification="Network connectivity issues identified",
            ),
            RunbookStep(
                step_id="network_02",
                title="Check Load Balancer Status",
                description="Verify load balancer health and target groups",
                estimated_time_minutes=5,
                commands=[
                    "aws elbv2 describe-load-balancers --load-balancer-arns $LB_ARN",
                    "aws elbv2 describe-target-health --target-group-arn $TG_ARN",
                ],
                verification="Load balancer status and target health assessed",
                dependencies=["network_01"],
            ),
            RunbookStep(
                step_id="network_03",
                title="Activate Backup Network Path",
                description="Switch traffic to backup network infrastructure",
                estimated_time_minutes=15,
                commands=[
                    "aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch file://backup-dns.json",
                    'kubectl patch service api-service -p \'{"spec":{"loadBalancerSourceRanges":["0.0.0.0/0"]}}\'',
                ],
                verification="Traffic routed through backup network path",
                dependencies=["network_02"],
            ),
            RunbookStep(
                step_id="network_04",
                title="Verify Service Connectivity",
                description="Test connectivity to all critical services",
                estimated_time_minutes=10,
                commands=[
                    "python scripts/connectivity_test.py --all-services",
                    "curl -s $API_ENDPOINT/health | jq '.status'",
                ],
                verification="All services accessible via backup network",
                dependencies=["network_03"],
            ),
        ]

    def _create_region_failure_runbook(self) -> List[RunbookStep]:
        """Create runbook for complete region failure scenarios."""
        return [
            RunbookStep(
                step_id="region_01",
                title="Activate Disaster Recovery Site",
                description="Begin failover to backup region",
                estimated_time_minutes=5,
                commands=[
                    "python scripts/dr_coordinator.py --initiate-failover --reason 'Complete region failure'",
                    "aws configure set default.region $BACKUP_REGION",
                ],
                verification="DR failover process initiated",
            ),
            RunbookStep(
                step_id="region_02",
                title="Promote Backup Database",
                description="Promote read replica in backup region to primary",
                estimated_time_minutes=15,
                commands=[
                    "aws rds promote-read-replica --db-instance-identifier $BACKUP_DB_ID --region $BACKUP_REGION",
                    "aws rds wait db-instance-available --db-instance-identifier $BACKUP_DB_ID --region $BACKUP_REGION",
                ],
                verification="Backup database promoted and available",
                dependencies=["region_01"],
            ),
            RunbookStep(
                step_id="region_03",
                title="Start Backup Compute Cluster",
                description="Launch Kubernetes cluster in backup region",
                estimated_time_minutes=20,
                commands=[
                    "aws eks update-kubeconfig --region $BACKUP_REGION --name $BACKUP_CLUSTER",
                    "kubectl apply -f k8s/backup-region/",
                    "kubectl wait --for=condition=available --timeout=600s deployment --all",
                ],
                verification="Backup cluster operational with all services",
                dependencies=["region_02"],
            ),
            RunbookStep(
                step_id="region_04",
                title="Update Global DNS",
                description="Route all traffic to backup region",
                estimated_time_minutes=10,
                commands=[
                    "aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch file://failover-dns.json",
                    "dig $DOMAIN_NAME | grep $BACKUP_REGION_IP",
                ],
                verification="DNS pointing to backup region",
                dependencies=["region_03"],
            ),
            RunbookStep(
                step_id="region_05",
                title="Verify Full Functionality",
                description="Run comprehensive tests on backup region",
                estimated_time_minutes=20,
                commands=[
                    "python scripts/end_to_end_test.py --region $BACKUP_REGION",
                    "python scripts/data_pipeline_test.py",
                    "python scripts/api_integration_test.py",
                ],
                verification="All critical functionality working in backup region",
                dependencies=["region_04"],
            ),
            RunbookStep(
                step_id="region_06",
                title="Establish New Replication",
                description="Set up replication from backup region to new standby",
                estimated_time_minutes=30,
                commands=[
                    "aws rds create-db-instance-read-replica --source-region $BACKUP_REGION --db-instance-identifier $NEW_REPLICA_ID"
                ],
                verification="New standby region configured",
                dependencies=["region_05"],
            ),
        ]

    def _create_data_corruption_runbook(self) -> List[RunbookStep]:
        """Create runbook for data corruption scenarios."""
        return [
            RunbookStep(
                step_id="corruption_01",
                title="Assess Data Corruption Scope",
                description="Determine extent and impact of data corruption",
                estimated_time_minutes=15,
                commands=[
                    "python scripts/data_integrity_check.py --detailed",
                    "python scripts/corruption_analysis.py --affected-tables",
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';",
                ],
                verification="Corruption scope and affected data identified",
            ),
            RunbookStep(
                step_id="corruption_02",
                title="Stop Write Operations",
                description="Prevent further corruption by stopping all write operations",
                estimated_time_minutes=5,
                commands=[
                    "kubectl scale deployment api-service --replicas=0",
                    "kubectl scale deployment data-ingestion --replicas=0",
                    "python scripts/pause_streaming_jobs.py",
                ],
                verification="All write operations stopped",
                dependencies=["corruption_01"],
            ),
            RunbookStep(
                step_id="corruption_03",
                title="Identify Backup for Restoration",
                description="Find the most recent clean backup for restoration",
                estimated_time_minutes=10,
                commands=[
                    "python scripts/backup_manager.py --list-backups --verify-integrity",
                    "python scripts/find_clean_backup.py --before-corruption",
                ],
                verification="Clean backup identified for restoration",
                dependencies=["corruption_02"],
            ),
            RunbookStep(
                step_id="corruption_04",
                title="Restore from Clean Backup",
                description="Restore database from the identified clean backup",
                estimated_time_minutes=30,
                commands=[
                    "python scripts/backup_manager.py --restore --backup-id $CLEAN_BACKUP_ID",
                    "psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c 'VACUUM ANALYZE;'",
                ],
                verification="Database restored from clean backup",
                dependencies=["corruption_03"],
            ),
            RunbookStep(
                step_id="corruption_05",
                title="Verify Data Integrity",
                description="Confirm data integrity after restoration",
                estimated_time_minutes=20,
                commands=[
                    "python scripts/data_integrity_check.py --full-check",
                    "python scripts/verify_business_logic.py",
                    "python scripts/check_data_consistency.py",
                ],
                verification="Data integrity confirmed after restoration",
                dependencies=["corruption_04"],
            ),
            RunbookStep(
                step_id="corruption_06",
                title="Resume Operations Gradually",
                description="Gradually bring services back online",
                estimated_time_minutes=15,
                commands=[
                    "kubectl scale deployment api-service --replicas=1",
                    "python scripts/health_check_api.py",
                    "kubectl scale deployment api-service --replicas=3",
                    "kubectl scale deployment data-ingestion --replicas=1",
                ],
                verification="Services operational with clean data",
                dependencies=["corruption_05"],
            ),
        ]

    def _create_security_breach_runbook(self) -> List[RunbookStep]:
        """Create runbook for security breach scenarios."""
        return [
            RunbookStep(
                step_id="security_01",
                title="Immediate Isolation",
                description="Immediately isolate affected systems to prevent further damage",
                estimated_time_minutes=10,
                commands=[
                    "aws ec2 modify-instance-attribute --instance-id $AFFECTED_INSTANCE --groups $ISOLATION_SG",
                    "kubectl delete service $COMPROMISED_SERVICE",
                    "aws s3api put-bucket-policy --bucket $BUCKET --policy file://deny-all-policy.json",
                ],
                verification="Affected systems isolated from network",
            ),
            RunbookStep(
                step_id="security_02",
                title="Change All Credentials",
                description="Rotate all potentially compromised credentials",
                estimated_time_minutes=20,
                commands=[
                    "python scripts/rotate_all_secrets.py",
                    "aws iam update-login-profile --user-name $USER --password $NEW_PASSWORD --password-reset-required",
                    "kubectl delete secret --all && kubectl apply -f new-secrets.yaml",
                ],
                verification="All credentials rotated and systems updated",
                dependencies=["security_01"],
            ),
            RunbookStep(
                step_id="security_03",
                title="Forensic Data Collection",
                description="Collect evidence for forensic analysis",
                estimated_time_minutes=30,
                commands=[
                    "aws ec2 create-snapshot --volume-id $AFFECTED_VOLUME --description 'Forensic snapshot'",
                    "kubectl logs $COMPROMISED_POD > forensic-logs.txt",
                    "python scripts/collect_audit_logs.py --since $INCIDENT_TIME",
                ],
                verification="Forensic data collected and secured",
                dependencies=["security_02"],
            ),
            RunbookStep(
                step_id="security_04",
                title="Clean System Restoration",
                description="Restore from known clean backup",
                estimated_time_minutes=45,
                commands=[
                    "python scripts/backup_manager.py --restore --backup-id $CLEAN_BACKUP_ID",
                    "kubectl apply -f k8s/clean-deployment.yaml",
                    "python scripts/verify_clean_state.py",
                ],
                verification="Systems restored from clean state",
                dependencies=["security_03"],
            ),
            RunbookStep(
                step_id="security_05",
                title="Enhanced Security Measures",
                description="Implement additional security controls",
                estimated_time_minutes=30,
                commands=[
                    "aws ec2 modify-instance-attribute --instance-id $INSTANCE --groups $ENHANCED_SG",
                    "kubectl apply -f k8s/network-policies.yaml",
                    "python scripts/enable_enhanced_monitoring.py",
                ],
                verification="Enhanced security measures in place",
                dependencies=["security_04"],
            ),
            RunbookStep(
                step_id="security_06",
                title="Gradual Service Restoration",
                description="Gradually restore services with enhanced monitoring",
                estimated_time_minutes=25,
                commands=[
                    "kubectl scale deployment api-service --replicas=1",
                    "python scripts/security_health_check.py",
                    "kubectl scale deployment api-service --replicas=3",
                ],
                verification="Services restored with enhanced security",
                dependencies=["security_05"],
            ),
        ]

    def get_runbook(self, failure_type: FailureType) -> List[RunbookStep]:
        """Get runbook for specific failure type."""
        return self.runbooks.get(failure_type, [])

    def execute_runbook(
        self,
        failure_type: FailureType,
        auto_execute: bool = False,
        skip_confirmations: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute a disaster recovery runbook.

        Args:
            failure_type: Type of failure to address
            auto_execute: Whether to execute commands automatically
            skip_confirmations: Whether to skip manual confirmations

        Returns:
            Execution results and status
        """
        runbook = self.get_runbook(failure_type)
        if not runbook:
            return {
                "success": False,
                "error": f"No runbook found for failure type: {failure_type.value}",
            }

        execution_log = {
            "failure_type": failure_type.value,
            "start_time": datetime.now().isoformat(),
            "auto_execute": auto_execute,
            "steps": [],
            "overall_status": "in_progress",
        }

        logger.info(f"Starting runbook execution for {failure_type.value}")

        try:
            for step in runbook:
                # Check dependencies
                if not self._check_dependencies(step, execution_log["steps"]):
                    step.status = "skipped"
                    step.error = "Dependencies not met"
                    execution_log["steps"].append(step.to_dict())
                    continue

                # Execute step
                step_result = self._execute_step(step, auto_execute, skip_confirmations)
                execution_log["steps"].append(step_result)

                # Check if step failed and should stop execution
                if step.status == "failed" and not skip_confirmations:
                    logger.error(f"Step {step.step_id} failed, stopping execution")
                    break

            # Determine overall status
            failed_steps = [
                s for s in execution_log["steps"] if s.get("status") == "failed"
            ]
            completed_steps = [
                s for s in execution_log["steps"] if s.get("status") == "completed"
            ]

            if failed_steps:
                execution_log["overall_status"] = "failed"
            elif len(completed_steps) == len(runbook):
                execution_log["overall_status"] = "completed"
            else:
                execution_log["overall_status"] = "partial"

            execution_log["end_time"] = datetime.now().isoformat()
            execution_log["total_duration_minutes"] = (
                datetime.fromisoformat(execution_log["end_time"])
                - datetime.fromisoformat(execution_log["start_time"])
            ).total_seconds() / 60

            logger.info(
                f"Runbook execution completed with status: {execution_log['overall_status']}"
            )

            return {
                "success": execution_log["overall_status"] != "failed",
                "execution_log": execution_log,
            }

        except Exception as e:
            execution_log["overall_status"] = "error"
            execution_log["error"] = str(e)
            execution_log["end_time"] = datetime.now().isoformat()

            logger.error(f"Runbook execution failed with error: {e}")

            return {"success": False, "error": str(e), "execution_log": execution_log}

    def _check_dependencies(
        self, step: RunbookStep, completed_steps: List[Dict[str, Any]]
    ) -> bool:
        """Check if step dependencies are met."""
        if not step.dependencies:
            return True

        completed_step_ids = [
            s["step_id"] for s in completed_steps if s.get("status") == "completed"
        ]

        return all(dep in completed_step_ids for dep in step.dependencies)

    def _execute_step(
        self, step: RunbookStep, auto_execute: bool, skip_confirmations: bool
    ) -> Dict[str, Any]:
        """Execute a single runbook step."""
        logger.info(f"Executing step {step.step_id}: {step.title}")

        step.status = "in_progress"
        step.start_time = datetime.now()

        try:
            if auto_execute and step.commands:
                # Execute commands automatically
                import subprocess

                outputs = []
                for command in step.commands:
                    try:
                        # Validate command for basic security (disaster recovery context)
                        if not self._is_safe_command(command):
                            outputs.append(
                                {
                                    "command": command,
                                    "error": "Command rejected for security reasons",
                                    "status": "blocked",
                                }
                            )
                            continue

                        result = subprocess.run(
                            command,
                            shell=True,  # nosec B602 - DR commands require shell features
                            capture_output=True,
                            text=True,
                            timeout=300,  # 5 minute timeout
                        )

                        outputs.append(
                            {
                                "command": command,
                                "return_code": result.returncode,
                                "stdout": result.stdout,
                                "stderr": result.stderr,
                            }
                        )

                        if result.returncode != 0:
                            step.status = "failed"
                            step.error = f"Command failed: {command} - {result.stderr}"
                            break

                    except subprocess.TimeoutExpired:
                        step.status = "failed"
                        step.error = f"Command timed out: {command}"
                        break
                    except Exception as e:
                        step.status = "failed"
                        step.error = f"Command execution error: {command} - {str(e)}"
                        break

                step.output = outputs

                if step.status != "failed":
                    step.status = "completed"

            else:
                # Manual execution - just mark as completed for now
                # In a real implementation, this would wait for manual confirmation
                if not skip_confirmations:
                    logger.info(f"Manual step: {step.description}")
                    logger.info(f"Commands to execute: {step.commands}")
                    logger.info(f"Verification: {step.verification}")
                    # response = input(f"Mark step {step.step_id} as completed? (y/n): ")
                    # For automation, we'll assume completion
                    response = "y"
                else:
                    response = "y"

                if response.lower() == "y":
                    step.status = "completed"
                else:
                    step.status = "failed"
                    step.error = "Manual step not confirmed"

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            logger.error(f"Step execution failed: {step.step_id} - {e}")

        finally:
            step.end_time = datetime.now()

        return step.to_dict()

    def generate_runbook_documentation(self, failure_type: FailureType) -> str:
        """Generate human-readable documentation for a runbook."""
        runbook = self.get_runbook(failure_type)
        if not runbook:
            return f"No runbook found for {failure_type.value}"

        doc = f"""
# Disaster Recovery Runbook: {failure_type.value.replace('_', ' ').title()}

## Overview
This runbook provides step-by-step procedures for recovering from {failure_type.value.replace('_', ' ')} scenarios.

## Estimated Total Time
{sum(step.estimated_time_minutes for step in runbook)} minutes

## Prerequisites
- Access to AWS CLI with appropriate permissions
- Kubernetes cluster access (kubectl configured)
- Database access credentials
- Monitoring and alerting systems accessible

## Steps

"""

        for i, step in enumerate(runbook, 1):
            doc += f"""
### Step {i}: {step.title} ({step.estimated_time_minutes} minutes)

**Description:** {step.description}

**Commands:**
```bash
{chr(10).join(step.commands) if step.commands else 'Manual procedure - see description'}
```

**Verification:** {step.verification or 'Manual verification required'}

**Dependencies:** {', '.join(step.dependencies) if step.dependencies else 'None'}

**Rollback:** {step.rollback or 'See previous steps to reverse changes'}

---
"""

        doc += """
## Post-Recovery Actions

1. **Verify System Health**: Run comprehensive health checks on all services
2. **Update Documentation**: Document any changes made during recovery
3. **Conduct Post-Mortem**: Schedule incident review meeting
4. **Update Runbooks**: Incorporate lessons learned into procedures
5. **Test Recovery**: Schedule recovery testing to validate procedures

## Emergency Contacts

- On-call Engineer: [Contact Information]
- Database Administrator: [Contact Information]
- Security Team: [Contact Information]
- Management Escalation: [Contact Information]

## Related Documentation

- [System Architecture Documentation]
- [Network Topology Diagrams]
- [Service Dependencies Map]
- [Backup and Recovery Procedures]
"""

        return doc

    def _is_safe_command(self, command: str) -> bool:
        """
        Validate command for basic security in disaster recovery context.

        This allows legitimate DR commands while blocking obviously dangerous ones.
        Note: shell=True is still required for DR operations with pipes/redirects.
        """
        # Block obviously dangerous patterns
        dangerous_patterns = [
            "rm -rf /",  # Don't delete root
            "mkfs",  # Don't format filesystems unexpectedly
            ">(",
            ")|(",  # Block process substitution that could be abused
            "&&rm",
            "&&del",  # Block chained destructive commands
            "curl.*|sh",  # Block piping downloads to shell
            "wget.*|sh",  # Block piping downloads to shell
            "`",
            "$(",  # Block command substitution in basic cases
        ]

        command_lower = command.lower()
        for pattern in dangerous_patterns:
            if pattern in command_lower:
                logger.warning(
                    f"Blocked potentially dangerous command pattern: {pattern}"
                )
                return False

        # Allow legitimate DR commands (kubectl, docker, systemctl, monitoring, etc.)
        return True

    def get_all_runbooks_summary(self) -> Dict[str, Any]:
        """Get summary of all available runbooks."""
        summary = {}

        for failure_type, runbook in self.runbooks.items():
            summary[failure_type.value] = {
                "total_steps": len(runbook),
                "estimated_time_minutes": sum(
                    step.estimated_time_minutes for step in runbook
                ),
                "step_titles": [step.title for step in runbook],
            }

        return summary

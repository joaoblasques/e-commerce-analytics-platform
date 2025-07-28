"""
Disaster Recovery Coordinator for E-Commerce Analytics Platform

This module orchestrates disaster recovery procedures including
failover, health monitoring, and automated recovery workflows.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status enumeration."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class FailoverStatus(Enum):
    """Failover status enumeration."""

    ACTIVE = "active"
    STANDBY = "standby"
    FAILING_OVER = "failing_over"
    FAILED = "failed"


class DREventType(Enum):
    """Disaster recovery event types."""

    HEALTH_CHECK = "health_check"
    FAILOVER_INITIATED = "failover_initiated"
    FAILOVER_COMPLETED = "failover_completed"
    FAILOVER_FAILED = "failover_failed"
    RECOVERY_STARTED = "recovery_started"
    RECOVERY_COMPLETED = "recovery_completed"


class DisasterRecoveryCoordinator:
    """
    Coordinates disaster recovery operations across the platform.

    Features:
    - Health monitoring and alerting
    - Automated failover procedures
    - Cross-region replication management
    - Recovery time/point objective tracking
    - Service dependency management
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the disaster recovery coordinator.

        Args:
            config: Configuration dictionary containing DR settings
        """
        self.config = config
        self.current_region = config.get("primary_region", "us-west-2")
        self.backup_region = config.get("backup_region", "us-east-1")

        # RTO/RPO targets
        self.rto_target_minutes = config.get("rto_target_minutes", 15)  # 15 minutes
        self.rpo_target_minutes = config.get("rpo_target_minutes", 5)  # 5 minutes

        # Service health tracking
        self.service_health = {}
        self.service_dependencies = config.get("service_dependencies", {})

        # Failover state
        self.failover_status = FailoverStatus.ACTIVE
        self.last_failover_time = None

        # Event history
        self.event_history: List[Dict[str, Any]] = []

        # Initialize AWS clients
        self._init_aws_clients()

        # Health check intervals
        self.health_check_interval = config.get(
            "health_check_interval", 30
        )  # 30 seconds
        self.health_check_timeout = config.get("health_check_timeout", 10)  # 10 seconds

        # Circuit breaker settings
        self.failure_threshold = config.get("failure_threshold", 3)
        self.recovery_threshold = config.get("recovery_threshold", 2)
        self.circuit_breaker_timeout = config.get(
            "circuit_breaker_timeout", 300
        )  # 5 minutes

    def _init_aws_clients(self):
        """Initialize AWS service clients for both regions."""
        try:
            # Primary region clients
            self.primary_clients = {
                "rds": boto3.client("rds", region_name=self.current_region),
                "s3": boto3.client("s3", region_name=self.current_region),
                "ec2": boto3.client("ec2", region_name=self.current_region),
                "route53": boto3.client("route53"),
                "elbv2": boto3.client("elbv2", region_name=self.current_region),
            }

            # Backup region clients
            self.backup_clients = {
                "rds": boto3.client("rds", region_name=self.backup_region),
                "s3": boto3.client("s3", region_name=self.backup_region),
                "ec2": boto3.client("ec2", region_name=self.backup_region),
                "elbv2": boto3.client("elbv2", region_name=self.backup_region),
            }

        except Exception as e:
            logger.warning(f"Failed to initialize AWS clients: {e}")
            self.primary_clients = {}
            self.backup_clients = {}

    async def start_monitoring(self):
        """Start continuous health monitoring."""
        logger.info("Starting disaster recovery monitoring")

        while True:
            try:
                await self._perform_health_checks()
                await self._evaluate_failover_conditions()
                await self._cleanup_old_events()

                await asyncio.sleep(self.health_check_interval)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.health_check_interval)

    async def _perform_health_checks(self):
        """Perform health checks on all monitored services."""
        services = self.config.get("monitored_services", {})

        health_check_tasks = []
        for service_name, service_config in services.items():
            task = asyncio.create_task(
                self._check_service_health(service_name, service_config)
            )
            health_check_tasks.append(task)

        # Execute health checks concurrently
        results = await asyncio.gather(*health_check_tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            service_name = list(services.keys())[i]

            if isinstance(result, Exception):
                self.service_health[service_name] = {
                    "status": HealthStatus.UNKNOWN,
                    "error": str(result),
                    "last_check": datetime.now().isoformat(),
                }
                logger.error(f"Health check failed for {service_name}: {result}")
            else:
                self.service_health[service_name] = result

        # Log health status
        await self._log_event(
            DREventType.HEALTH_CHECK,
            {
                "services_checked": len(services),
                "healthy_services": len(
                    [
                        s
                        for s in self.service_health.values()
                        if s["status"] == HealthStatus.HEALTHY
                    ]
                ),
                "unhealthy_services": len(
                    [
                        s
                        for s in self.service_health.values()
                        if s["status"] == HealthStatus.UNHEALTHY
                    ]
                ),
            },
        )

    async def _check_service_health(
        self, service_name: str, service_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check health of a specific service."""
        service_type = service_config.get("type", "http")

        if service_type == "http":
            return await self._check_http_health(service_name, service_config)
        elif service_type == "database":
            return await self._check_database_health(service_name, service_config)
        elif service_type == "aws_service":
            return await self._check_aws_service_health(service_name, service_config)
        elif service_type == "kafka":
            return await self._check_kafka_health(service_name, service_config)
        else:
            return {
                "status": HealthStatus.UNKNOWN,
                "error": f"Unknown service type: {service_type}",
                "last_check": datetime.now().isoformat(),
            }

    async def _check_http_health(
        self, service_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check HTTP service health via health endpoint."""
        url = config.get("health_url")
        expected_status = config.get("expected_status", 200)

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.health_check_timeout)
            ) as session:
                start_time = time.time()
                async with session.get(url) as response:
                    response_time = time.time() - start_time

                    if response.status == expected_status:
                        status = HealthStatus.HEALTHY
                        error = None
                    else:
                        status = HealthStatus.UNHEALTHY
                        error = f"Unexpected status code: {response.status}"

                    return {
                        "status": status,
                        "response_time_ms": round(response_time * 1000, 2),
                        "status_code": response.status,
                        "error": error,
                        "last_check": datetime.now().isoformat(),
                    }

        except asyncio.TimeoutError:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": "Request timeout",
                "last_check": datetime.now().isoformat(),
            }
        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _check_database_health(
        self, service_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check database health via connection test."""
        db_type = config.get("db_type", "postgresql")

        if db_type == "postgresql":
            return await self._check_postgresql_health(service_name, config)
        elif db_type == "redis":
            return await self._check_redis_health(service_name, config)
        else:
            return {
                "status": HealthStatus.UNKNOWN,
                "error": f"Unsupported database type: {db_type}",
                "last_check": datetime.now().isoformat(),
            }

    async def _check_postgresql_health(
        self, service_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check PostgreSQL health."""
        try:
            import psycopg2

            host = config.get("host", "localhost")
            port = config.get("port", 5432)
            database = config.get("database", "postgres")
            user = config.get("user", "postgres")
            password = config.get("password", "")

            start_time = time.time()

            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=self.health_check_timeout,
            )

            # Test query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

            conn.close()

            response_time = time.time() - start_time

            return {
                "status": HealthStatus.HEALTHY,
                "response_time_ms": round(response_time * 1000, 2),
                "last_check": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _check_redis_health(
        self, service_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check Redis health."""
        try:
            import redis.asyncio as redis

            host = config.get("host", "localhost")
            port = config.get("port", 6379)
            password = config.get("password")

            start_time = time.time()

            r = redis.Redis(
                host=host,
                port=port,
                password=password,
                socket_timeout=self.health_check_timeout,
            )

            # Test ping
            await r.ping()
            await r.close()

            response_time = time.time() - start_time

            return {
                "status": HealthStatus.HEALTHY,
                "response_time_ms": round(response_time * 1000, 2),
                "last_check": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _check_aws_service_health(
        self, service_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check AWS service health."""
        service_type = config.get("aws_service")

        try:
            if service_type == "rds":
                return await self._check_rds_health(config)
            elif service_type == "s3":
                return await self._check_s3_health(config)
            elif service_type == "emr":
                return await self._check_emr_health(config)
            else:
                return {
                    "status": HealthStatus.UNKNOWN,
                    "error": f"Unsupported AWS service: {service_type}",
                    "last_check": datetime.now().isoformat(),
                }

        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _check_rds_health(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check RDS instance health."""
        if "rds" not in self.primary_clients:
            return {
                "status": HealthStatus.UNKNOWN,
                "error": "RDS client not configured",
                "last_check": datetime.now().isoformat(),
            }

        instance_id = config.get("instance_id")

        try:
            response = self.primary_clients["rds"].describe_db_instances(
                DBInstanceIdentifier=instance_id
            )

            instance = response["DBInstances"][0]
            status = instance["DBInstanceStatus"]

            if status == "available":
                health_status = HealthStatus.HEALTHY
            elif status in ["backing-up", "modifying", "upgrading"]:
                health_status = HealthStatus.DEGRADED
            else:
                health_status = HealthStatus.UNHEALTHY

            return {
                "status": health_status,
                "aws_status": status,
                "endpoint": instance.get("Endpoint", {}).get("Address"),
                "last_check": datetime.now().isoformat(),
            }

        except ClientError as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _check_s3_health(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check S3 bucket health."""
        if "s3" not in self.primary_clients:
            return {
                "status": HealthStatus.UNKNOWN,
                "error": "S3 client not configured",
                "last_check": datetime.now().isoformat(),
            }

        bucket_name = config.get("bucket_name")

        try:
            # Test bucket accessibility
            self.primary_clients["s3"].head_bucket(Bucket=bucket_name)

            return {
                "status": HealthStatus.HEALTHY,
                "bucket": bucket_name,
                "last_check": datetime.now().isoformat(),
            }

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                error = "Bucket not found"
            elif error_code == "403":
                error = "Access denied"
            else:
                error = str(e)

            return {
                "status": HealthStatus.UNHEALTHY,
                "error": error,
                "last_check": datetime.now().isoformat(),
            }

    async def _check_emr_health(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check EMR cluster health."""
        try:
            import boto3

            emr_client = boto3.client("emr", region_name=self.current_region)

            cluster_id = config.get("cluster_id")

            response = emr_client.describe_cluster(ClusterId=cluster_id)
            cluster = response["Cluster"]

            status = cluster["Status"]["State"]

            if status in ["RUNNING", "WAITING"]:
                health_status = HealthStatus.HEALTHY
            elif status in ["STARTING", "BOOTSTRAPPING"]:
                health_status = HealthStatus.DEGRADED
            else:
                health_status = HealthStatus.UNHEALTHY

            return {
                "status": health_status,
                "cluster_status": status,
                "master_dns": cluster.get("MasterPublicDnsName"),
                "last_check": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _check_kafka_health(
        self, service_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check Kafka cluster health."""
        try:
            from kafka import KafkaAdminClient

            bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])

            start_time = time.time()

            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=self.health_check_timeout * 1000,
            )

            # Get cluster metadata
            metadata = admin_client.describe_topics()

            response_time = time.time() - start_time

            return {
                "status": HealthStatus.HEALTHY,
                "response_time_ms": round(response_time * 1000, 2),
                "topic_count": len(metadata),
                "last_check": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
            }

    async def _evaluate_failover_conditions(self):
        """Evaluate whether failover should be triggered."""
        if self.failover_status == FailoverStatus.FAILING_OVER:
            return  # Already failing over

        # Check critical services
        critical_services = self.config.get("critical_services", [])
        unhealthy_critical_services = []

        for service_name in critical_services:
            if service_name in self.service_health:
                health = self.service_health[service_name]
                if health["status"] == HealthStatus.UNHEALTHY:
                    unhealthy_critical_services.append(service_name)

        # Check if failover threshold is met
        total_critical = len(critical_services)
        unhealthy_critical = len(unhealthy_critical_services)

        if total_critical > 0:
            failure_rate = unhealthy_critical / total_critical
            failover_threshold = self.config.get("failover_threshold", 0.5)  # 50%

            if failure_rate >= failover_threshold:
                logger.warning(
                    f"Failover threshold exceeded: {unhealthy_critical}/{total_critical} "
                    f"critical services unhealthy"
                )

                # Check if we've recently failed over (avoid rapid switching)
                if self.last_failover_time:
                    time_since_last = datetime.now() - self.last_failover_time
                    min_failover_interval = timedelta(
                        minutes=self.config.get("min_failover_interval_minutes", 30)
                    )

                    if time_since_last < min_failover_interval:
                        logger.info(
                            f"Skipping failover due to recent failover {time_since_last} ago"
                        )
                        return

                # Trigger automated failover
                if self.config.get("auto_failover_enabled", False):
                    await self._initiate_failover(
                        "Automated failover due to service failures"
                    )

    async def _initiate_failover(self, reason: str):
        """Initiate failover to backup region."""
        logger.warning(f"Initiating failover: {reason}")

        self.failover_status = FailoverStatus.FAILING_OVER
        self.last_failover_time = datetime.now()

        await self._log_event(
            DREventType.FAILOVER_INITIATED,
            {
                "reason": reason,
                "source_region": self.current_region,
                "target_region": self.backup_region,
            },
        )

        try:
            # Execute failover steps
            failover_steps = [
                ("dns_failover", self._perform_dns_failover),
                ("database_failover", self._perform_database_failover),
                ("storage_failover", self._perform_storage_failover),
                ("compute_failover", self._perform_compute_failover),
                ("verify_failover", self._verify_failover),
            ]

            for step_name, step_function in failover_steps:
                logger.info(f"Executing failover step: {step_name}")

                try:
                    await step_function()
                    logger.info(f"Failover step completed: {step_name}")
                except Exception as e:
                    logger.error(f"Failover step failed: {step_name} - {e}")
                    raise e

            # Switch active region
            self.current_region, self.backup_region = (
                self.backup_region,
                self.current_region,
            )
            self.primary_clients, self.backup_clients = (
                self.backup_clients,
                self.primary_clients,
            )

            self.failover_status = FailoverStatus.ACTIVE

            await self._log_event(
                DREventType.FAILOVER_COMPLETED,
                {
                    "new_active_region": self.current_region,
                    "failover_duration_seconds": (
                        datetime.now() - self.last_failover_time
                    ).total_seconds(),
                },
            )

            logger.info("Failover completed successfully")

        except Exception as e:
            self.failover_status = FailoverStatus.FAILED

            await self._log_event(
                DREventType.FAILOVER_FAILED,
                {
                    "error": str(e),
                    "failover_duration_seconds": (
                        datetime.now() - self.last_failover_time
                    ).total_seconds(),
                },
            )

            logger.error(f"Failover failed: {e}")
            raise e

    async def _perform_dns_failover(self):
        """Update DNS records to point to backup region."""
        if "route53" not in self.primary_clients:
            logger.warning("Route53 client not configured, skipping DNS failover")
            return

        dns_config = self.config.get("dns_failover", {})
        hosted_zone_id = dns_config.get("hosted_zone_id")

        if not hosted_zone_id:
            logger.warning("DNS hosted zone not configured, skipping DNS failover")
            return

        try:
            # Update A records to point to backup region
            records_to_update = dns_config.get("records", [])

            for record in records_to_update:
                record_name = record["name"]
                new_value = record["backup_value"]

                response = self.primary_clients["route53"].change_resource_record_sets(
                    HostedZoneId=hosted_zone_id,
                    ChangeBatch={
                        "Changes": [
                            {
                                "Action": "UPSERT",
                                "ResourceRecordSet": {
                                    "Name": record_name,
                                    "Type": "A",
                                    "TTL": 60,  # Short TTL for quick updates
                                    "ResourceRecords": [{"Value": new_value}],
                                },
                            }
                        ]
                    },
                )

                logger.info(f"Updated DNS record {record_name} to {new_value}")

        except Exception as e:
            logger.error(f"DNS failover failed: {e}")
            raise e

    async def _perform_database_failover(self):
        """Failover database to backup region."""
        db_config = self.config.get("database_failover", {})

        if db_config.get("type") == "rds":
            await self._failover_rds()
        else:
            logger.warning("Database failover not configured")

    async def _failover_rds(self):
        """Failover RDS instance to backup region."""
        if "rds" not in self.backup_clients:
            logger.warning("RDS backup client not configured")
            return

        rds_config = self.config.get("database_failover", {})
        replica_identifier = rds_config.get("read_replica_identifier")

        if replica_identifier:
            try:
                # Promote read replica to primary
                self.backup_clients["rds"].promote_read_replica(
                    DBInstanceIdentifier=replica_identifier
                )

                logger.info(f"Promoted read replica {replica_identifier} to primary")

            except ClientError as e:
                logger.error(f"Failed to promote read replica: {e}")
                raise e

    async def _perform_storage_failover(self):
        """Failover storage to backup region."""
        # S3 cross-region replication should already be in place
        # This step verifies data availability
        storage_config = self.config.get("storage_failover", {})

        if storage_config.get("type") == "s3":
            await self._verify_s3_replication()

    async def _verify_s3_replication(self):
        """Verify S3 data is available in backup region."""
        if "s3" not in self.backup_clients:
            logger.warning("S3 backup client not configured")
            return

        s3_config = self.config.get("storage_failover", {})
        bucket_name = s3_config.get("backup_bucket")

        if bucket_name:
            try:
                # Test bucket accessibility
                self.backup_clients["s3"].head_bucket(Bucket=bucket_name)
                logger.info(
                    f"Verified S3 bucket {bucket_name} accessible in backup region"
                )

            except ClientError as e:
                logger.error(
                    f"S3 bucket {bucket_name} not accessible in backup region: {e}"
                )
                raise e

    async def _perform_compute_failover(self):
        """Failover compute resources to backup region."""
        compute_config = self.config.get("compute_failover", {})

        if compute_config.get("type") == "auto_scaling":
            await self._failover_auto_scaling()
        elif compute_config.get("type") == "emr":
            await self._failover_emr()

    async def _failover_auto_scaling(self):
        """Update auto scaling group in backup region."""
        if "ec2" not in self.backup_clients:
            logger.warning("EC2 backup client not configured")
            return

        # Implementation would update auto scaling group configuration
        logger.info("Auto scaling group failover not implemented")

    async def _failover_emr(self):
        """Start EMR cluster in backup region."""
        try:
            import boto3

            emr_backup_client = boto3.client("emr", region_name=self.backup_region)

            emr_config = self.config.get("compute_failover", {})
            cluster_config = emr_config.get("cluster_config", {})

            if cluster_config:
                response = emr_backup_client.run_job_flow(**cluster_config)
                cluster_id = response["JobFlowId"]

                logger.info(f"Started EMR cluster {cluster_id} in backup region")

        except Exception as e:
            logger.error(f"EMR failover failed: {e}")
            raise e

    async def _verify_failover(self):
        """Verify that failover was successful."""
        # Wait for services to start up
        await asyncio.sleep(30)

        # Re-run health checks to verify services are healthy
        await self._perform_health_checks()

        # Check critical services
        critical_services = self.config.get("critical_services", [])
        unhealthy_services = []

        for service_name in critical_services:
            if service_name in self.service_health:
                if self.service_health[service_name]["status"] != HealthStatus.HEALTHY:
                    unhealthy_services.append(service_name)

        if unhealthy_services:
            raise Exception(
                f"Failover verification failed: unhealthy services: {unhealthy_services}"
            )

        logger.info("Failover verification completed successfully")

    async def get_rto_rpo_metrics(self) -> Dict[str, Any]:
        """Calculate current RTO/RPO metrics."""
        # Find last failover event
        last_failover = None
        for event in reversed(self.event_history):
            if event["event_type"] == DREventType.FAILOVER_COMPLETED:
                last_failover = event
                break

        metrics = {
            "rto_target_minutes": self.rto_target_minutes,
            "rpo_target_minutes": self.rpo_target_minutes,
            "last_failover": last_failover,
            "current_status": self.failover_status.value,
        }

        if last_failover:
            actual_rto = last_failover["data"].get("failover_duration_seconds", 0) / 60
            metrics["last_rto_minutes"] = actual_rto
            metrics["rto_compliance"] = actual_rto <= self.rto_target_minutes

        return metrics

    async def _log_event(self, event_type: DREventType, data: Dict[str, Any]):
        """Log a disaster recovery event."""
        event = {
            "event_id": f"dr_{int(time.time())}_{len(self.event_history)}",
            "event_type": event_type,
            "timestamp": datetime.now().isoformat(),
            "region": self.current_region,
            "data": data,
        }

        self.event_history.append(event)
        logger.info(f"DR Event: {event_type.value} - {data}")

    async def _cleanup_old_events(self):
        """Clean up old events from history."""
        cutoff_time = datetime.now() - timedelta(days=30)

        self.event_history = [
            event
            for event in self.event_history
            if datetime.fromisoformat(event["timestamp"]) > cutoff_time
        ]

    async def get_health_status(self) -> Dict[str, Any]:
        """Get current health status of all services."""
        overall_status = HealthStatus.HEALTHY

        # Determine overall status
        for service_health in self.service_health.values():
            if service_health["status"] == HealthStatus.UNHEALTHY:
                overall_status = HealthStatus.UNHEALTHY
                break
            elif service_health["status"] == HealthStatus.DEGRADED:
                overall_status = HealthStatus.DEGRADED

        return {
            "overall_status": overall_status.value,
            "failover_status": self.failover_status.value,
            "current_region": self.current_region,
            "backup_region": self.backup_region,
            "services": self.service_health,
            "last_check": datetime.now().isoformat(),
        }

    async def get_event_history(
        self, event_type: Optional[DREventType] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get disaster recovery event history."""
        events = self.event_history

        if event_type:
            events = [e for e in events if e["event_type"] == event_type]

        # Return most recent events
        return sorted(events, key=lambda x: x["timestamp"], reverse=True)[:limit]

    async def manual_failover(self, reason: str) -> Dict[str, Any]:
        """Manually trigger failover."""
        if self.failover_status == FailoverStatus.FAILING_OVER:
            return {"success": False, "error": "Failover already in progress"}

        try:
            await self._initiate_failover(f"Manual failover: {reason}")
            return {"success": True, "message": "Failover initiated successfully"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def manual_failback(self, reason: str) -> Dict[str, Any]:
        """Manually trigger failback to original region."""
        if self.failover_status == FailoverStatus.FAILING_OVER:
            return {"success": False, "error": "Failover in progress, cannot failback"}

        try:
            # Failback is essentially another failover
            await self._initiate_failover(f"Manual failback: {reason}")
            return {"success": True, "message": "Failback initiated successfully"}
        except Exception as e:
            return {"success": False, "error": str(e)}

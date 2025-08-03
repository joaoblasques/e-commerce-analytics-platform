#!/usr/bin/env python3
"""
Kafka Management Script for E-Commerce Analytics Platform

This script provides comprehensive Kafka administration capabilities including:
- Topic creation, deletion, and configuration
- Partition management and rebalancing
- Consumer group management
- Monitoring and health checks
- Data retention policy configuration

Usage:
    python manage_kafka.py create-topics
    python manage_kafka.py list-topics
    python manage_kafka.py describe-topic <topic-name>
    python manage_kafka.py delete-topic <topic-name>
    python manage_kafka.py list-consumer-groups
    python manage_kafka.py reset-consumer-group <group-name>
    python manage_kafka.py health-check
"""

import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import click
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient
from kafka.admin.config_resource import ConfigResource
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("kafka_management.log"),
    ],
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_CLIENT_ID = "ecap-kafka-manager"

# Topic configurations for e-commerce analytics platform
TOPIC_CONFIGS = {
    "transactions": {
        "partitions": 6,
        "replication_factor": 1,
        "configs": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "segment.ms": str(24 * 60 * 60 * 1000),  # 1 day
            "cleanup.policy": "delete",
            "compression.type": "lz4",
            "min.insync.replicas": "1",
            "max.message.bytes": "1048576",  # 1MB
        },
        "description": "E-commerce transaction events with high throughput requirements",
    },
    "user-events": {
        "partitions": 12,
        "replication_factor": 1,
        "configs": {
            "retention.ms": str(3 * 24 * 60 * 60 * 1000),  # 3 days
            "segment.ms": str(12 * 60 * 60 * 1000),  # 12 hours
            "cleanup.policy": "delete",
            "compression.type": "lz4",
            "min.insync.replicas": "1",
            "max.message.bytes": "524288",  # 512KB
        },
        "description": "User behavior events (page views, clicks, searches) with very high volume",
    },
    "product-updates": {
        "partitions": 3,
        "replication_factor": 1,
        "configs": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
            "segment.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "cleanup.policy": "compact",
            "compression.type": "gzip",
            "min.insync.replicas": "1",
            "max.message.bytes": "2097152",  # 2MB
        },
        "description": "Product catalog updates with log compaction for latest state",
    },
    "fraud-alerts": {
        "partitions": 2,
        "replication_factor": 1,
        "configs": {
            "retention.ms": str(90 * 24 * 60 * 60 * 1000),  # 90 days
            "segment.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "cleanup.policy": "delete",
            "compression.type": "gzip",
            "min.insync.replicas": "1",
            "max.message.bytes": "1048576",  # 1MB
        },
        "description": "Fraud detection alerts requiring longer retention",
    },
    "analytics-results": {
        "partitions": 4,
        "replication_factor": 1,
        "configs": {
            "retention.ms": str(14 * 24 * 60 * 60 * 1000),  # 14 days
            "segment.ms": str(24 * 60 * 60 * 1000),  # 1 day
            "cleanup.policy": "delete",
            "compression.type": "gzip",
            "min.insync.replicas": "1",
            "max.message.bytes": "10485760",  # 10MB
        },
        "description": "Processed analytics results from Spark jobs",
    },
}


class KafkaManager:
    """Kafka management operations for the e-commerce analytics platform."""

    def __init__(self, bootstrap_servers: List[str] = None):
        """Initialize Kafka manager with connection settings."""
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.client_id = KAFKA_CLIENT_ID
        self.admin_client = None
        self.producer = None
        self.consumer = None

    def connect(self):
        """Establish connection to Kafka cluster."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                request_timeout_ms=30000,
                connections_max_idle_ms=540000,
            )
            logger.info(f"Connected to Kafka cluster at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False

    def create_topics(self, topics: List[str] = None) -> bool:
        """Create Kafka topics with optimized configurations."""
        if not self.admin_client:
            if not self.connect():
                return False

        topics_to_create = topics or list(TOPIC_CONFIGS.keys())
        new_topics = []

        for topic_name in topics_to_create:
            if topic_name not in TOPIC_CONFIGS:
                logger.warning(f"Topic {topic_name} not found in configuration")
                continue

            config = TOPIC_CONFIGS[topic_name]
            topic = NewTopic(
                name=topic_name,
                num_partitions=config["partitions"],
                replication_factor=config["replication_factor"],
                topic_configs=config["configs"],
            )
            new_topics.append(topic)
            logger.info(f"Preparing to create topic: {topic_name}")

        try:
            # Create topics
            fs = self.admin_client.create_topics(new_topics, validate_only=False)

            # Wait for creation to complete
            for topic in new_topics:
                try:
                    if hasattr(fs, "items"):
                        future = fs[topic.name]
                    else:
                        future = fs.get(topic.name, None)

                    if future:
                        future.result()
                        logger.info(f"✅ Topic '{topic.name}' created successfully")
                except TopicAlreadyExistsError:
                    logger.info(f"ℹ️  Topic '{topic.name}' already exists")
                except Exception as e:
                    logger.error(f"❌ Failed to create topic '{topic.name}': {e}")
                    return False

            # Verify topics were created
            time.sleep(2)
            existing_topics = self.list_topics()
            for topic_name in topics_to_create:
                if topic_name in existing_topics:
                    config = TOPIC_CONFIGS[topic_name]
                    logger.info(
                        f"✅ {topic_name}: {config['partitions']} partitions, {config['description']}"
                    )
                else:
                    logger.error(f"❌ Topic {topic_name} not found after creation")
                    return False

            return True

        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            return False

    def list_topics(self) -> List[str]:
        """List all topics in the Kafka cluster."""
        try:
            # Use KafkaConsumer to get topic metadata
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
            topics = consumer.list_topics()
            consumer.close()

            topic_names = list(topics.keys()) if topics else []
            logger.info(f"Found {len(topic_names)} topics in cluster")
            return topic_names
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []

    def describe_topic(self, topic_name: str) -> Optional[Dict]:
        """Get detailed information about a specific topic."""
        if not self.admin_client:
            if not self.connect():
                return None

        try:
            metadata = self.admin_client.describe_topics([topic_name])
            if topic_name in metadata:
                topic_metadata = metadata[topic_name]

                # Get topic configuration
                config_resources = [
                    ConfigResource(ConfigResourceType.TOPIC, topic_name)
                ]
                configs = self.admin_client.describe_configs(config_resources)
                topic_config = configs.get(config_resources[0], {})

                result = {
                    "name": topic_name,
                    "partitions": len(topic_metadata.partitions),
                    "replication_factor": len(topic_metadata.partitions[0].replicas)
                    if topic_metadata.partitions
                    else 0,
                    "configs": {k: v.value for k, v in topic_config.items()}
                    if hasattr(topic_config, "items")
                    else {},
                    "partition_details": [],
                }

                for partition in topic_metadata.partitions:
                    result["partition_details"].append(
                        {
                            "partition_id": partition.partition_id,
                            "leader": partition.leader,
                            "replicas": partition.replicas,
                            "isr": partition.isr,
                        }
                    )

                return result
            else:
                logger.warning(f"Topic {topic_name} not found")
                return None

        except Exception as e:
            logger.error(f"Failed to describe topic {topic_name}: {e}")
            return None

    def delete_topic(self, topic_name: str) -> bool:
        """Delete a topic from the Kafka cluster."""
        if not self.admin_client:
            if not self.connect():
                return False

        try:
            fs = self.admin_client.delete_topics([topic_name])

            # Wait for deletion to complete
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic}' deleted successfully")
                    return True
                except UnknownTopicOrPartitionError:
                    logger.warning(f"Topic '{topic}' does not exist")
                    return False
                except Exception as e:
                    logger.error(f"❌ Failed to delete topic '{topic}': {e}")
                    return False

        except Exception as e:
            logger.error(f"Failed to delete topic {topic_name}: {e}")
            return False

    def list_consumer_groups(self) -> List[str]:
        """List all consumer groups in the Kafka cluster."""
        if not self.admin_client:
            if not self.connect():
                return []

        try:
            groups = self.admin_client.list_consumer_groups()
            group_names = [group.group_id for group in groups]
            logger.info(f"Found {len(group_names)} consumer groups")
            return group_names
        except Exception as e:
            logger.error(f"Failed to list consumer groups: {e}")
            return []

    def health_check(self) -> Dict[str, any]:
        """Perform comprehensive health check of Kafka cluster."""
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "cluster_connection": False,
            "topics": {},
            "consumer_groups": [],
            "overall_status": "UNHEALTHY",
        }

        # Test cluster connection
        if self.connect():
            health_status["cluster_connection"] = True

            # Check topics
            topics = self.list_topics()
            for topic_name in TOPIC_CONFIGS.keys():
                if topic_name in topics:
                    topic_info = self.describe_topic(topic_name)
                    health_status["topics"][topic_name] = {
                        "exists": True,
                        "partitions": topic_info["partitions"] if topic_info else 0,
                        "status": "HEALTHY",
                    }
                else:
                    health_status["topics"][topic_name] = {
                        "exists": False,
                        "status": "MISSING",
                    }

            # Check consumer groups
            health_status["consumer_groups"] = self.list_consumer_groups()

            # Overall health assessment
            all_topics_healthy = all(
                topic["exists"] for topic in health_status["topics"].values()
            )

            if all_topics_healthy:
                health_status["overall_status"] = "HEALTHY"
            else:
                health_status["overall_status"] = "DEGRADED"

        return health_status

    def produce_test_message(self, topic_name: str, message: Dict) -> bool:
        """Produce a test message to verify topic functionality."""
        try:
            if not self.producer:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id=self.client_id,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda v: str(v).encode("utf-8") if v else None,
                )

            future = self.producer.send(
                topic_name, key=f"test-{int(time.time())}", value=message
            )

            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            logger.info(
                f"✅ Test message sent to {topic_name} "
                f"(partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset})"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to send test message to {topic_name}: {e}")
            return False

    def close(self):
        """Close all connections."""
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()


# CLI Commands
@click.group()
def cli():
    """Kafka Management CLI for E-Commerce Analytics Platform."""
    pass


@cli.command()
@click.option("--topics", "-t", multiple=True, help="Specific topics to create")
def create_topics(topics):
    """Create Kafka topics with optimized configurations."""
    manager = KafkaManager()

    click.echo("🚀 Creating Kafka topics for E-Commerce Analytics Platform...")
    click.echo("=" * 60)

    topic_list = list(topics) if topics else None
    success = manager.create_topics(topic_list)

    if success:
        click.echo("\n✅ All topics created successfully!")
        click.echo("\nTopic Summary:")
        click.echo("-" * 40)

        topics_to_show = topic_list or TOPIC_CONFIGS.keys()
        for topic_name in topics_to_show:
            if topic_name in TOPIC_CONFIGS:
                config = TOPIC_CONFIGS[topic_name]
                click.echo(f"📊 {topic_name}:")
                click.echo(f"   Partitions: {config['partitions']}")
                click.echo(
                    f"   Retention: {int(config['configs']['retention.ms']) // (24*60*60*1000)} days"
                )
                click.echo(f"   Purpose: {config['description']}")
                click.echo()
    else:
        click.echo("❌ Failed to create topics. Check logs for details.")
        sys.exit(1)

    manager.close()


@cli.command()
def list_topics():
    """List all topics in the Kafka cluster."""
    manager = KafkaManager()

    click.echo("📋 Kafka Topics:")
    click.echo("=" * 40)

    topics = manager.list_topics()
    if topics:
        for topic in sorted(topics):
            click.echo(f"📊 {topic}")
    else:
        click.echo("No topics found or connection failed.")

    manager.close()


@cli.command()
@click.argument("topic_name")
def describe_topic(topic_name):
    """Describe a specific topic in detail."""
    manager = KafkaManager()

    click.echo(f"📊 Topic Details: {topic_name}")
    click.echo("=" * 50)

    topic_info = manager.describe_topic(topic_name)
    if topic_info:
        click.echo(f"Name: {topic_info['name']}")
        click.echo(f"Partitions: {topic_info['partitions']}")
        click.echo(f"Replication Factor: {topic_info['replication_factor']}")
        click.echo()

        click.echo("Configuration:")
        for key, value in topic_info["configs"].items():
            click.echo(f"  {key}: {value}")

        click.echo()
        click.echo("Partition Details:")
        for partition in topic_info["partition_details"]:
            click.echo(
                f"  Partition {partition['partition_id']}: "
                f"Leader={partition['leader']}, "
                f"Replicas={partition['replicas']}, "
                f"ISR={partition['isr']}"
            )
    else:
        click.echo("Topic not found or connection failed.")

    manager.close()


@cli.command()
@click.argument("topic_name")
@click.confirmation_option(prompt="Are you sure you want to delete this topic?")
def delete_topic(topic_name):
    """Delete a topic from the Kafka cluster."""
    manager = KafkaManager()

    click.echo(f"🗑️  Deleting topic: {topic_name}")

    success = manager.delete_topic(topic_name)
    if success:
        click.echo(f"✅ Topic '{topic_name}' deleted successfully!")
    else:
        click.echo(f"❌ Failed to delete topic '{topic_name}'.")

    manager.close()


@cli.command()
def list_consumer_groups():
    """List all consumer groups in the Kafka cluster."""
    manager = KafkaManager()

    click.echo("👥 Consumer Groups:")
    click.echo("=" * 30)

    groups = manager.list_consumer_groups()
    if groups:
        for group in sorted(groups):
            click.echo(f"👥 {group}")
    else:
        click.echo("No consumer groups found or connection failed.")

    manager.close()


@cli.command()
def health_check():
    """Perform comprehensive health check of Kafka cluster."""
    manager = KafkaManager()

    click.echo("🏥 Kafka Health Check")
    click.echo("=" * 40)

    health = manager.health_check()

    click.echo(f"Timestamp: {health['timestamp']}")
    click.echo(f"Cluster Connection: {'✅' if health['cluster_connection'] else '❌'}")
    click.echo(f"Overall Status: {health['overall_status']}")
    click.echo()

    click.echo("Topics Status:")
    for topic_name, status in health["topics"].items():
        status_icon = "✅" if status["exists"] else "❌"
        click.echo(f"  {status_icon} {topic_name}: {status['status']}")
        if status["exists"]:
            click.echo(f"     Partitions: {status['partitions']}")

    click.echo()
    click.echo(f"Consumer Groups: {len(health['consumer_groups'])}")

    manager.close()


@cli.command()
@click.argument("topic_name")
@click.option(
    "--message",
    "-m",
    default='{"test": "message", "timestamp": "2024-01-01T00:00:00Z"}',
)
def test_produce(topic_name, message):
    """Send a test message to a topic."""
    manager = KafkaManager()

    click.echo(f"📤 Sending test message to {topic_name}")

    try:
        test_message = json.loads(message)
        success = manager.produce_test_message(topic_name, test_message)

        if success:
            click.echo("✅ Test message sent successfully!")
        else:
            click.echo("❌ Failed to send test message.")
    except json.JSONDecodeError:
        click.echo("❌ Invalid JSON message format.")

    manager.close()


if __name__ == "__main__":
    cli()

#!/usr/bin/env python3
"""
Demo script for reliability features in Kafka data producers.

This script demonstrates:
- Dead letter queue for failed messages
- Retry mechanisms with exponential backoff
- Message deduplication
- Health monitoring and alerting
"""

import argparse
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict

import redis

from src.data_generation import (
    DataGenerationConfig,
    ECommerceDataGenerator,
    ReliableKafkaProducer,
    RetryConfig,
    default_alert_handler,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def custom_alert_handler(alert: Dict[str, Any]) -> None:
    """Custom alert handler that logs and could send notifications."""
    severity = alert.get("severity", "unknown")
    alert_type = alert.get("type", "unknown")
    value = alert.get("value", "unknown")
    threshold = alert.get("threshold", "unknown")

    logger.warning(
        f"🚨 CUSTOM ALERT [{severity.upper()}] {alert_type}: "
        f"Value {value} exceeds threshold {threshold}"
    )

    # In a real system, you might:
    # - Send to Slack/Discord
    # - Create PagerDuty incident
    # - Send email notification
    # - Update monitoring dashboard


def demo_basic_reliability(producer: ReliableKafkaProducer) -> None:
    """Demonstrate basic reliability features."""
    logger.info("🔧 Demonstrating basic reliability features...")

    # Generate some test data
    config = DataGenerationConfig()
    generator = ECommerceDataGenerator(config)

    # Send some messages
    logger.info("📤 Sending test messages...")
    message_ids = []

    for i in range(10):
        transaction = generator.generate_transactions(1)[0]
        message_id = producer.send_message(
            topic="transactions", value=transaction, key=transaction["user_id"]
        )
        if message_id:
            message_ids.append(message_id)

        time.sleep(0.1)  # Small delay

    # Flush and wait
    producer.flush()
    time.sleep(2)

    # Show stats
    stats = producer.get_stats()
    logger.info(f"📊 Messages sent: {stats['messages_sent']}")
    logger.info(f"📊 Messages failed: {stats['messages_failed']}")
    logger.info(f"📊 Messages duplicate: {stats['messages_duplicate']}")
    logger.info(f"📊 Error rate: {stats['error_rate']:.2%}")

    if stats.get("latency_stats"):
        latency = stats["latency_stats"]
        logger.info(f"📊 Avg latency: {latency['avg_ms']:.2f}ms")
        logger.info(f"📊 P95 latency: {latency['p95_ms']:.2f}ms")


def demo_deduplication(producer: ReliableKafkaProducer) -> None:
    """Demonstrate message deduplication."""
    logger.info("🔄 Demonstrating message deduplication...")

    # Send the same message multiple times
    test_message = {
        "user_id": "test-user-123",
        "product_id": "test-product-456",
        "amount": 99.99,
        "timestamp": datetime.now().isoformat(),
    }

    logger.info("📤 Sending duplicate messages...")
    results = []

    for i in range(5):
        message_id = producer.send_message("transactions", test_message)
        results.append(message_id)
        logger.info(f"Attempt {i+1}: {'✅ Sent' if message_id else '🔄 Duplicate'}")

    # Check deduplication stats
    if producer.deduplicator:
        dedup_stats = producer.deduplicator.get_stats()
        logger.info(f"📊 Total checked: {dedup_stats['total_checked']}")
        logger.info(f"📊 Duplicates found: {dedup_stats['duplicates_found']}")
        logger.info(f"📊 Duplicate rate: {dedup_stats['duplicate_rate']:.2%}")


def demo_retry_and_dlq(producer: ReliableKafkaProducer) -> None:
    """Demonstrate retry mechanisms and dead letter queue."""
    logger.info("🔁 Demonstrating retry mechanisms and DLQ...")

    # Check DLQ before
    dlq_before = len(producer.get_dlq_messages())
    logger.info(f"📊 DLQ size before: {dlq_before}")

    # Send some messages (some might fail if Kafka is not available)
    logger.info("📤 Sending messages that might fail...")

    for i in range(5):
        message = {
            "test_id": f"retry-test-{i}",
            "data": f"test data {i}",
            "timestamp": datetime.now().isoformat(),
        }

        message_id = producer.send_message("test-retry-topic", message)
        if message_id:
            logger.info(f"Message {i}: Sent with ID {message_id}")
        else:
            logger.info(f"Message {i}: Duplicate or failed")

    # Wait for retries to complete
    time.sleep(5)

    # Check retry stats
    retry_stats = producer.retry_manager.get_stats()
    logger.info(f"📊 Total retries: {retry_stats['total_retries']}")
    logger.info(f"📊 Successful retries: {retry_stats['successful_retries']}")
    logger.info(f"📊 Failed retries: {retry_stats['failed_retries']}")

    # Check DLQ after
    dlq_after = len(producer.get_dlq_messages())
    logger.info(f"📊 DLQ size after: {dlq_after}")

    if dlq_after > dlq_before:
        logger.info("💀 Some messages ended up in DLQ")
        dlq_messages = producer.get_dlq_messages()
        for msg in dlq_messages[-3:]:  # Show last 3
            logger.info(f"💀 DLQ Message: {msg.id} - {msg.last_error}")


def demo_health_monitoring(producer: ReliableKafkaProducer) -> None:
    """Demonstrate health monitoring."""
    logger.info("❤️ Demonstrating health monitoring...")

    # Get current health status
    health = producer.get_health_status()
    logger.info(f"🏥 Health status: {health['status']}")
    logger.info(f"🏥 Timestamp: {health['timestamp']}")

    if health.get("metrics"):
        metrics = health["metrics"]
        logger.info(f"📊 Avg error rate: {metrics['avg_error_rate']:.4f}")
        logger.info(f"📊 Avg latency: {metrics['avg_latency_ms']:.2f}ms")
        logger.info(f"📊 Max DLQ size: {metrics['max_dlq_size']}")

    if health.get("issues"):
        logger.warning(f"⚠️ Health issues: {health['issues']}")
    else:
        logger.info("✅ No health issues detected")


def demo_batch_operations(producer: ReliableKafkaProducer) -> None:
    """Demonstrate batch operations."""
    logger.info("📦 Demonstrating batch operations...")

    # Generate batch of messages
    config = DataGenerationConfig()
    generator = ECommerceDataGenerator(config)

    transactions = generator.generate_transactions(20)

    # Send batch
    logger.info("📤 Sending batch of 20 transactions...")
    results = producer.send_batch(
        topic="transactions",
        messages=transactions,
        key_extractor=lambda x: x["user_id"],
    )

    logger.info(f"📊 Batch results: {results}")


def main():
    """Main demo function."""
    parser = argparse.ArgumentParser(description="Demo reliability features")
    parser.add_argument(
        "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6379/0",
        help="Redis URL for deduplication",
    )
    parser.add_argument(
        "--disable-dedup", action="store_true", help="Disable deduplication"
    )
    parser.add_argument(
        "--demo",
        choices=["basic", "deduplication", "retry", "health", "batch", "all"],
        default="all",
        help="Which demo to run",
    )

    args = parser.parse_args()

    logger.info("🚀 Starting Kafka Producer Reliability Demo")
    logger.info(f"🔧 Kafka servers: {args.kafka_servers}")
    logger.info(f"🔧 Redis URL: {args.redis_url}")
    logger.info(f"🔧 Deduplication: {'Disabled' if args.disable_dedup else 'Enabled'}")

    # Setup Redis client
    redis_client = None
    if not args.disable_dedup:
        try:
            redis_client = redis.from_url(args.redis_url)
            redis_client.ping()
            logger.info("✅ Redis connection successful")
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed: {e}, using memory cache")
            redis_client = None

    # Setup retry configuration
    retry_config = RetryConfig(
        max_retries=3,
        initial_delay_ms=100,
        max_delay_ms=5000,
        backoff_multiplier=2.0,
        jitter=True,
    )

    # Create reliable producer
    producer = ReliableKafkaProducer(
        bootstrap_servers=args.kafka_servers.split(","),
        redis_client=redis_client,
        enable_deduplication=not args.disable_dedup,
        retry_config=retry_config,
        health_check_interval=10,
    )

    # Add custom alert handler
    producer.add_health_alert_callback(custom_alert_handler)
    producer.add_health_alert_callback(default_alert_handler)

    try:
        # Run selected demos
        if args.demo in ["basic", "all"]:
            demo_basic_reliability(producer)
            time.sleep(1)

        if args.demo in ["deduplication", "all"]:
            demo_deduplication(producer)
            time.sleep(1)

        if args.demo in ["retry", "all"]:
            demo_retry_and_dlq(producer)
            time.sleep(1)

        if args.demo in ["health", "all"]:
            demo_health_monitoring(producer)
            time.sleep(1)

        if args.demo in ["batch", "all"]:
            demo_batch_operations(producer)
            time.sleep(1)

        # Final stats
        logger.info("📊 Final Statistics:")
        final_stats = producer.get_stats()

        logger.info(f"  Messages sent: {final_stats['messages_sent']}")
        logger.info(f"  Messages failed: {final_stats['messages_failed']}")
        logger.info(f"  Messages duplicated: {final_stats['messages_duplicate']}")
        logger.info(f"  Error rate: {final_stats['error_rate']:.2%}")
        logger.info(f"  DLQ size: {final_stats['dlq_stats']['total_messages']}")

        if final_stats.get("latency_stats"):
            latency = final_stats["latency_stats"]
            logger.info(f"  Average latency: {latency['avg_ms']:.2f}ms")

        logger.info("✅ Demo completed successfully!")

    except KeyboardInterrupt:
        logger.info("⏹️ Demo interrupted by user")
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise
    finally:
        logger.info("🧹 Cleaning up...")
        producer.close()
        logger.info("👋 Demo finished")


if __name__ == "__main__":
    main()

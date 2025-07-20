#!/usr/bin/env python3
"""
Demonstration script for Kafka Structured Streaming Consumers

This script demonstrates the streaming consumer functionality including:
- Setting up and starting consumers
- Health monitoring
- Real-time data processing
- Error handling and recovery

Usage:
    python scripts/demo_streaming_consumers.py --mode interactive
    python scripts/demo_streaming_consumers.py --mode automated --duration 60
"""

import argparse
import json
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from pyspark.sql import SparkSession

# Add src to path for imports
sys.path.insert(0, "src")

from streaming.consumer_manager import (
    StreamingConsumerManager,
    create_default_consumers,
)
from streaming.consumers import TransactionStreamConsumer, UserBehaviorStreamConsumer
from utils.logger import get_logger
from utils.spark_utils import create_spark_session

# Global variables for demo
manager: Optional[StreamingConsumerManager] = None
spark: Optional[SparkSession] = None
logger = get_logger("demo_streaming_consumers")


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down demo...")

    if manager:
        manager.stop_all_consumers()

    if spark:
        spark.stop()

    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def print_banner():
    """Print demo banner."""
    print("=" * 70)
    print("🚀 KAFKA STRUCTURED STREAMING CONSUMERS DEMO")
    print("   E-Commerce Analytics Platform")
    print("=" * 70)
    print()


def print_section(title: str):
    """Print section header."""
    print(f"\n{'='*20} {title} {'='*20}")


def create_demo_spark_session() -> SparkSession:
    """Create Spark session optimized for demo."""
    return create_spark_session(
        app_name="StreamingConsumersDemo",
        master="spark://spark-master:7077",
        enable_hive_support=False,
        additional_configs={
            "spark.sql.streaming.checkpointLocation": "/tmp/demo_checkpoints",
            "spark.sql.streaming.kafka.consumer.pollTimeoutMs": "512",
            "spark.sql.shuffle.partitions": "4",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.streaming.metricsEnabled": "true",
            "spark.sql.streaming.ui.enabled": "true",
        },
    )


def setup_consumer_manager() -> StreamingConsumerManager:
    """Set up the consumer manager with default consumers."""
    global spark

    print_section("SETTING UP CONSUMER MANAGER")

    # Create Spark session
    print("🔧 Creating Spark session...")
    spark = create_demo_spark_session()
    print(f"✅ Spark session created: {spark.sparkContext.appName}")

    # Create consumer manager
    print("🔧 Creating consumer manager...")
    consumer_manager = StreamingConsumerManager(
        spark=spark,
        kafka_bootstrap_servers="localhost:9092",
        base_checkpoint_location="/tmp/demo_checkpoints",
        max_concurrent_consumers=4,
    )
    print("✅ Consumer manager created")

    # Set up default consumers
    print("🔧 Registering default consumers...")
    create_default_consumers(consumer_manager)
    print("✅ Default consumers registered")

    # Display registered consumers
    consumers = consumer_manager.consumers
    print(f"\n📊 Registered Consumers ({len(consumers)}):")
    print("-" * 50)

    for name, consumer in consumers.items():
        print(f"• {name}:")
        print(f"  Topic: {consumer.topic}")
        print(f"  Consumer Group: {consumer.consumer_group}")
        print(f"  Checkpoint: {consumer.checkpoint_location}")
        print()

    return consumer_manager


def start_consumers(manager: StreamingConsumerManager):
    """Start all consumers."""
    print_section("STARTING CONSUMERS")

    print("🚀 Starting all consumers...")
    try:
        manager.start_all_consumers(
            output_mode="append",
            trigger_interval="5 seconds",
            output_format="console",
        )
        print("✅ All consumers started successfully!")

        # Wait a moment for consumers to initialize
        time.sleep(3)

        # Display initial status
        print("\n📊 Initial Consumer Status:")
        display_consumer_status(manager)

    except Exception as e:
        logger.error(f"Failed to start consumers: {e}")
        raise


def display_consumer_status(manager: StreamingConsumerManager):
    """Display current consumer status."""
    status = manager.get_all_consumer_status()

    # Manager status
    manager_status = status["manager_status"]
    print(f"   Active Consumers: {manager_status['active_consumers']}")
    print(f"   Registered Consumers: {manager_status['registered_consumers']}")

    # Health summary
    health = status["health_summary"]
    health_icon = "🟢" if health["overall_status"] == "healthy" else "🟡"
    print(f"   Overall Health: {health_icon} {health['overall_status'].upper()}")
    print(
        f"   Healthy/Total: {health['healthy_consumers']}/{health['total_consumers']}"
    )

    # Individual consumers
    consumers = status["consumers"]
    if consumers:
        print("\n   Individual Consumer Status:")
        for consumer_name, consumer_status in consumers.items():
            stream_status = consumer_status.get("stream_status", {})
            status_icon = "🟢" if stream_status.get("status") == "active" else "🔴"
            print(
                f"   {status_icon} {consumer_name}: {stream_status.get('status', 'unknown')}"
            )

            # Show processing rates if available
            if stream_status.get("input_rows_per_second") is not None:
                input_rate = stream_status.get("input_rows_per_second", 0)
                processed_rate = stream_status.get("processed_rows_per_second", 0)
                print(f"      Input Rate: {input_rate:.2f} rows/sec")
                print(f"      Processed Rate: {processed_rate:.2f} rows/sec")


def monitor_consumers(manager: StreamingConsumerManager, duration: int = 60):
    """Monitor consumers for specified duration."""
    print_section("MONITORING CONSUMERS")

    print(f"📊 Monitoring consumers for {duration} seconds...")
    print("   (Press Ctrl+C to stop early)")
    print()

    start_time = time.time()
    iteration = 0

    try:
        while time.time() - start_time < duration:
            iteration += 1
            current_time = time.strftime("%H:%M:%S")

            print(f"📊 Monitor Update #{iteration} - {current_time}")
            print("-" * 50)

            display_consumer_status(manager)

            # Check for any health issues
            health_summary = manager.health_monitor.get_health_summary()
            if health_summary["overall_status"] != "healthy":
                print("\n⚠️  Health Issues Detected:")
                for consumer_name, health_data in health_summary[
                    "consumer_details"
                ].items():
                    if health_data.get("status") != "healthy":
                        print(
                            f"   • {consumer_name}: {health_data.get('status', 'unknown')}"
                        )
                        if "last_error" in health_data:
                            print(f"     Error: {health_data['last_error']}")

            print("\n" + "=" * 50)

            # Wait before next iteration
            time.sleep(10)

        print(f"✅ Monitoring completed after {duration} seconds")

    except KeyboardInterrupt:
        print("\n⏹️  Monitoring stopped by user")


def demonstrate_error_recovery(manager: StreamingConsumerManager):
    """Demonstrate error handling and recovery."""
    print_section("ERROR RECOVERY DEMONSTRATION")

    print("🔧 Demonstrating error recovery capabilities...")

    # Get a consumer to test with
    consumer_names = list(manager.consumers.keys())
    if not consumer_names:
        print("❌ No consumers available for error recovery demo")
        return

    test_consumer = consumer_names[0]
    print(f"🎯 Using consumer: {test_consumer}")

    # Stop consumer to simulate failure
    print("🛑 Stopping consumer to simulate failure...")
    manager.stop_consumer(test_consumer)
    time.sleep(2)

    # Check status
    print("📊 Status after simulated failure:")
    display_consumer_status(manager)

    # Restart consumer
    print("🔄 Restarting consumer...")
    manager.start_consumer(
        test_consumer,
        output_mode="append",
        trigger_interval="5 seconds",
        output_format="console",
    )
    time.sleep(3)

    # Check recovery
    print("📊 Status after recovery:")
    display_consumer_status(manager)

    print("✅ Error recovery demonstration completed")


def interactive_demo():
    """Run interactive demonstration."""
    global manager

    print_banner()
    print("🎮 Interactive Demo Mode")
    print("   Follow the prompts to explore streaming consumer features")
    print()

    try:
        # Setup
        manager = setup_consumer_manager()

        # Start consumers
        start_consumers(manager)

        # Interactive menu
        while True:
            print("\n" + "=" * 50)
            print("🎮 INTERACTIVE MENU")
            print("=" * 50)
            print("1. Show consumer status")
            print("2. Monitor consumers (30 seconds)")
            print("3. Demonstrate error recovery")
            print("4. Show health summary")
            print("5. Stop all consumers")
            print("6. Exit demo")

            choice = input("\nEnter your choice (1-6): ").strip()

            if choice == "1":
                print_section("CONSUMER STATUS")
                display_consumer_status(manager)

            elif choice == "2":
                monitor_consumers(manager, duration=30)

            elif choice == "3":
                demonstrate_error_recovery(manager)

            elif choice == "4":
                print_section("HEALTH SUMMARY")
                health_summary = manager.health_monitor.get_health_summary()
                print(json.dumps(health_summary, indent=2))

            elif choice == "5":
                print_section("STOPPING CONSUMERS")
                print("🛑 Stopping all consumers...")
                manager.stop_all_consumers()
                print("✅ All consumers stopped")

            elif choice == "6":
                break

            else:
                print("❌ Invalid choice. Please enter 1-6.")

        print("\n👋 Exiting interactive demo...")

    except Exception as e:
        logger.error(f"Error in interactive demo: {e}")
        raise

    finally:
        # Cleanup
        if manager:
            manager.stop_all_consumers()


def automated_demo(duration: int = 120):
    """Run automated demonstration."""
    global manager

    print_banner()
    print("🤖 Automated Demo Mode")
    print(f"   Running demonstration for {duration} seconds")
    print()

    try:
        # Setup
        manager = setup_consumer_manager()

        # Start consumers
        start_consumers(manager)

        # Monitor for half the duration
        monitor_duration = duration // 2
        monitor_consumers(manager, duration=monitor_duration)

        # Demonstrate error recovery
        demonstrate_error_recovery(manager)

        # Monitor for remaining time
        remaining_time = (
            duration - monitor_duration - 20
        )  # Account for error recovery time
        if remaining_time > 0:
            print(f"\n🔄 Continuing monitoring for {remaining_time} more seconds...")
            monitor_consumers(manager, duration=remaining_time)

        # Final status
        print_section("FINAL STATUS")
        display_consumer_status(manager)

        print("\n✅ Automated demo completed successfully!")

    except Exception as e:
        logger.error(f"Error in automated demo: {e}")
        raise

    finally:
        # Cleanup
        if manager:
            print("\n🧹 Cleaning up...")
            manager.stop_all_consumers()
            print("✅ Cleanup completed")


def check_prerequisites():
    """Check if prerequisites are met for the demo."""
    print_section("CHECKING PREREQUISITES")

    # Check if Kafka topics exist
    print("🔍 Checking Kafka topics...")
    try:
        from data_ingestion.monitoring import KafkaHealthChecker

        health_checker = KafkaHealthChecker("localhost:9092")
        health_status = health_checker.check_health()

        if health_status["kafka_accessible"]:
            print("✅ Kafka is accessible")

            required_topics = ["transactions", "user-events"]
            missing_topics = []

            for topic in required_topics:
                if topic not in health_status.get("topics", []):
                    missing_topics.append(topic)

            if missing_topics:
                print(f"⚠️  Missing topics: {missing_topics}")
                print(
                    "   Run 'python scripts/manage_kafka.py create-topics' to create them"
                )
                return False
            else:
                print("✅ All required topics exist")
        else:
            print("❌ Kafka is not accessible")
            print("   Make sure Kafka is running: docker-compose up -d")
            return False

    except Exception as e:
        print(f"⚠️  Could not check Kafka status: {e}")
        print("   Demo will continue but may fail if Kafka is not available")

    # Check Spark
    print("🔍 Checking Spark availability...")
    try:
        test_spark = create_demo_spark_session()
        test_spark.stop()
        print("✅ Spark is available")
    except Exception as e:
        print(f"❌ Spark is not available: {e}")
        print("   Make sure Spark is running: docker-compose up -d")
        return False

    print("✅ All prerequisites met")
    return True


def main():
    """Main demo function."""
    parser = argparse.ArgumentParser(description="Streaming Consumers Demo")
    parser.add_argument(
        "--mode",
        choices=["interactive", "automated"],
        default="interactive",
        help="Demo mode (default: interactive)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Duration for automated mode in seconds (default: 120)",
    )
    parser.add_argument(
        "--skip-prerequisites", action="store_true", help="Skip prerequisite checks"
    )

    args = parser.parse_args()

    try:
        # Check prerequisites
        if not args.skip_prerequisites:
            if not check_prerequisites():
                print(
                    "\n❌ Prerequisites not met. Use --skip-prerequisites to continue anyway."
                )
                sys.exit(1)

        # Run appropriate demo mode
        if args.mode == "interactive":
            interactive_demo()
        else:
            automated_demo(args.duration)

    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        print(f"\n❌ Demo failed: {e}")
        sys.exit(1)
    finally:
        # Final cleanup
        if manager:
            manager.stop_all_consumers()
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()

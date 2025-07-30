"""
CLI for managing Kafka Structured Streaming consumers.

This module provides command-line interface for starting, stopping, and monitoring
streaming consumers in the e-commerce analytics platform.
"""

import json
import signal
import sys
import time
from typing import Optional

import click

from src.utils.logger import get_logger
from src.utils.spark_utils import create_spark_session, get_secure_temp_dir

from .consumer_manager import StreamingConsumerManager, create_default_consumers
from .consumers import TransactionStreamConsumer, UserBehaviorStreamConsumer

# Global manager instance for signal handling
_manager: Optional[StreamingConsumerManager] = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger = get_logger("consumer_cli")
    logger.info(f"Received signal {signum}, shutting down consumers...")

    if _manager:
        _manager.stop_all_consumers()

    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@click.group()
@click.option(
    "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
)
@click.option(
    "--checkpoint-dir",
    default=None,
    help="Checkpoint directory",
)
@click.option("--max-consumers", default=4, help="Maximum concurrent consumers")
@click.option(
    "--spark-master", default="spark://spark-master:7077", help="Spark master URL"
)
@click.option("--log-level", default="INFO", help="Log level")
@click.pass_context
def cli(ctx, kafka_servers, checkpoint_dir, max_consumers, spark_master, log_level):
    """Kafka Streaming Consumer Management CLI."""
    global _manager

    # Ensure ctx.obj is a dict
    ctx.ensure_object(dict)

    # Use secure temp directory if checkpoint_dir not provided
    if checkpoint_dir is None:
        checkpoint_dir = get_secure_temp_dir("streaming_checkpoints")

    # Create Spark session
    spark = create_spark_session(
        app_name="StreamingConsumerCLI",
        master=spark_master,
        enable_hive_support=False,
        additional_configs={
            "spark.sql.streaming.checkpointLocation": checkpoint_dir,
            "spark.sql.streaming.kafka.consumer.pollTimeoutMs": "512",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        },
    )

    # Create consumer manager
    manager = StreamingConsumerManager(
        spark=spark,
        kafka_bootstrap_servers=kafka_servers,
        base_checkpoint_location=checkpoint_dir,
        max_concurrent_consumers=max_consumers,
    )

    _manager = manager

    # Store in context
    ctx.obj["manager"] = manager
    ctx.obj["spark"] = spark
    ctx.obj["kafka_servers"] = kafka_servers


@cli.command()
@click.pass_context
def setup_default_consumers(ctx):
    """Set up default consumers for the e-commerce platform."""
    manager = ctx.obj["manager"]

    click.echo("🚀 Setting up default streaming consumers...")

    try:
        create_default_consumers(manager)
        click.echo("✅ Default consumers registered successfully!")

        # Display registered consumers
        consumers = manager.consumers
        click.echo(f"\n📊 Registered Consumers ({len(consumers)}):")
        click.echo("-" * 50)

        for name, consumer in consumers.items():
            click.echo(f"• {name}:")
            click.echo(f"  Topic: {consumer.topic}")
            click.echo(f"  Consumer Group: {consumer.consumer_group}")
            click.echo(f"  Checkpoint: {consumer.checkpoint_location}")
            click.echo()

    except Exception as e:
        click.echo(f"❌ Failed to set up consumers: {e}")
        sys.exit(1)


@cli.command()
@click.option("--name", required=True, help="Consumer name")
@click.option("--topic", required=True, help="Kafka topic")
@click.option("--consumer-group", required=True, help="Consumer group ID")
@click.option(
    "--consumer-type",
    type=click.Choice(["transaction", "user_behavior"]),
    required=True,
    help="Consumer type",
)
@click.option("--max-offsets", default=1000, help="Max offsets per trigger")
@click.pass_context
def register_consumer(ctx, name, topic, consumer_group, consumer_type, max_offsets):
    """Register a new streaming consumer."""
    manager = ctx.obj["manager"]

    consumer_classes = {
        "transaction": TransactionStreamConsumer,
        "user_behavior": UserBehaviorStreamConsumer,
    }

    try:
        manager.register_consumer(
            name=name,
            consumer_class=consumer_classes[consumer_type],
            topic=topic,
            consumer_group=consumer_group,
            max_offsets_per_trigger=max_offsets,
            enable_backpressure=True,
        )

        click.echo(f"✅ Registered consumer: {name}")
        click.echo(f"   Type: {consumer_type}")
        click.echo(f"   Topic: {topic}")
        click.echo(f"   Consumer Group: {consumer_group}")

    except Exception as e:
        click.echo(f"❌ Failed to register consumer: {e}")
        sys.exit(1)


@cli.command()
@click.option("--name", help="Specific consumer name to start")
@click.option(
    "--output-mode",
    default="append",
    type=click.Choice(["append", "update", "complete"]),
    help="Streaming output mode",
)
@click.option("--trigger-interval", default="5 seconds", help="Trigger interval")
@click.option("--output-format", default="console", help="Output format")
@click.pass_context
def start_consumers(ctx, name, output_mode, trigger_interval, output_format):
    """Start streaming consumers."""
    manager = ctx.obj["manager"]

    if name:
        # Start specific consumer
        click.echo(f"🚀 Starting consumer: {name}")
        try:
            manager.start_consumer(
                name=name,
                output_mode=output_mode,
                trigger_interval=trigger_interval,
                output_format=output_format,
            )
            click.echo(f"✅ Consumer {name} started successfully!")
        except Exception as e:
            click.echo(f"❌ Failed to start consumer {name}: {e}")
            sys.exit(1)
    else:
        # Start all consumers
        click.echo("🚀 Starting all registered consumers...")
        try:
            manager.start_all_consumers(
                output_mode=output_mode,
                trigger_interval=trigger_interval,
                output_format=output_format,
            )
            click.echo("✅ All consumers started successfully!")
        except Exception as e:
            click.echo(f"❌ Failed to start consumers: {e}")
            sys.exit(1)

    # Show status
    status = manager.get_all_consumer_status()
    active_consumers = status["manager_status"]["active_consumers"]
    click.echo(f"\n📊 Active consumers: {active_consumers}")


@cli.command()
@click.option("--name", help="Specific consumer name to stop")
@click.pass_context
def stop_consumers(ctx, name):
    """Stop streaming consumers."""
    manager = ctx.obj["manager"]

    if name:
        # Stop specific consumer
        click.echo(f"🛑 Stopping consumer: {name}")
        manager.stop_consumer(name)
        click.echo(f"✅ Consumer {name} stopped")
    else:
        # Stop all consumers
        click.echo("🛑 Stopping all consumers...")
        manager.stop_all_consumers()
        click.echo("✅ All consumers stopped")


@cli.command()
@click.option("--name", help="Specific consumer name to check")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
@click.pass_context
def status(ctx, name, output_format):
    """Show consumer status."""
    manager = ctx.obj["manager"]

    if name:
        # Show specific consumer status
        consumer_status = manager.get_consumer_status(name)

        if output_format == "json":
            click.echo(json.dumps(consumer_status, indent=2))
        else:
            click.echo(f"📊 Consumer Status: {name}")
            click.echo("=" * 50)
            click.echo(f"Future Status: {consumer_status.get('future_status', 'N/A')}")
            click.echo(
                f"Stream Status: {consumer_status.get('stream_status', {}).get('status', 'N/A')}"
            )
            click.echo(f"Topic: {consumer_status.get('topic', 'N/A')}")
            click.echo(
                f"Consumer Group: {consumer_status.get('consumer_group', 'N/A')}"
            )

            stream_status = consumer_status.get("stream_status", {})
            if stream_status.get("input_rows_per_second") is not None:
                click.echo(
                    f"Input Rate: {stream_status.get('input_rows_per_second', 0):.2f} rows/sec"
                )
                click.echo(
                    f"Processed Rate: {stream_status.get('processed_rows_per_second', 0):.2f} rows/sec"
                )
    else:
        # Show all consumers status
        all_status = manager.get_all_consumer_status()

        if output_format == "json":
            click.echo(json.dumps(all_status, indent=2))
        else:
            # Manager status
            manager_status = all_status["manager_status"]
            click.echo("📊 Consumer Manager Status")
            click.echo("=" * 50)
            click.echo(f"Active Consumers: {manager_status['active_consumers']}")
            click.echo(
                f"Registered Consumers: {manager_status['registered_consumers']}"
            )
            click.echo(f"Shutdown Requested: {manager_status['shutdown_requested']}")

            # Health summary
            health = all_status["health_summary"]
            click.echo("\n🏥 Health Summary")
            click.echo("-" * 30)
            click.echo(f"Overall Status: {health['overall_status'].upper()}")
            click.echo(
                f"Healthy: {health['healthy_consumers']}/{health['total_consumers']}"
            )
            click.echo(f"Monitoring Active: {health['monitoring_active']}")

            # Individual consumers
            consumers = all_status["consumers"]
            if consumers:
                click.echo("\n📋 Individual Consumer Status")
                click.echo("-" * 50)
                for consumer_name, consumer_status in consumers.items():
                    stream_status = consumer_status.get("stream_status", {})
                    status_indicator = (
                        "🟢" if stream_status.get("status") == "active" else "🔴"
                    )
                    click.echo(
                        f"{status_indicator} {consumer_name}: {stream_status.get('status', 'unknown')}"
                    )


@cli.command()
@click.option("--interval", default=10, help="Refresh interval in seconds")
@click.option("--count", default=0, help="Number of updates (0 for infinite)")
@click.pass_context
def monitor(ctx, interval, count):
    """Monitor streaming consumers in real-time."""
    manager = ctx.obj["manager"]

    click.echo("📊 Real-time Consumer Monitoring")
    click.echo("Press Ctrl+C to stop")
    click.echo("=" * 60)

    iteration = 0
    try:
        while count == 0 or iteration < count:
            # Clear screen (simple approach)
            click.clear()
            click.echo(f"📊 Consumer Monitoring - Update #{iteration + 1}")
            click.echo(f"⏰ {time.strftime('%Y-%m-%d %H:%M:%S')}")
            click.echo("=" * 60)

            # Get and display status
            all_status = manager.get_all_consumer_status()

            # Health summary
            health = all_status["health_summary"]
            status_color = "🟢" if health["overall_status"] == "healthy" else "🟡"
            click.echo(
                f"{status_color} Overall Status: {health['overall_status'].upper()}"
            )
            click.echo(
                f"   Healthy Consumers: {health['healthy_consumers']}/{health['total_consumers']}"
            )
            click.echo()

            # Consumer details
            consumers = all_status["consumers"]
            for consumer_name, consumer_status in consumers.items():
                stream_status = consumer_status.get("stream_status", {})
                status_indicator = (
                    "🟢" if stream_status.get("status") == "active" else "🔴"
                )

                click.echo(f"{status_indicator} {consumer_name}")
                click.echo(f"   Status: {stream_status.get('status', 'unknown')}")

                if stream_status.get("input_rows_per_second") is not None:
                    input_rate = stream_status.get("input_rows_per_second", 0)
                    processed_rate = stream_status.get("processed_rows_per_second", 0)
                    click.echo(f"   Input Rate: {input_rate:.2f} rows/sec")
                    click.echo(f"   Processed Rate: {processed_rate:.2f} rows/sec")

                    if input_rate > 0:
                        processing_ratio = processed_rate / input_rate
                        ratio_indicator = (
                            "🟢"
                            if processing_ratio >= 0.9
                            else "🟡"
                            if processing_ratio >= 0.7
                            else "🔴"
                        )
                        click.echo(
                            f"   Processing Ratio: {ratio_indicator} {processing_ratio:.2%}"
                        )

                click.echo()

            iteration += 1
            if count == 0 or iteration < count:
                time.sleep(interval)

    except KeyboardInterrupt:
        click.echo("\n👋 Monitoring stopped by user")


@cli.command()
@click.option("--setup-defaults", is_flag=True, help="Set up default consumers first")
@click.option("--output-mode", default="append", help="Streaming output mode")
@click.option("--trigger-interval", default="5 seconds", help="Trigger interval")
@click.pass_context
def run(ctx, setup_defaults, output_mode, trigger_interval):
    """Run streaming consumers (setup, start, and monitor)."""
    manager = ctx.obj["manager"]

    try:
        if setup_defaults:
            click.echo("🚀 Setting up default consumers...")
            create_default_consumers(manager)
            click.echo("✅ Default consumers registered")

        # Check if we have any consumers
        if not manager.consumers:
            click.echo(
                "❌ No consumers registered. Use --setup-defaults or register consumers first."
            )
            sys.exit(1)

        # Start all consumers
        click.echo("🚀 Starting all consumers...")
        manager.start_all_consumers(
            output_mode=output_mode,
            trigger_interval=trigger_interval,
            output_format="console",
        )
        click.echo("✅ All consumers started")

        # Show initial status
        click.echo("\n📊 Initial Status:")
        status = manager.get_all_consumer_status()
        for name, consumer_status in status["consumers"].items():
            stream_status = consumer_status.get("stream_status", {})
            click.echo(f"   • {name}: {stream_status.get('status', 'unknown')}")

        click.echo("\n🔄 Streaming is now active. Press Ctrl+C to stop.")

        # Wait for termination
        manager.wait_for_all_consumers()

    except KeyboardInterrupt:
        click.echo("\n🛑 Received interrupt signal, stopping consumers...")
        manager.stop_all_consumers()
        click.echo("✅ All consumers stopped gracefully")
    except Exception as e:
        click.echo(f"❌ Error during execution: {e}")
        manager.stop_all_consumers()
        sys.exit(1)


if __name__ == "__main__":
    cli()

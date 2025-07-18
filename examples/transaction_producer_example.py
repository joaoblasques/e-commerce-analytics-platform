#!/usr/bin/env python3
"""
Example script for running the transaction data producer.

This script demonstrates how to use the TransactionProducer to generate
realistic e-commerce transaction data and send it to a Kafka topic.
"""

import logging
import time

from src.data_ingestion.monitoring import monitor
from src.data_ingestion.producers.transaction_producer import TransactionProducer

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    """Run the transaction producer example."""
    logger.info("Starting transaction producer example...")

    # Configuration
    config = {
        "bootstrap_servers": "localhost:9092",
        "topic": "transactions",
        "generation_rate": 500.0,  # 500 transactions per hour
    }

    # Create and configure producer
    producer = TransactionProducer(
        bootstrap_servers=config["bootstrap_servers"],
        topic=config["topic"],
        generation_rate=config["generation_rate"],
    )

    # Register with monitoring
    metrics = monitor.register_producer("transaction_producer_example")

    try:
        logger.info(f"Producer configuration: {config}")
        logger.info("Press Ctrl+C to stop the producer...")

        # Run for demonstration (or until interrupted)
        start_time = time.time()
        message_count = 0

        while True:
            # Generate and send a message
            message = producer.generate_message()
            key = producer.get_message_key(message)

            success = producer.send_message(message, key)

            if success:
                message_count += 1
                metrics.add_success(len(str(message)))

                # Log every 10th message
                if message_count % 10 == 0:
                    logger.info(f"Sent {message_count} messages")

                    # Show sample message
                    if message_count == 10:
                        logger.info(f"Sample message: {message}")

            else:
                metrics.add_failure("Failed to send message")

            # Calculate delay based on generation rate
            delay = producer.get_delay_seconds()
            time.sleep(delay)

            # Log metrics every minute
            if message_count > 0 and message_count % 50 == 0:
                elapsed = time.time() - start_time
                rate = message_count / elapsed
                logger.info(f"Current rate: {rate:.2f} messages/sec")

    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception as e:
        logger.error(f"Producer error: {e}")
    finally:
        # Clean up
        producer.flush()
        producer.close()

        # Final metrics
        final_metrics = producer.get_metrics()
        logger.info(f"Final producer metrics: {final_metrics}")

        monitor_metrics = monitor.get_producer_metrics("transaction_producer_example")
        if monitor_metrics:
            logger.info(f"Monitor metrics: {monitor_metrics.to_dict()}")


if __name__ == "__main__":
    main()

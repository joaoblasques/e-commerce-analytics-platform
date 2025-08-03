#!/usr/bin/env python3
"""
Test data verification script for the 5-minute setup.

This script verifies that the generated test data is available in Kafka topics
and provides feedback on data availability for streaming consumer setup.
"""

import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer


def main() -> int:
    """Verify test data availability in Kafka topics."""
    print("🔍 Verifying test data availability...")

    topics = ["transactions", "user-events", "product-updates"]
    total_messages = 0

    try:
        for topic in topics:
            # Create consumer for each topic
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=["localhost:9092"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
                auto_offset_reset="earliest",
            )

            # Count messages in topic
            count = sum(1 for _ in consumer)
            consumer.close()

            total_messages += count
            print(f"✅ {topic}: {count} messages")

        # Provide feedback on data availability
        print(f"📊 Total messages available: {total_messages}")

        if total_messages >= 10:
            print("🚀 Ready to start streaming consumers!")
            return 0
        else:
            print("⚠️  Consider generating more data for full demo")
            return 1

    except Exception as e:
        print(f"❌ Error verifying test data: {e}")
        print("💡 Make sure Kafka is running and topics exist")
        return 1


if __name__ == "__main__":
    sys.exit(main())

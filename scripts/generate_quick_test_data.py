#!/usr/bin/env python3
"""
Quick test data generation script for the 5-minute setup.

This script generates minimal test data (13 messages) across 3 Kafka topics
for quick platform testing and verification.
"""

import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaProducer

from src.data_generation.config import DataGenerationConfig
from src.data_generation.generator import ECommerceDataGenerator


def main():
    """Generate minimal test data for quick platform verification."""
    print("🚀 Generating minimal test data...")

    try:
        # Create minimal config and generator
        config = DataGenerationConfig()
        config.num_products = 10
        config.num_users = 10
        generator = ECommerceDataGenerator(config)

        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        )

        # Generate minimal test data
        print("📊 Generating 5 transactions...")
        transactions = generator.generate_transactions(5)
        for tx in transactions:
            producer.send("transactions", key=tx["user_id"], value=tx)

        print("📱 Generating 5 user events...")
        events = generator.generate_user_events(5)
        for event in events:
            producer.send("user-events", key=event["session_id"], value=event)

        print("📦 Generating 3 product updates...")
        updates = generator.generate_product_updates(3)
        for update in updates:
            producer.send("product-updates", key=update["product_id"], value=update)

        producer.flush()
        producer.close()

        print("✅ Generated 13 test messages across 3 topics")
        print("🎯 Ready for streaming consumer setup!")

    except Exception as e:
        print(f"❌ Error generating test data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

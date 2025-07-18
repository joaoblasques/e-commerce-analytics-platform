"""Command line interface for user behavior event producer."""

import argparse
import signal
import sys
import time

from .user_behavior_producer import UserBehaviorProducer


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    print(f"\nReceived signal {signum}. Shutting down...")
    sys.exit(0)


def main():
    """Main CLI function for user behavior producer."""
    parser = argparse.ArgumentParser(
        description="User Behavior Event Producer for E-commerce Analytics Platform"
    )

    # Kafka configuration
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic", default="user-events", help="Kafka topic name (default: user-events)"
    )

    # Producer configuration
    parser.add_argument(
        "--rate",
        type=float,
        default=5000.0,
        help="Events generation rate per hour (default: 5000)",
    )
    parser.add_argument(
        "--duration", type=int, help="Duration to run in seconds (default: infinite)"
    )
    parser.add_argument(
        "--min-session-duration",
        type=int,
        default=60,
        help="Minimum session duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--max-session-duration",
        type=int,
        default=1800,
        help="Maximum session duration in seconds (default: 1800)",
    )

    # Output configuration
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress all output except errors"
    )

    args = parser.parse_args()

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Validate arguments
    if args.rate <= 0:
        print("Error: Generation rate must be positive")
        sys.exit(1)

    if args.min_session_duration >= args.max_session_duration:
        print("Error: Minimum session duration must be less than maximum")
        sys.exit(1)

    # Create producer configuration
    producer_config = {
        "acks": "1",
        "retries": 3,
        "batch_size": 16384,
        "linger_ms": 10,
        "buffer_memory": 33554432,
        "compression_type": "lz4",
    }

    # Create and configure producer
    try:
        producer = UserBehaviorProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            producer_config=producer_config,
            generation_rate=args.rate,
            session_duration_range=(
                args.min_session_duration,
                args.max_session_duration,
            ),
        )

        # Configure logging level
        if args.quiet:
            producer.logger.setLevel("ERROR")
        elif args.verbose:
            producer.logger.setLevel("DEBUG")
        else:
            producer.logger.setLevel("INFO")

        # Print startup information
        if not args.quiet:
            print("🚀 Starting User Behavior Event Producer")
            print(f"📡 Kafka Servers: {args.bootstrap_servers}")
            print(f"📝 Topic: {args.topic}")
            print(f"⚡ Generation Rate: {args.rate:,.0f} events/hour")
            print(
                f"⏱️  Session Duration: {args.min_session_duration}-{args.max_session_duration} seconds"
            )
            if args.duration:
                print(f"⏰ Duration: {args.duration} seconds")
            else:
                print("⏰ Duration: Infinite (until interrupted)")
            print(f"🔧 Producer Config: {producer_config}")
            print("\nPress Ctrl+C to stop the producer\n")

        # Run producer
        start_time = time.time()
        producer.run_continuous(duration_seconds=args.duration)

        # Print final statistics
        if not args.quiet:
            end_time = time.time()
            duration = end_time - start_time
            metrics = producer.get_metrics()

            print("\n📊 Final Statistics:")
            print(f"   ⏱️  Total Runtime: {duration:.2f} seconds")
            print(f"   ✅ Messages Sent: {metrics['messages_sent']:,}")
            print(f"   ❌ Messages Failed: {metrics['messages_failed']:,}")
            print(f"   📤 Bytes Sent: {metrics['bytes_sent']:,}")
            print(f"   📈 Success Rate: {metrics['success_rate']:.2f}%")
            print(
                f"   🏃 Average Rate: {metrics['messages_sent'] / duration * 3600:.0f} events/hour"
            )
            print(f"   💾 Active Sessions: {len(producer.active_sessions)}")

    except KeyboardInterrupt:
        if not args.quiet:
            print("\n⏹️  Producer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

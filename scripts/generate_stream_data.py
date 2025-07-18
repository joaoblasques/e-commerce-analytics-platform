#!/usr/bin/env python3
"""
Data Generation CLI for E-Commerce Analytics Platform

This script provides command-line interface for generating realistic e-commerce data
and streaming it to Kafka topics.

Usage:
    python generate_stream_data.py generate --type transactions --count 1000
    python generate_stream_data.py stream --scenario peak_traffic --duration 1.0
    python generate_stream_data.py historical --days 30
"""

import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import click
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_generation import (
    ECommerceDataGenerator,
    KafkaDataProducer,
    DataGenerationConfig,
    DataGenerationOrchestrator
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), 
              help='Path to configuration file (JSON or YAML)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config, verbose):
    """E-Commerce Data Generation CLI."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration
    if config:
        config_path = Path(config)
        if config_path.suffix.lower() in ['.yaml', '.yml']:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
        else:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
        
        data_config = DataGenerationConfig(**config_data)
    else:
        data_config = DataGenerationConfig()
    
    # Validate configuration
    errors = data_config.validate()
    if errors:
        click.echo("Configuration errors:", err=True)
        for error in errors:
            click.echo(f"  - {error}", err=True)
        sys.exit(1)
    
    ctx.ensure_object(dict)
    ctx.obj['config'] = data_config


@cli.command()
@click.option('--type', '-t', 'data_type', 
              type=click.Choice(['transactions', 'user-events', 'product-updates', 'all']),
              required=True, help='Type of data to generate')
@click.option('--count', '-n', type=int, default=1000, 
              help='Number of records to generate')
@click.option('--output', '-o', type=click.Path(), 
              help='Output file path (JSON format)')
@click.option('--start-date', type=click.DateTime(), 
              help='Start date for data generation (YYYY-MM-DD)')
@click.option('--end-date', type=click.DateTime(), 
              help='End date for data generation (YYYY-MM-DD)')
@click.option('--pretty', is_flag=True, 
              help='Pretty-print JSON output')
@click.pass_context
def generate(ctx, data_type, count, output, start_date, end_date, pretty):
    """Generate data locally (no Kafka streaming)."""
    config = ctx.obj['config']
    
    click.echo(f"🚀 Generating {count} {data_type} records...")
    
    # Initialize generator
    generator = ECommerceDataGenerator(config)
    
    # Set time range
    if start_date and end_date:
        start_time = start_date
        end_time = end_date
    else:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
    
    # Generate data
    data = []
    
    if data_type == 'transactions' or data_type == 'all':
        click.echo("Generating transactions...")
        transactions = generator.generate_transactions(
            count if data_type == 'transactions' else count // 3,
            start_time, end_time
        )
        data.extend(transactions)
    
    if data_type == 'user-events' or data_type == 'all':
        click.echo("Generating user events...")
        events = generator.generate_user_events(
            count if data_type == 'user-events' else count // 3,
            start_time, end_time
        )
        data.extend(events)
    
    if data_type == 'product-updates' or data_type == 'all':
        click.echo("Generating product updates...")
        updates = generator.generate_product_updates(
            count if data_type == 'product-updates' else count // 3
        )
        data.extend(updates)
    
    # Output results
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2 if pretty else None, default=str)
        
        click.echo(f"✅ Data written to {output_path}")
        click.echo(f"📊 Generated {len(data)} records")
    else:
        # Print to stdout
        json_output = json.dumps(data, indent=2 if pretty else None, default=str)
        click.echo(json_output)
    
    # Show statistics
    stats = generator.get_statistics()
    click.echo(f"\n📈 Generation Statistics:")
    click.echo(f"  Total users: {stats['total_users']}")
    click.echo(f"  Active users: {stats['active_users']}")
    click.echo(f"  Total products: {stats['total_products']}")
    click.echo(f"  Categories: {len(stats['categories'])}")


@cli.command()
@click.option('--scenario', '-s', 
              type=click.Choice(['normal_traffic', 'peak_traffic', 'fraud_testing', 'maintenance_mode']),
              default='normal_traffic', help='Streaming scenario')
@click.option('--duration', '-d', type=float, default=1.0, 
              help='Duration in hours')
@click.option('--topics', '-t', multiple=True, 
              help='Specific topics to stream (can be used multiple times)')
@click.option('--rate-multiplier', '-r', type=float, default=1.0, 
              help='Rate multiplier for streaming')
@click.option('--dry-run', is_flag=True, 
              help='Show what would be done without actually streaming')
@click.pass_context
def stream(ctx, scenario, duration, topics, rate_multiplier, dry_run):
    """Stream data to Kafka topics."""
    config = ctx.obj['config']
    
    click.echo(f"🌊 Starting data streaming scenario: {scenario}")
    click.echo(f"⏱️  Duration: {duration} hours")
    click.echo(f"📈 Rate multiplier: {rate_multiplier}")
    
    if topics:
        click.echo(f"📋 Topics: {list(topics)}")
    
    if dry_run:
        click.echo("🔍 DRY RUN - No actual data will be streamed")
        return
    
    # Initialize orchestrator
    orchestrator = DataGenerationOrchestrator(config)
    
    try:
        if topics:
            # Custom topic streaming
            producer = KafkaDataProducer(orchestrator.generator)
            producer.start_streaming(
                topics=list(topics),
                duration_hours=duration,
                rate_multiplier=rate_multiplier
            )
            
            # Wait for completion
            import time
            time.sleep(duration * 3600)
            producer.stop_streaming()
            
            # Show stats
            stats = producer.get_stats()
            click.echo(f"\n📊 Streaming Statistics:")
            click.echo(f"  Messages sent: {stats['messages_sent']}")
            click.echo(f"  Messages failed: {stats['messages_failed']}")
            click.echo(f"  Bytes sent: {stats['bytes_sent']:,}")
            click.echo(f"  Messages per second: {stats.get('messages_per_second', 0):.2f}")
            
        else:
            # Run predefined scenario
            orchestrator.run_scenario(scenario, duration)
    
    except KeyboardInterrupt:
        click.echo("\n⚠️  Streaming interrupted by user")
    except Exception as e:
        click.echo(f"\n❌ Error during streaming: {e}", err=True)
        sys.exit(1)
    finally:
        orchestrator.close()
    
    click.echo("✅ Streaming completed")


@cli.command()
@click.option('--days', '-d', type=int, default=30, 
              help='Number of days of historical data to generate')
@click.option('--batch-size', '-b', type=int, default=1000, 
              help='Batch size for sending data')
@click.pass_context
def historical(ctx, days, batch_size):
    """Generate and load historical data."""
    config = ctx.obj['config']
    
    click.echo(f"📚 Generating {days} days of historical data...")
    
    # Initialize orchestrator
    orchestrator = DataGenerationOrchestrator(config)
    
    try:
        # Generate historical data
        results = orchestrator.generate_historical_data(days)
        
        click.echo(f"✅ Historical data generation completed:")
        for topic, count in results.items():
            click.echo(f"  {topic}: {count:,} records")
        
    except Exception as e:
        click.echo(f"❌ Error generating historical data: {e}", err=True)
        sys.exit(1)
    finally:
        orchestrator.close()


@cli.command()
@click.option('--kafka-servers', '-k', default='localhost:9092', 
              help='Kafka bootstrap servers')
@click.pass_context
def health(ctx, kafka_servers):
    """Check health of data generation components."""
    config = ctx.obj['config']
    
    click.echo("🏥 Checking health of data generation components...")
    
    # Test generator
    click.echo("\n🔧 Testing data generator...")
    try:
        generator = ECommerceDataGenerator(config)
        stats = generator.get_statistics()
        click.echo(f"  ✅ Generator initialized successfully")
        click.echo(f"  📊 Users: {stats['total_users']}, Products: {stats['total_products']}")
    except Exception as e:
        click.echo(f"  ❌ Generator error: {e}")
        return
    
    # Test Kafka producer
    click.echo("\n📡 Testing Kafka producer...")
    try:
        producer = KafkaDataProducer(generator, bootstrap_servers=kafka_servers.split(','))
        health_status = producer.health_check()
        
        if health_status['overall_status'] == 'HEALTHY':
            click.echo(f"  ✅ Kafka connection healthy")
            click.echo(f"  📋 Available topics: {len(health_status.get('available_topics', []))}")
        else:
            click.echo(f"  ⚠️  Kafka status: {health_status['overall_status']}")
            if 'missing_topics' in health_status:
                click.echo(f"  📋 Missing topics: {health_status['missing_topics']}")
        
        producer.close()
    except Exception as e:
        click.echo(f"  ❌ Kafka producer error: {e}")
        return
    
    click.echo("\n✅ Health check completed")


@cli.command()
@click.option('--format', '-f', type=click.Choice(['json', 'yaml']), default='json', 
              help='Configuration format')
@click.option('--output', '-o', type=click.Path(), 
              help='Output file path')
@click.pass_context
def config(ctx, format, output):
    """Show or export configuration."""
    config = ctx.obj['config']
    
    if format == 'json':
        config_str = config.to_json()
    else:
        config_str = yaml.dump(config.__dict__, default_flow_style=False)
    
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            f.write(config_str)
        
        click.echo(f"✅ Configuration exported to {output_path}")
    else:
        click.echo(config_str)


@cli.command()
@click.pass_context
def stats(ctx):
    """Show comprehensive statistics."""
    config = ctx.obj['config']
    
    click.echo("📊 E-Commerce Data Generation Statistics")
    click.echo("=" * 50)
    
    # Initialize components
    generator = ECommerceDataGenerator(config)
    orchestrator = DataGenerationOrchestrator(config)
    
    try:
        # Get comprehensive stats
        stats = orchestrator.get_comprehensive_stats()
        
        # Generator stats
        gen_stats = stats['generator_stats']
        click.echo(f"\n🔧 Generator Statistics:")
        click.echo(f"  Total users: {gen_stats['total_users']:,}")
        click.echo(f"  Active users: {gen_stats['active_users']:,}")
        click.echo(f"  Total products: {gen_stats['total_products']:,}")
        click.echo(f"  Active products: {gen_stats['active_products']:,}")
        click.echo(f"  Categories: {len(gen_stats['categories'])}")
        
        # Producer stats
        prod_stats = stats['producer_stats']
        click.echo(f"\n📡 Producer Statistics:")
        click.echo(f"  Messages sent: {prod_stats['messages_sent']:,}")
        click.echo(f"  Messages failed: {prod_stats['messages_failed']:,}")
        click.echo(f"  Bytes sent: {prod_stats['bytes_sent']:,}")
        
        # Configuration summary
        click.echo(f"\n⚙️  Configuration Summary:")
        click.echo(f"  Base transaction rate: {config.base_transaction_rate:,}/hour")
        click.echo(f"  Base event rate: {config.base_event_rate:,}/hour")
        click.echo(f"  Fraud rate: {config.fraud_rate:.1%}")
        click.echo(f"  Regions: {len(config.regions)}")
        
    finally:
        orchestrator.close()


if __name__ == '__main__':
    cli()
"""
Delta Lake CLI

Command-line interface for Delta Lake operations including table management,
optimization, vacuum, time travel, and maintenance scheduling.
"""

import json
from datetime import datetime
from typing import Optional

import click

from ..utils.logger import setup_logging
from ..utils.spark_utils import get_secure_temp_dir
from .delta import DeltaLakeManager
from .delta_config import DeltaTableConfigurations
from .delta_maintenance import DeltaMaintenanceManager
from .delta_streaming import DeltaStreamingManager

logger = setup_logging(__name__)


@click.group()
@click.option(
    "--base-path", default="s3a://data-lake/delta", help="Base path for Delta Lake"
)
@click.pass_context
def delta_cli(ctx, base_path):
    """Delta Lake management CLI for e-commerce analytics platform."""
    ctx.ensure_object(dict)
    ctx.obj["base_path"] = base_path
    ctx.obj["delta_manager"] = DeltaLakeManager(base_path=base_path)


@delta_cli.group()
@click.pass_context
def table(ctx):
    """Table management operations."""
    pass


@table.command()
@click.argument("table_name")
@click.option(
    "--schema-type",
    type=click.Choice(
        [
            "transaction",
            "user_events",
            "customer_profile",
            "product_catalog",
            "analytics_results",
        ]
    ),
    help="Predefined schema type",
)
@click.option("--partition-columns", help="Comma-separated partition columns")
@click.pass_context
def create(ctx, table_name, schema_type, partition_columns):
    """Create a new Delta Lake table."""
    delta_manager = ctx.obj["delta_manager"]

    if schema_type:
        # Use predefined configuration
        configs = {
            "transaction": DeltaTableConfigurations.transaction_table_config(),
            "user_events": DeltaTableConfigurations.user_events_table_config(),
            "customer_profile": DeltaTableConfigurations.customer_profile_table_config(),
            "product_catalog": DeltaTableConfigurations.product_catalog_table_config(),
            "analytics_results": DeltaTableConfigurations.analytics_results_table_config(),
        }

        config = configs[schema_type]
        config["name"] = table_name  # Override name

        table_path = delta_manager.create_delta_table(
            table_name=config["name"],
            schema=config["schema"],
            partition_columns=config["partition_columns"],
            properties=config["properties"],
        )

        click.echo(f"✅ Created Delta table '{table_name}' using {schema_type} schema")
        click.echo(f"📍 Path: {table_path}")
        click.echo(f"📊 Partitions: {config['partition_columns']}")

    else:
        click.echo("❌ Schema type is required for table creation")
        click.echo(
            "Available types: transaction, user_events, customer_profile, product_catalog, analytics_results"
        )


@table.command()
@click.pass_context
def list(ctx):
    """List all Delta Lake tables."""
    delta_manager = ctx.obj["delta_manager"]
    tables = delta_manager.list_tables()

    if tables:
        click.echo("📋 Delta Lake Tables:")
        click.echo("-" * 50)
        for table in tables:
            click.echo(f"  📊 {table['name']}")
            click.echo(f"     Path: {table['path']}")
            click.echo(f"     Status: {table['status']}")
            click.echo()
    else:
        click.echo("No Delta Lake tables found")


@table.command()
@click.argument("table_name")
@click.pass_context
def info(ctx, table_name):
    """Get detailed information about a Delta table."""
    delta_manager = ctx.obj["delta_manager"]

    try:
        info = delta_manager.get_table_info(table_name)

        click.echo(f"📊 Table Information: {table_name}")
        click.echo("=" * 50)
        click.echo(f"Path: {info['table_path']}")
        click.echo(f"Record Count: {info['current_count']:,}")
        click.echo(f"Latest Version: {info['latest_version']}")
        if info["creation_time"]:
            click.echo(f"Creation Time: {info['creation_time']}")

        # Show schema
        schema_dict = json.loads(info["schema"])
        click.echo(f"\n📋 Schema ({len(schema_dict['fields'])} columns):")
        for field in schema_dict["fields"]:
            nullable = "nullable" if field["nullable"] else "not null"
            click.echo(f"  - {field['name']}: {field['type']} ({nullable})")

    except Exception as e:
        click.echo(f"❌ Error getting table info: {e}")


@table.command()
@click.argument("table_name")
@click.option("--version", type=int, help="Specific version to read")
@click.option("--timestamp", help="Specific timestamp to read (YYYY-MM-DD HH:MM:SS)")
@click.option("--limit", default=10, help="Number of rows to display")
@click.pass_context
def show(ctx, table_name, version, timestamp, limit):
    """Show data from a Delta table with time travel support."""
    delta_manager = ctx.obj["delta_manager"]

    try:
        # Parse timestamp if provided
        timestamp_dt = None
        if timestamp:
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        # Read with time travel
        df = delta_manager.time_travel_read(
            table_name=table_name, timestamp=timestamp_dt, version=version
        )

        # Show data
        rows = df.limit(limit).collect()

        if rows:
            # Get column names
            columns = df.columns

            click.echo(f"📊 Data from table '{table_name}':")
            if version is not None:
                click.echo(f"   Version: {version}")
            if timestamp_dt:
                click.echo(f"   Timestamp: {timestamp}")
            click.echo(f"   Showing {len(rows)} of {df.count():,} rows")
            click.echo("=" * 80)

            # Print headers
            header = " | ".join(
                [col[:15].ljust(15) for col in columns[:5]]
            )  # Show first 5 columns
            click.echo(header)
            click.echo("-" * len(header))

            # Print rows
            for row in rows:
                row_data = " | ".join(
                    [str(row[col])[:15].ljust(15) for col in columns[:5]]
                )
                click.echo(row_data)
        else:
            click.echo(f"No data found in table '{table_name}'")

    except Exception as e:
        click.echo(f"❌ Error reading table: {e}")


@table.command()
@click.argument("table_name")
@click.option("--limit", default=20, help="Number of history entries to show")
@click.pass_context
def history(ctx, table_name, limit):
    """Show the history of operations on a Delta table."""
    delta_manager = ctx.obj["delta_manager"]

    try:
        history_df = delta_manager.get_table_history(table_name, limit)
        history_rows = history_df.collect()

        if history_rows:
            click.echo(f"📜 History for table '{table_name}':")
            click.echo("=" * 80)

            for row in history_rows:
                click.echo(f"Version {row['version']}: {row['operation']}")
                click.echo(f"  Timestamp: {row['timestamp']}")
                if "operationParameters" in row and row["operationParameters"]:
                    params = row["operationParameters"]
                    click.echo(f"  Parameters: {params}")
                if "operationMetrics" in row and row["operationMetrics"]:
                    metrics = row["operationMetrics"]
                    click.echo(f"  Metrics: {metrics}")
                click.echo()
        else:
            click.echo(f"No history found for table '{table_name}'")

    except Exception as e:
        click.echo(f"❌ Error getting table history: {e}")


@delta_cli.group()
@click.pass_context
def optimize(ctx):
    """Optimization operations."""
    pass


@optimize.command()
@click.argument("table_name")
@click.option("--where", help="WHERE clause for selective optimization")
@click.option("--zorder", help="Comma-separated columns for Z-ordering")
@click.pass_context
def compact(ctx, table_name, where, zorder):
    """Optimize Delta table by compacting small files."""
    delta_manager = ctx.obj["delta_manager"]

    try:
        z_order_columns = zorder.split(",") if zorder else None

        click.echo(f"🔧 Starting optimization for table '{table_name}'...")
        if where:
            click.echo(f"   WHERE clause: {where}")
        if z_order_columns:
            click.echo(f"   Z-order columns: {z_order_columns}")

        delta_manager.optimize_table(
            table_name=table_name, where_clause=where, z_order_columns=z_order_columns
        )

        click.echo(f"✅ Optimization completed for table '{table_name}'")

    except Exception as e:
        click.echo(f"❌ Error optimizing table: {e}")


@optimize.command()
@click.argument("table_name")
@click.option(
    "--retention-hours",
    default=168,
    help="Retention period in hours (default: 168 = 7 days)",
)
@click.option("--dry-run", is_flag=True, help="Perform dry run without deleting files")
@click.pass_context
def vacuum(ctx, table_name, retention_hours, dry_run):
    """Vacuum Delta table to remove old files."""
    delta_manager = ctx.obj["delta_manager"]

    try:
        click.echo(f"🧹 Starting vacuum for table '{table_name}'...")
        click.echo(f"   Retention: {retention_hours} hours")
        click.echo(f"   Dry run: {'Yes' if dry_run else 'No'}")

        result = delta_manager.vacuum_table(
            table_name=table_name, retention_hours=retention_hours, dry_run=dry_run
        )

        if dry_run:
            files = result.collect()
            click.echo(f"📊 Files that would be deleted: {len(files)}")
            if files:
                click.echo("Files to delete:")
                for file_info in files[:10]:  # Show first 10
                    click.echo(f"  - {file_info['path']}")
                if len(files) > 10:
                    click.echo(f"  ... and {len(files) - 10} more files")
        else:
            click.echo(f"✅ Vacuum completed for table '{table_name}'")

    except Exception as e:
        click.echo(f"❌ Error vacuuming table: {e}")


@delta_cli.group()
@click.pass_context
def maintenance(ctx):
    """Automated maintenance operations."""
    pass


@maintenance.command()
@click.option("--table-name", help="Specific table to analyze (default: all tables)")
@click.pass_context
def analyze(ctx, table_name):
    """Analyze tables and provide maintenance recommendations."""
    delta_manager = ctx.obj["delta_manager"]
    maintenance_manager = DeltaMaintenanceManager(delta_manager)

    try:
        if table_name:
            tables_to_analyze = [table_name]
        else:
            tables_to_analyze = list(delta_manager.tables.keys())

        for table in tables_to_analyze:
            click.echo(f"🔍 Analyzing table '{table}'...")
            recommendations = maintenance_manager.get_maintenance_recommendations(table)

            click.echo(f"\n📋 Recommendations for '{table}':")
            click.echo(
                f"   Overall Priority: {recommendations['overall_priority'].upper()}"
            )
            click.echo(f"   Priority Score: {recommendations['priority_score']}")

            if recommendations["recommendations"]:
                click.echo(f"   Recommended Actions:")
                for i, rec in enumerate(recommendations["recommendations"], 1):
                    click.echo(
                        f"     {i}. {rec['action'].upper()} - {rec['priority']} priority"
                    )
                    click.echo(f"        Reason: {rec['reason']}")
                    click.echo(f"        Benefit: {rec['expected_benefit']}")
                    if "suggested_columns" in rec:
                        click.echo(
                            f"        Suggested columns: {rec['suggested_columns']}"
                        )
                    click.echo()
            else:
                click.echo("   No maintenance recommendations at this time")

            click.echo("-" * 60)

    except Exception as e:
        click.echo(f"❌ Error analyzing tables: {e}")


@maintenance.command()
@click.option(
    "--dry-run", is_flag=True, help="Show what would be done without executing"
)
@click.pass_context
def auto(ctx, dry_run):
    """Run automated maintenance for all tables."""
    delta_manager = ctx.obj["delta_manager"]
    maintenance_manager = DeltaMaintenanceManager(delta_manager)

    try:
        if dry_run:
            click.echo("🔍 Dry run - analyzing maintenance needs...")
            # For dry run, just show recommendations
            for table_name in delta_manager.tables.keys():
                recommendations = maintenance_manager.get_maintenance_recommendations(
                    table_name
                )
                if recommendations["recommendations"]:
                    click.echo(
                        f"📊 {table_name}: {len(recommendations['recommendations'])} actions needed"
                    )
                    for rec in recommendations["recommendations"]:
                        click.echo(f"   - {rec['action']} ({rec['priority']} priority)")
        else:
            click.echo("🚀 Starting automated maintenance...")
            results = maintenance_manager.run_automated_maintenance()

            # Show results summary
            click.echo(f"\n📊 Maintenance Results:")
            click.echo(f"   Optimized tables: {len(results['optimization_results'])}")
            click.echo(f"   Vacuumed tables: {len(results['vacuum_results'])}")
            click.echo(f"   Skipped tables: {len(results['skipped_tables'])}")

            # Show successful optimizations
            if results["optimization_results"]:
                click.echo(f"\n✅ Optimization Results:")
                for result in results["optimization_results"]:
                    if result["status"] == "success":
                        improvements = result.get("improvements", {})
                        click.echo(
                            f"   {result['table_name']}: {improvements.get('file_count_reduction', 0)} files reduced"
                        )

            # Show errors if any
            failed_ops = [
                r
                for r in results["optimization_results"] + results["vacuum_results"]
                if r["status"] == "failed"
            ]
            if failed_ops:
                click.echo(f"\n❌ Failed Operations:")
                for result in failed_ops:
                    click.echo(
                        f"   {result['table_name']}: {result.get('error', 'Unknown error')}"
                    )

    except Exception as e:
        click.echo(f"❌ Error running automated maintenance: {e}")


@maintenance.command()
@click.pass_context
def schedule(ctx):
    """Show maintenance schedule for all tables."""
    delta_manager = ctx.obj["delta_manager"]
    maintenance_manager = DeltaMaintenanceManager(delta_manager)

    try:
        schedule = maintenance_manager.schedule_maintenance_jobs()

        click.echo("📅 Maintenance Schedule:")
        click.echo("=" * 60)

        for table_name, table_info in schedule["tables"].items():
            plan = table_info["maintenance_plan"]
            click.echo(f"📊 {table_name} ({table_info['table_type']}):")
            click.echo(f"   Next Optimize: {plan['next_optimize']}")
            click.echo(f"   Next Vacuum: {plan['next_vacuum']}")
            click.echo(
                f"   Optimize Frequency: {plan['schedule']['optimize_frequency']}"
            )
            click.echo(f"   Vacuum Frequency: {plan['schedule']['vacuum_frequency']}")
            click.echo()

    except Exception as e:
        click.echo(f"❌ Error getting maintenance schedule: {e}")


@delta_cli.group()
@click.pass_context
def streaming(ctx):
    """Streaming operations."""
    pass


@streaming.command()
@click.option(
    "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
)
@click.option(
    "--checkpoint-path",
    default=get_secure_temp_dir("delta_streaming_checkpoints"),
    help="Checkpoint base path",
)
@click.pass_context
def start_all(ctx, kafka_servers, checkpoint_path):
    """Start all streaming jobs for Delta Lake."""
    delta_manager = ctx.obj["delta_manager"]
    streaming_manager = DeltaStreamingManager(
        delta_manager=delta_manager,
        kafka_bootstrap_servers=kafka_servers,
        checkpoint_base_path=checkpoint_path,
    )

    try:
        click.echo("🚀 Starting all streaming jobs...")

        # Start transaction streaming
        click.echo("   Starting transaction streaming...")
        streaming_manager.start_transaction_streaming()

        # Start user events streaming
        click.echo("   Starting user events streaming...")
        streaming_manager.start_user_events_streaming()

        # Start customer profile streaming
        click.echo("   Starting customer profile streaming...")
        streaming_manager.start_customer_profile_streaming()

        click.echo("✅ All streaming jobs started successfully")
        click.echo(f"📊 Active streams: {len(streaming_manager.active_streams)}")

        # Show health status
        health = streaming_manager.monitor_streaming_health()
        click.echo("\n📊 Stream Health:")
        for stream_name, metrics in health.items():
            status_icon = "✅" if metrics["status"] == "active" else "❌"
            click.echo(f"   {status_icon} {stream_name}: {metrics['status']}")

    except Exception as e:
        click.echo(f"❌ Error starting streaming jobs: {e}")


@streaming.command()
@click.option(
    "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
)
@click.option(
    "--checkpoint-path",
    default=get_secure_temp_dir("delta_streaming_checkpoints"),
    help="Checkpoint base path",
)
@click.pass_context
def status(ctx, kafka_servers, checkpoint_path):
    """Show status of streaming jobs."""
    delta_manager = ctx.obj["delta_manager"]
    streaming_manager = DeltaStreamingManager(
        delta_manager=delta_manager,
        kafka_bootstrap_servers=kafka_servers,
        checkpoint_base_path=checkpoint_path,
    )

    try:
        health = streaming_manager.monitor_streaming_health()
        stats = streaming_manager.get_stream_statistics()

        click.echo("📊 Streaming Status:")
        click.echo("=" * 50)
        click.echo(f"Active Streams: {stats['active_streams']}")
        click.echo(
            f"Average Processing Rate: {stats['average_processing_rate']:.2f} rows/sec"
        )
        click.echo()

        if health:
            click.echo("Stream Details:")
            for stream_name, metrics in health.items():
                status_icon = "✅" if metrics["status"] == "active" else "❌"
                click.echo(f"  {status_icon} {stream_name}:")
                click.echo(f"     Status: {metrics['status']}")
                if metrics["status"] == "active":
                    click.echo(f"     Batch ID: {metrics.get('batch_id', 'N/A')}")
                    click.echo(
                        f"     Input Rate: {metrics.get('input_rows_per_second', 0):.2f} rows/sec"
                    )
                    click.echo(
                        f"     Processing Rate: {metrics.get('processed_rows_per_second', 0):.2f} rows/sec"
                    )
                else:
                    if metrics.get("exception"):
                        click.echo(f"     Error: {metrics['exception']}")
                click.echo()
        else:
            click.echo("No active streams found")

    except Exception as e:
        click.echo(f"❌ Error getting streaming status: {e}")


@delta_cli.command()
@click.argument("table_name")
@click.option(
    "--output-format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
@click.pass_context
def describe(ctx, table_name, output_format):
    """Describe a Delta Lake table in detail."""
    delta_manager = ctx.obj["delta_manager"]

    try:
        info = delta_manager.get_table_info(table_name)

        if output_format == "json":
            click.echo(json.dumps(info, default=str, indent=2))
        else:
            click.echo(f"🔍 Table Description: {table_name}")
            click.echo("=" * 60)
            click.echo(f"Path: {info['table_path']}")
            click.echo(f"Records: {info['current_count']:,}")
            click.echo(f"Latest Version: {info['latest_version']}")
            if info["creation_time"]:
                click.echo(f"Created: {info['creation_time']}")

            # Show recent history
            history_df = delta_manager.get_table_history(table_name, limit=5)
            recent_ops = history_df.collect()

            if recent_ops:
                click.echo(f"\nRecent Operations:")
                for op in recent_ops:
                    click.echo(
                        f"  Version {op['version']}: {op['operation']} at {op['timestamp']}"
                    )

    except Exception as e:
        click.echo(f"❌ Error describing table: {e}")


if __name__ == "__main__":
    delta_cli()

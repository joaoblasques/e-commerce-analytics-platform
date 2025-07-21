"""
Data Lake CLI

Command-line interface for data lake operations including storage,
ingestion, compaction, and metadata management.
"""

import json

import click

from ..utils.logger import setup_logger
from .compaction import DataCompactor
from .ingestion import DataLakeIngester
from .metadata import MetadataManager, TableStatus
from .storage import DataLakeStorage

logger = setup_logger(__name__)


@click.group()
@click.option("--base-path", default="s3a://data-lake", help="Base path for data lake")
@click.option(
    "--catalog-path",
    default="s3a://data-lake/catalog",
    help="Path for catalog metadata",
)
@click.pass_context
def data_lake(ctx, base_path, catalog_path):
    """Data Lake operations CLI."""
    ctx.ensure_object(dict)
    ctx.obj["base_path"] = base_path
    ctx.obj["catalog_path"] = catalog_path


@data_lake.group()
@click.pass_context
def storage(ctx):
    """Data lake storage operations."""
    pass


@storage.command()
@click.argument("table_name")
@click.option(
    "--data-type", required=True, help="Type of data for partitioning strategy"
)
@click.option("--mode", default="append", help="Write mode (append, overwrite)")
@click.option("--source-path", required=True, help="Source data path")
@click.option("--file-format", default="parquet", help="Source file format")
@click.pass_context
def write(ctx, table_name, data_type, mode, source_path, file_format):
    """Write data to data lake with optimal partitioning."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)

        # Read source data
        if file_format.lower() == "json":
            df = storage_manager.spark.read.json(source_path)
        elif file_format.lower() == "csv":
            df = storage_manager.spark.read.option("header", "true").csv(source_path)
        elif file_format.lower() == "parquet":
            df = storage_manager.spark.read.parquet(source_path)
        else:
            click.echo(f"Unsupported file format: {file_format}")
            return

        # Write to data lake
        output_path = storage_manager.write_partitioned_data(
            df=df, table_name=table_name, data_type=data_type, mode=mode
        )

        click.echo(f"Successfully wrote data to: {output_path}")

    except Exception as e:
        click.echo(f"Error writing data: {str(e)}")


@storage.command()
@click.argument("table_name")
@click.option(
    "--partition-filter", multiple=True, help="Partition filters (column=value)"
)
@click.option("--columns", multiple=True, help="Specific columns to read")
@click.option("--limit", type=int, help="Limit number of rows to display")
@click.pass_context
def read(ctx, table_name, partition_filter, columns, limit):
    """Read data from data lake table."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)

        # Parse partition filters
        filters = {}
        for pf in partition_filter:
            if "=" in pf:
                key, value = pf.split("=", 1)
                filters[key.strip()] = value.strip()

        # Read data
        df = storage_manager.read_partitioned_data(
            table_name=table_name,
            partition_filters=filters if filters else None,
            columns=list(columns) if columns else None,
        )

        if limit:
            df = df.limit(limit)

        # Show results
        df.show(truncate=False)

    except Exception as e:
        click.echo(f"Error reading data: {str(e)}")


@storage.command()
@click.argument("table_name")
@click.pass_context
def info(ctx, table_name):
    """Get information about a stored table."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)

        info = storage_manager.get_table_info(table_name)
        click.echo(json.dumps(info, indent=2))

    except Exception as e:
        click.echo(f"Error getting table info: {str(e)}")


@storage.command()
@click.pass_context
def list_tables(ctx):
    """List all tables in the data lake."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)

        tables = storage_manager.list_tables()

        if tables:
            click.echo("Tables in data lake:")
            for table in tables:
                click.echo(
                    f"  {table['table_name']} - {table['path']} (modified: {table['modified']})"
                )
        else:
            click.echo("No tables found in data lake")

    except Exception as e:
        click.echo(f"Error listing tables: {str(e)}")


@data_lake.group()
@click.pass_context
def ingestion(ctx):
    """Data lake ingestion operations."""
    pass


@ingestion.command()
@click.argument("topic")
@click.argument("table_name")
@click.option("--data-type", required=True, help="Type of data for partitioning")
@click.option("--batch-size", default=1000, help="Batch size for processing")
@click.option("--max-batches", type=int, help="Maximum number of batches to process")
@click.option("--kafka-servers", default="kafka:9092", help="Kafka bootstrap servers")
@click.pass_context
def kafka_batch(
    ctx, topic, table_name, data_type, batch_size, max_batches, kafka_servers
):
    """Ingest data from Kafka in batch mode."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)
        ingester = DataLakeIngester(
            storage=storage_manager, kafka_bootstrap_servers=kafka_servers
        )

        result = ingester.ingest_from_kafka_batch(
            topic=topic,
            table_name=table_name,
            data_type=data_type,
            batch_size=batch_size,
            max_batches=max_batches,
        )

        click.echo(json.dumps(result, indent=2))

    except Exception as e:
        click.echo(f"Error ingesting from Kafka: {str(e)}")


@ingestion.command()
@click.argument("topic")
@click.argument("table_name")
@click.option("--data-type", required=True, help="Type of data for partitioning")
@click.option("--checkpoint-location", help="Checkpoint location for streaming")
@click.option("--trigger-interval", default="10 seconds", help="Trigger interval")
@click.option("--kafka-servers", default="kafka:9092", help="Kafka bootstrap servers")
@click.pass_context
def kafka_stream(
    ctx,
    topic,
    table_name,
    data_type,
    checkpoint_location,
    trigger_interval,
    kafka_servers,
):
    """Start streaming ingestion from Kafka."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)
        ingester = DataLakeIngester(
            storage=storage_manager, kafka_bootstrap_servers=kafka_servers
        )

        stream_id = ingester.ingest_from_kafka_streaming(
            topic=topic,
            table_name=table_name,
            data_type=data_type,
            checkpoint_location=checkpoint_location,
            trigger_interval=trigger_interval,
        )

        click.echo(f"Started streaming ingestion with ID: {stream_id}")
        click.echo("Use 'data-lake ingestion stream-status' to monitor progress")

    except Exception as e:
        click.echo(f"Error starting streaming ingestion: {str(e)}")


@ingestion.command()
@click.option("--stream-id", help="Specific stream ID to check")
@click.pass_context
def stream_status(ctx, stream_id):
    """Get status of streaming queries."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)
        ingester = DataLakeIngester(storage=storage_manager)

        status = ingester.get_stream_status(stream_id)
        click.echo(json.dumps(status, indent=2))

    except Exception as e:
        click.echo(f"Error getting stream status: {str(e)}")


@data_lake.group()
@click.pass_context
def compaction(ctx):
    """Data lake compaction operations."""
    pass


@compaction.command()
@click.argument("table_name")
@click.option("--target-file-size", default=128, help="Target file size in MB")
@click.option("--max-file-size", default=256, help="Maximum file size in MB")
@click.option(
    "--backup/--no-backup", default=True, help="Create backup before compaction"
)
@click.pass_context
def compact(ctx, table_name, target_file_size, max_file_size, backup):
    """Compact a table to optimize file sizes."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)
        compactor = DataCompactor(storage=storage_manager)

        result = compactor.compact_table(
            table_name=table_name,
            target_file_size_mb=target_file_size,
            max_file_size_mb=max_file_size,
            backup=backup,
        )

        click.echo(json.dumps(result, indent=2))

    except Exception as e:
        click.echo(f"Error compacting table: {str(e)}")


@compaction.command()
@click.argument("table_name")
@click.pass_context
def analyze(ctx, table_name):
    """Analyze table file structure and recommend optimizations."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)
        compactor = DataCompactor(storage=storage_manager)

        analysis = compactor.analyze_table_files(table_name)
        click.echo(json.dumps(analysis, indent=2))

        if analysis.get("needs_compaction"):
            click.echo(f"\n⚠️  Recommendation: {analysis.get('compaction_reason')}")
        else:
            click.echo("\n✅ Table is well optimized")

    except Exception as e:
        click.echo(f"Error analyzing table: {str(e)}")


@compaction.command()
@click.option(
    "--min-file-count", default=10, help="Minimum file count to trigger optimization"
)
@click.option("--target-file-size", default=128, help="Target file size in MB")
@click.option(
    "--parallel/--no-parallel", default=True, help="Process tables in parallel"
)
@click.pass_context
def optimize_all(ctx, min_file_count, target_file_size, parallel):
    """Optimize all tables that need compaction."""
    try:
        base_path = ctx.obj["base_path"]
        storage_manager = DataLakeStorage(base_path=base_path)
        compactor = DataCompactor(storage=storage_manager)

        result = compactor.optimize_all_tables(
            min_file_count=min_file_count,
            target_file_size_mb=target_file_size,
            parallel=parallel,
        )

        summary = result.get("summary", {})
        click.echo(f"Optimization complete:")
        click.echo(f"  Optimized: {summary.get('optimized_count', 0)}")
        click.echo(f"  Skipped: {summary.get('skipped_count', 0)}")
        click.echo(f"  Failed: {summary.get('failed_count', 0)}")

        if result.get("failed"):
            click.echo("\nFailed tables:")
            for failure in result["failed"]:
                click.echo(
                    f"  {failure['table_name']}: {failure.get('error', 'Unknown error')}"
                )

    except Exception as e:
        click.echo(f"Error optimizing tables: {str(e)}")


@data_lake.group()
@click.pass_context
def catalog(ctx):
    """Data lake catalog operations."""
    pass


@catalog.command()
@click.argument("table_name")
@click.argument("table_path")
@click.option("--description", help="Table description")
@click.option("--owner", help="Table owner")
@click.option("--tag", "tags", multiple=True, help="Table tags")
@click.pass_context
def register(ctx, table_name, table_path, description, owner, tags):
    """Register a table in the catalog."""
    try:
        catalog_path = ctx.obj["catalog_path"]
        metadata_manager = MetadataManager(catalog_path=catalog_path)

        metadata = metadata_manager.register_table(
            table_name=table_name,
            table_path=table_path,
            description=description,
            owner=owner,
            tags=list(tags) if tags else None,
        )

        click.echo(f"Successfully registered table: {table_name}")
        click.echo(f"  Record count: {metadata.record_count}")
        click.echo(f"  Columns: {len(metadata.columns)}")
        click.echo(f"  Partitions: {metadata.partition_columns}")

    except Exception as e:
        click.echo(f"Error registering table: {str(e)}")


@catalog.command()
@click.argument("table_name")
@click.option("--compute-column-stats/--no-compute-column-stats", default=True)
@click.option("--sample-size", type=int, help="Sample size for statistics")
@click.pass_context
def update_stats(ctx, table_name, compute_column_stats, sample_size):
    """Update statistics for a table."""
    try:
        catalog_path = ctx.obj["catalog_path"]
        metadata_manager = MetadataManager(catalog_path=catalog_path)

        metadata = metadata_manager.update_table_statistics(
            table_name=table_name,
            compute_column_stats=compute_column_stats,
            sample_size=sample_size,
        )

        click.echo(f"Updated statistics for {table_name}:")
        click.echo(f"  Record count: {metadata.record_count}")
        click.echo(f"  File count: {metadata.file_count}")
        click.echo(f"  Total size: {metadata.total_size_bytes / (1024*1024):.1f} MB")

    except Exception as e:
        click.echo(f"Error updating statistics: {str(e)}")


@catalog.command()
@click.option("--database", help="Database name to filter by")
@click.option(
    "--status",
    type=click.Choice(["active", "deprecated", "archived", "under_maintenance"]),
)
@click.option("--tag", "tags", multiple=True, help="Tags to filter by")
@click.pass_context
def list_catalog_tables(ctx, database, status, tags):
    """List tables in the catalog."""
    try:
        catalog_path = ctx.obj["catalog_path"]
        metadata_manager = MetadataManager(catalog_path=catalog_path)

        # Convert status string to enum
        status_enum = None
        if status:
            status_enum = TableStatus(status)

        tables = metadata_manager.list_tables(
            database=database, status=status_enum, tags=list(tags) if tags else None
        )

        if tables:
            click.echo("Catalog tables:")
            for table_meta in tables:
                click.echo(f"  {table_meta.table_name}")
                click.echo(f"    Database: {table_meta.database_name}")
                click.echo(f"    Status: {table_meta.status.value}")
                click.echo(f"    Records: {table_meta.record_count or 'Unknown'}")
                if table_meta.description:
                    click.echo(f"    Description: {table_meta.description}")
                if table_meta.tags:
                    click.echo(f"    Tags: {', '.join(table_meta.tags)}")
                click.echo()
        else:
            click.echo("No tables found matching criteria")

    except Exception as e:
        click.echo(f"Error listing tables: {str(e)}")


@catalog.command()
@click.argument("search_term")
@click.option(
    "--field",
    "fields",
    multiple=True,
    default=["table_name", "description", "tags"],
    help="Fields to search in",
)
@click.pass_context
def search(ctx, search_term, fields):
    """Search tables in the catalog."""
    try:
        catalog_path = ctx.obj["catalog_path"]
        metadata_manager = MetadataManager(catalog_path=catalog_path)

        tables = metadata_manager.search_tables(
            search_term=search_term, search_fields=list(fields)
        )

        if tables:
            click.echo(f"Found {len(tables)} tables matching '{search_term}':")
            for table_meta in tables:
                click.echo(
                    f"  {table_meta.table_name} - {table_meta.description or 'No description'}"
                )
        else:
            click.echo(f"No tables found matching '{search_term}'")

    except Exception as e:
        click.echo(f"Error searching tables: {str(e)}")


if __name__ == "__main__":
    data_lake()

"""
Data Lifecycle Management CLI

Command-line interface for managing data lifecycle operations including
retention policies, archiving, lineage tracking, and cost optimization.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ..utils.logger import setup_logging
from .delta import DeltaLakeManager
from .lifecycle_config import LifecycleConfigManager
from .lifecycle_manager import (
    ArchivePolicy,
    DataLifecycleManager,
    DataTier,
    LineageRecord,
    RetentionPolicy,
    RetentionRule,
)

logger = setup_logging(__name__)


class LifecycleCLI:
    """Command-line interface for data lifecycle management."""

    def __init__(self):
        self.delta_manager = None
        self.lifecycle_manager = None
        self.config_manager = LifecycleConfigManager()

    def setup_managers(self, base_path: str = "s3a://data-lake/delta"):
        """Initialize Delta Lake and Lifecycle managers."""
        try:
            self.delta_manager = DeltaLakeManager(base_path=base_path)
            self.lifecycle_manager = DataLifecycleManager(self.delta_manager)

            # Load default configurations
            for table_name, rule in self.config_manager.retention_rules.items():
                self.lifecycle_manager.add_retention_rule(rule)

            for table_name, policy in self.config_manager.archive_policies.items():
                self.lifecycle_manager.add_archive_policy(policy)

            logger.info("Lifecycle managers initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize managers: {e}")
            raise

    def apply_retention(self, args):
        """Apply retention policies to tables."""
        print("🔄 Applying retention policies...")

        try:
            # Filter tables if specified
            tables_to_process = (
                args.tables
                if args.tables
                else list(self.lifecycle_manager.retention_rules.keys())
            )

            if not tables_to_process:
                print("❌ No tables configured for retention policies")
                return

            # Apply retention policies
            results = {}
            for table_name in tables_to_process:
                if table_name not in self.lifecycle_manager.retention_rules:
                    print(f"⚠️  No retention rule configured for table: {table_name}")
                    continue

                print(f"📊 Processing table: {table_name}")

                try:
                    rule = self.lifecycle_manager.retention_rules[table_name]
                    result = self.lifecycle_manager._apply_table_retention_policy(
                        table_name, rule, args.dry_run
                    )
                    results[table_name] = result

                    # Print results
                    print(
                        f"   Hot records (0-{rule.hot_days} days): {result.get('hot_records', 0):,}"
                    )
                    print(
                        f"   Warm records ({rule.hot_days}-{rule.warm_days} days): {result.get('warm_records', 0):,}"
                    )
                    print(
                        f"   Cold records ({rule.warm_days}-{rule.cold_days} days): {result.get('cold_records', 0):,}"
                    )

                    if result.get("archived_records", 0) > 0:
                        action = "Would archive" if args.dry_run else "Archived"
                        print(f"   📦 {action}: {result['archived_records']:,} records")

                    if result.get("deleted_records", 0) > 0:
                        action = "Would delete" if args.dry_run else "Deleted"
                        print(f"   🗑️  {action}: {result['deleted_records']:,} records")

                except Exception as e:
                    print(f"   ❌ Error processing {table_name}: {e}")
                    results[table_name] = {"error": str(e)}

            # Save results if not dry run
            if not args.dry_run and args.output:
                with open(args.output, "w") as f:
                    json.dump(
                        {"timestamp": datetime.now().isoformat(), "results": results},
                        f,
                        indent=2,
                        default=str,
                    )
                print(f"📄 Results saved to: {args.output}")

            print("✅ Retention policy application completed")

        except Exception as e:
            logger.error(f"Failed to apply retention policies: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)

    def show_lineage(self, args):
        """Show data lineage for a table."""
        print(f"🔍 Tracing lineage for table: {args.table}")

        try:
            lineage = self.lifecycle_manager.get_lineage_graph(args.table, args.depth)

            if "error" in lineage:
                print(f"❌ Error: {lineage['error']}")
                return

            # Display lineage information
            print(f"\n📊 Table: {lineage['table']}")

            # Upstream dependencies
            if lineage["upstream"]:
                print(f"\n⬆️  Upstream dependencies ({len(lineage['upstream'])}):")
                for upstream in lineage["upstream"]:
                    print(f"   • {upstream}")
            else:
                print(f"\n⬆️  No upstream dependencies found")

            # Downstream consumers
            if lineage["downstream"]:
                print(f"\n⬇️  Downstream consumers ({len(lineage['downstream'])}):")
                for downstream in lineage["downstream"]:
                    print(f"   • {downstream}")
            else:
                print(f"\n⬇️  No downstream consumers found")

            # Recent operations
            operations = lineage["operations"]
            if operations:
                print(f"\n🔄 Recent operations ({len(operations)}):")
                for i, op in enumerate(operations[-10:]):  # Show last 10 operations
                    timestamp = datetime.fromisoformat(op["timestamp"]).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                    print(
                        f"   {i+1:2d}. {op['operation']} at {timestamp} ({op['record_count']:,} records)"
                    )

                if len(operations) > 10:
                    print(f"   ... and {len(operations) - 10} more operations")

            # Save lineage graph if requested
            if args.output:
                with open(args.output, "w") as f:
                    json.dump(lineage, f, indent=2, default=str)
                print(f"\n📄 Lineage graph saved to: {args.output}")

        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)

    def optimize_storage(self, args):
        """Optimize storage costs and tiers."""
        print("💰 Analyzing storage for cost optimization...")

        try:
            # Get storage metrics
            metrics = self.lifecycle_manager.optimize_storage_costs()

            if not metrics:
                print("❌ No tables found for optimization")
                return

            # Display metrics
            total_size_gb = sum(m.size_bytes for m in metrics.values()) / (
                1024 * 1024 * 1024
            )
            total_cost = sum(m.storage_cost_usd for m in metrics.values())

            print(f"\n📊 Storage Summary:")
            print(f"   Total tables: {len(metrics)}")
            print(f"   Total size: {total_size_gb:.2f} GB")
            print(f"   Estimated monthly cost: ${total_cost:.2f}")

            # Show per-table metrics
            print(f"\n📋 Per-table metrics:")
            print(
                f"{'Table':<25} {'Size (GB)':<12} {'Tier':<15} {'Cost ($)':<10} {'Recommendation'}"
            )
            print("-" * 80)

            for table_name, table_metrics in metrics.items():
                size_gb = table_metrics.size_bytes / (1024 * 1024 * 1024)
                current_tier = table_metrics.tier.value
                cost = table_metrics.storage_cost_usd

                # Get recommendation
                days_since_access = (datetime.now() - table_metrics.last_accessed).days
                if days_since_access > 90 and current_tier in [
                    "standard",
                    "infrequent",
                ]:
                    recommendation = "→ Archive"
                elif days_since_access > 7 and current_tier == "standard":
                    recommendation = "→ Infrequent"
                else:
                    recommendation = "Optimal"

                print(
                    f"{table_name:<25} {size_gb:<12.2f} {current_tier:<15} {cost:<10.2f} {recommendation}"
                )

            # Generate optimization recommendations
            recommendations = self.lifecycle_manager._generate_recommendations(
                metrics, {}
            )

            if recommendations:
                print(f"\n💡 Optimization recommendations:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"   {i}. {rec}")

            # Save metrics if requested
            if args.output:
                output_data = {
                    "timestamp": datetime.now().isoformat(),
                    "summary": {
                        "total_tables": len(metrics),
                        "total_size_gb": total_size_gb,
                        "total_monthly_cost_usd": total_cost,
                    },
                    "table_metrics": {
                        table: {
                            "size_bytes": m.size_bytes,
                            "size_gb": m.size_bytes / (1024 * 1024 * 1024),
                            "record_count": m.record_count,
                            "tier": m.tier.value,
                            "storage_cost_usd": m.storage_cost_usd,
                            "last_accessed": m.last_accessed.isoformat(),
                            "access_count_30d": m.access_count_30d,
                        }
                        for table, m in metrics.items()
                    },
                    "recommendations": recommendations,
                }

                with open(args.output, "w") as f:
                    json.dump(output_data, f, indent=2, default=str)
                print(f"\n📄 Storage analysis saved to: {args.output}")

        except Exception as e:
            logger.error(f"Failed to optimize storage: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)

    def generate_report(self, args):
        """Generate comprehensive lifecycle report."""
        print("📊 Generating data lifecycle report...")

        try:
            # Generate comprehensive report
            report = self.lifecycle_manager.generate_lifecycle_report()

            # Display report summary
            print(f"\n📋 Data Lifecycle Report")
            print(f"   Generated: {report['timestamp']}")

            # Retention summary
            retention_summary = report["retention_summary"]
            if retention_summary:
                print(f"\n🔄 Retention Policy Summary:")
                total_hot = sum(
                    r.get("hot_records", 0)
                    for r in retention_summary.values()
                    if isinstance(r, dict)
                )
                total_warm = sum(
                    r.get("warm_records", 0)
                    for r in retention_summary.values()
                    if isinstance(r, dict)
                )
                total_cold = sum(
                    r.get("cold_records", 0)
                    for r in retention_summary.values()
                    if isinstance(r, dict)
                )

                print(f"   Hot records (recent): {total_hot:,}")
                print(f"   Warm records: {total_warm:,}")
                print(f"   Cold records: {total_cold:,}")

            # Storage summary
            storage_summary = report["storage_summary"]
            print(f"\n💾 Storage Summary:")
            print(f"   Total tables: {storage_summary.get('total_tables', 0)}")
            print(f"   Total size: {storage_summary.get('total_size_gb', 0):.2f} GB")
            print(
                f"   Monthly cost: ${storage_summary.get('total_monthly_cost_usd', 0):.2f}"
            )

            # Storage by tier
            by_tier = storage_summary.get("by_tier", {})
            if by_tier:
                print(f"\n📊 Storage by tier:")
                for tier, tier_data in by_tier.items():
                    print(
                        f"   {tier.capitalize()}: {tier_data['table_count']} tables, "
                        f"{tier_data['total_size_gb']:.2f} GB, ${tier_data['total_cost_usd']:.2f}"
                    )

            # Lineage summary
            lineage_summary = report["lineage_summary"]
            print(f"\n🔗 Lineage Summary:")
            print(
                f"   Total operations tracked: {lineage_summary.get('total_operations', 0)}"
            )
            print(f"   Tables with lineage: {lineage_summary.get('tables_tracked', 0)}")
            print(
                f"   Recent operations (7 days): {lineage_summary.get('recent_operations', 0)}"
            )

            # Recommendations
            recommendations = report["recommendations"]
            if recommendations:
                print(f"\n💡 Recommendations ({len(recommendations)}):")
                for i, rec in enumerate(recommendations, 1):
                    print(f"   {i}. {rec}")

            # Save report
            output_file = (
                args.output
                or f"lifecycle_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(output_file, "w") as f:
                json.dump(report, f, indent=2, default=str)

            print(f"\n📄 Complete report saved to: {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)

    def config_table(self, args):
        """Configure retention and archive policies for a table."""
        print(f"⚙️  Configuring lifecycle policies for table: {args.table}")

        try:
            # Get existing configuration
            existing_rule = self.config_manager.get_retention_rule(args.table)
            existing_policy = self.config_manager.get_archive_policy(args.table)

            # Update retention rule if parameters provided
            if any(
                [
                    args.hot_days,
                    args.warm_days,
                    args.cold_days,
                    args.archive_days,
                    args.delete_days,
                ]
            ):
                retention_rule = RetentionRule(
                    table_name=args.table,
                    hot_days=args.hot_days or existing_rule.hot_days,
                    warm_days=args.warm_days or existing_rule.warm_days,
                    cold_days=args.cold_days or existing_rule.cold_days,
                    archive_after_days=args.archive_days
                    or existing_rule.archive_after_days,
                    delete_after_days=args.delete_days
                    or existing_rule.delete_after_days,
                    partition_column=args.partition_column
                    or existing_rule.partition_column,
                    enabled=args.enabled
                    if args.enabled is not None
                    else existing_rule.enabled,
                )

                self.config_manager.update_retention_rule(args.table, retention_rule)
                self.lifecycle_manager.add_retention_rule(retention_rule)

                print(f"✅ Updated retention rule:")
                print(f"   Hot period: {retention_rule.hot_days} days")
                print(f"   Warm period: {retention_rule.warm_days} days")
                print(f"   Cold period: {retention_rule.cold_days} days")
                if retention_rule.archive_after_days:
                    print(f"   Archive after: {retention_rule.archive_after_days} days")
                if retention_rule.delete_after_days:
                    print(f"   Delete after: {retention_rule.delete_after_days} days")

            # Update archive policy if parameters provided
            if any([args.archive_path, args.archive_format, args.compression]):
                archive_policy = ArchivePolicy(
                    table_name=args.table,
                    archive_path=args.archive_path or existing_policy.archive_path,
                    format=args.archive_format or existing_policy.format,
                    compression=args.compression or existing_policy.compression,
                    partition_by=args.partition_by or existing_policy.partition_by,
                    enabled=args.enabled
                    if args.enabled is not None
                    else existing_policy.enabled,
                )

                self.config_manager.update_archive_policy(args.table, archive_policy)
                self.lifecycle_manager.add_archive_policy(archive_policy)

                print(f"✅ Updated archive policy:")
                print(f"   Archive path: {archive_policy.archive_path}")
                print(f"   Format: {archive_policy.format}")
                print(f"   Compression: {archive_policy.compression}")

            # Save configuration if requested
            if args.save_config:
                config_file = args.save_config
                self.config_manager.export_configuration(config_file)
                print(f"📄 Configuration saved to: {config_file}")

        except Exception as e:
            logger.error(f"Failed to configure table: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)

    def list_tables(self, args):
        """List tables with their lifecycle configuration."""
        print("📋 Tables with lifecycle configuration:")

        try:
            # Get all configured tables
            retention_tables = set(self.config_manager.retention_rules.keys())
            archive_tables = set(self.config_manager.archive_policies.keys())
            all_tables = retention_tables.union(archive_tables)

            if not all_tables:
                print("❌ No tables configured")
                return

            print(
                f"\n{'Table':<25} {'Retention':<12} {'Archive':<12} {'Hot Days':<10} {'Delete Days'}"
            )
            print("-" * 75)

            for table_name in sorted(all_tables):
                retention_rule = self.config_manager.retention_rules.get(table_name)
                archive_policy = self.config_manager.archive_policies.get(table_name)

                retention_status = (
                    "Enabled"
                    if retention_rule and retention_rule.enabled
                    else "Disabled"
                )
                archive_status = (
                    "Enabled"
                    if archive_policy and archive_policy.enabled
                    else "Disabled"
                )
                hot_days = retention_rule.hot_days if retention_rule else "N/A"
                delete_days = (
                    retention_rule.delete_after_days if retention_rule else "Never"
                )

                print(
                    f"{table_name:<25} {retention_status:<12} {archive_status:<12} {hot_days:<10} {delete_days}"
                )

            # Show summary for each table if detailed view requested
            if args.detailed:
                for table_name in sorted(all_tables):
                    print(f"\n📊 {table_name}:")
                    summary = self.config_manager.get_table_lifecycle_summary(
                        table_name
                    )

                    print(
                        f"   Retention: {summary['retention']['hot_days']}/{summary['retention']['warm_days']}/{summary['retention']['cold_days']} days"
                    )
                    if summary["retention"]["archive_after_days"]:
                        print(
                            f"   Archive after: {summary['retention']['archive_after_days']} days"
                        )
                    if summary["retention"]["delete_after_days"]:
                        print(
                            f"   Delete after: {summary['retention']['delete_after_days']} days"
                        )

                    print(
                        f"   Archive: {summary['archiving']['format']} format, {summary['archiving']['compression']} compression"
                    )
                    print(f"   Path: {summary['archiving']['archive_path']}")

        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data Lifecycle Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Apply retention policies (dry run)
  python -m src.data_lake.lifecycle_cli retention --dry-run

  # Apply retention to specific tables
  python -m src.data_lake.lifecycle_cli retention --tables transactions user_events

  # Show lineage for a table
  python -m src.data_lake.lifecycle_cli lineage --table transactions --depth 2

  # Optimize storage costs
  python -m src.data_lake.lifecycle_cli optimize --output storage_analysis.json

  # Generate comprehensive report
  python -m src.data_lake.lifecycle_cli report --output lifecycle_report.json

  # Configure table retention policy
  python -m src.data_lake.lifecycle_cli config --table transactions --hot-days 30 --warm-days 90 --cold-days 365

  # List all configured tables
  python -m src.data_lake.lifecycle_cli list --detailed
        """,
    )

    # Global arguments
    parser.add_argument(
        "--base-path",
        default="s3a://data-lake/delta",
        help="Base path for Delta Lake tables",
    )
    parser.add_argument("--config", help="Path to lifecycle configuration file")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Retention command
    retention_parser = subparsers.add_parser(
        "retention", help="Apply retention policies"
    )
    retention_parser.add_argument(
        "--tables", nargs="+", help="Specific tables to process"
    )
    retention_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    retention_parser.add_argument("--output", help="Output file for results")
    retention_parser.set_defaults(func="apply_retention")

    # Lineage command
    lineage_parser = subparsers.add_parser("lineage", help="Show data lineage")
    lineage_parser.add_argument(
        "--table", required=True, help="Table to trace lineage for"
    )
    lineage_parser.add_argument(
        "--depth", type=int, default=3, help="Maximum lineage depth"
    )
    lineage_parser.add_argument("--output", help="Output file for lineage graph")
    lineage_parser.set_defaults(func="show_lineage")

    # Optimize command
    optimize_parser = subparsers.add_parser("optimize", help="Optimize storage costs")
    optimize_parser.add_argument(
        "--strategy",
        choices=["cost_optimized", "performance_optimized", "compliance_optimized"],
        default="cost_optimized",
        help="Optimization strategy",
    )
    optimize_parser.add_argument(
        "--output", help="Output file for optimization analysis"
    )
    optimize_parser.set_defaults(func="optimize_storage")

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate lifecycle report")
    report_parser.add_argument("--output", help="Output file for report")
    report_parser.set_defaults(func="generate_report")

    # Config command
    config_parser = subparsers.add_parser(
        "config", help="Configure table lifecycle policies"
    )
    config_parser.add_argument("--table", required=True, help="Table name to configure")
    config_parser.add_argument("--hot-days", type=int, help="Days to keep data hot")
    config_parser.add_argument("--warm-days", type=int, help="Days to keep data warm")
    config_parser.add_argument("--cold-days", type=int, help="Days to keep data cold")
    config_parser.add_argument("--archive-days", type=int, help="Days before archiving")
    config_parser.add_argument("--delete-days", type=int, help="Days before deletion")
    config_parser.add_argument(
        "--partition-column", help="Partition column for time-based retention"
    )
    config_parser.add_argument("--archive-path", help="Archive storage path")
    config_parser.add_argument(
        "--archive-format", choices=["parquet", "orc", "delta"], help="Archive format"
    )
    config_parser.add_argument(
        "--compression",
        choices=["gzip", "snappy", "lz4", "zstd"],
        help="Compression type",
    )
    config_parser.add_argument(
        "--partition-by", nargs="+", help="Archive partition columns"
    )
    config_parser.add_argument("--enabled", type=bool, help="Enable/disable policies")
    config_parser.add_argument("--save-config", help="Save configuration to file")
    config_parser.set_defaults(func="config_table")

    # List command
    list_parser = subparsers.add_parser("list", help="List configured tables")
    list_parser.add_argument(
        "--detailed", action="store_true", help="Show detailed configuration"
    )
    list_parser.set_defaults(func="list_tables")

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Initialize CLI
    cli = LifecycleCLI()

    try:
        # Setup managers
        cli.setup_managers(args.base_path)

        # Load configuration if provided
        if args.config:
            cli.config_manager.import_configuration(args.config)
            print(f"📄 Loaded configuration from: {args.config}")

        # Execute command
        func = getattr(cli, args.func)
        func(args)

    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"CLI error: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

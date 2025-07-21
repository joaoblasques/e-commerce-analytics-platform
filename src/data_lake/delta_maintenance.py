"""
Delta Lake Maintenance and Optimization

Provides automated maintenance, optimization, and vacuum operations
for Delta Lake tables to ensure optimal performance and cost efficiency.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from ..utils.logger import setup_logging
from .delta import DeltaLakeManager
from .delta_config import DeltaMaintenanceScheduler, DeltaOptimizationStrategies

logger = setup_logging(__name__)


class DeltaMaintenanceManager:
    """
    Manages automated maintenance operations for Delta Lake tables including
    optimization, vacuum, Z-ordering, and performance monitoring.
    """

    def __init__(
        self,
        delta_manager: DeltaLakeManager,
        maintenance_base_path: str = "s3a://data-lake/maintenance",
    ):
        """
        Initialize Delta Maintenance Manager.

        Args:
            delta_manager: Delta Lake manager instance
            maintenance_base_path: Base path for maintenance logs and metadata
        """
        self.delta_manager = delta_manager
        self.spark = delta_manager.spark
        self.maintenance_path = maintenance_base_path
        self.maintenance_history: List[Dict[str, Any]] = []

        logger.info("Delta Maintenance Manager initialized")
        logger.info(f"Maintenance base path: {maintenance_base_path}")

    def run_optimization_job(
        self,
        table_name: str,
        optimization_type: str = "compact",
        where_clause: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run optimization job on a Delta table.

        Args:
            table_name: Table name to optimize
            optimization_type: Type of optimization ('compact', 'zorder', 'both')
            where_clause: Optional where clause for selective optimization
            z_order_columns: Columns for Z-ordering

        Returns:
            Optimization results and metrics
        """
        start_time = datetime.now()
        logger.info(
            f"Starting {optimization_type} optimization for table '{table_name}'"
        )

        # Pre-optimization metrics
        pre_metrics = self._collect_table_metrics(table_name)

        try:
            if optimization_type in ["compact", "both"]:
                # File compaction
                self.delta_manager.optimize_table(
                    table_name=table_name,
                    where_clause=where_clause,
                )
                logger.info(f"Compaction completed for table '{table_name}'")

            if optimization_type in ["zorder", "both"] and z_order_columns:
                # Z-order optimization
                self.delta_manager.optimize_table(
                    table_name=table_name,
                    where_clause=where_clause,
                    z_order_columns=z_order_columns,
                )
                logger.info(f"Z-order optimization completed for table '{table_name}'")

            # Post-optimization metrics
            post_metrics = self._collect_table_metrics(table_name)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Calculate improvements
            improvement_metrics = self._calculate_optimization_improvements(
                pre_metrics, post_metrics
            )

            result = {
                "table_name": table_name,
                "optimization_type": optimization_type,
                "status": "success",
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "where_clause": where_clause,
                "z_order_columns": z_order_columns,
                "pre_optimization_metrics": pre_metrics,
                "post_optimization_metrics": post_metrics,
                "improvements": improvement_metrics,
            }

            # Log the operation
            self.maintenance_history.append(result)
            self._log_maintenance_operation(result)

            logger.info(f"Optimization job completed successfully for '{table_name}'")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(
                f"File count reduction: {improvement_metrics.get('file_count_reduction', 0)}"
            )

            return result

        except Exception as e:
            error_result = {
                "table_name": table_name,
                "optimization_type": optimization_type,
                "status": "failed",
                "start_time": start_time,
                "end_time": datetime.now(),
                "error": str(e),
                "where_clause": where_clause,
                "z_order_columns": z_order_columns,
            }

            self.maintenance_history.append(error_result)
            self._log_maintenance_operation(error_result)

            logger.error(f"Optimization job failed for '{table_name}': {e}")
            return error_result

    def run_vacuum_job(
        self,
        table_name: str,
        retention_hours: int = 168,  # 7 days
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Run vacuum job on a Delta table.

        Args:
            table_name: Table name to vacuum
            retention_hours: Retention period in hours
            dry_run: Whether to perform dry run

        Returns:
            Vacuum results and metrics
        """
        start_time = datetime.now()
        logger.info(f"Starting vacuum job for table '{table_name}'")
        logger.info(f"Retention hours: {retention_hours}, Dry run: {dry_run}")

        try:
            # Pre-vacuum metrics
            pre_metrics = self._collect_table_metrics(table_name)

            # Run vacuum
            vacuum_result = self.delta_manager.vacuum_table(
                table_name=table_name,
                retention_hours=retention_hours,
                dry_run=dry_run,
            )

            # Post-vacuum metrics (if not dry run)
            post_metrics = pre_metrics
            if not dry_run:
                post_metrics = self._collect_table_metrics(table_name)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "table_name": table_name,
                "operation_type": "vacuum",
                "status": "success",
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "retention_hours": retention_hours,
                "dry_run": dry_run,
                "vacuum_result": vacuum_result.collect() if vacuum_result else [],
                "pre_vacuum_metrics": pre_metrics,
                "post_vacuum_metrics": post_metrics,
            }

            # Log the operation
            self.maintenance_history.append(result)
            self._log_maintenance_operation(result)

            logger.info(f"Vacuum job completed successfully for '{table_name}'")
            logger.info(f"Duration: {duration:.2f} seconds")

            return result

        except Exception as e:
            error_result = {
                "table_name": table_name,
                "operation_type": "vacuum",
                "status": "failed",
                "start_time": start_time,
                "end_time": datetime.now(),
                "error": str(e),
                "retention_hours": retention_hours,
                "dry_run": dry_run,
            }

            self.maintenance_history.append(error_result)
            self._log_maintenance_operation(error_result)

            logger.error(f"Vacuum job failed for '{table_name}': {e}")
            return error_result

    def run_automated_maintenance(
        self,
        table_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Run automated maintenance based on predefined schedules and thresholds.

        Args:
            table_configs: Custom table configurations for maintenance

        Returns:
            Results for all maintenance operations
        """
        logger.info("Starting automated maintenance for all tables")
        results = {
            "optimization_results": [],
            "vacuum_results": [],
            "skipped_tables": [],
        }

        # Default maintenance configurations
        if table_configs is None:
            table_configs = self._get_default_maintenance_configs()

        for table_name in self.delta_manager.tables.keys():
            table_config = table_configs.get(table_name, {})

            # Check if maintenance is needed
            maintenance_needed = self._assess_maintenance_needs(
                table_name, table_config
            )

            if not maintenance_needed["needs_maintenance"]:
                results["skipped_tables"].append(
                    {
                        "table_name": table_name,
                        "reason": "maintenance not needed",
                        "assessment": maintenance_needed,
                    }
                )
                continue

            # Run optimization if needed
            if maintenance_needed["needs_optimization"]:
                opt_result = self.run_optimization_job(
                    table_name=table_name,
                    optimization_type=table_config.get("optimization_type", "compact"),
                    z_order_columns=table_config.get("z_order_columns"),
                )
                results["optimization_results"].append(opt_result)

            # Run vacuum if needed
            if maintenance_needed["needs_vacuum"]:
                vacuum_result = self.run_vacuum_job(
                    table_name=table_name,
                    retention_hours=table_config.get("retention_hours", 168),
                    dry_run=False,
                )
                results["vacuum_results"].append(vacuum_result)

        logger.info("Automated maintenance completed")
        logger.info(f"Optimized {len(results['optimization_results'])} tables")
        logger.info(f"Vacuumed {len(results['vacuum_results'])} tables")
        logger.info(f"Skipped {len(results['skipped_tables'])} tables")

        return results

    def schedule_maintenance_jobs(
        self,
        schedule_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Schedule maintenance jobs based on table types and usage patterns.

        Args:
            schedule_config: Custom scheduling configuration

        Returns:
            Scheduling plan and next run times
        """
        if schedule_config is None:
            schedule_config = DeltaMaintenanceScheduler.get_optimization_schedule()

        scheduling_plan = {
            "created_at": datetime.now(),
            "tables": {},
            "next_runs": {
                "optimize": [],
                "vacuum": [],
            },
        }

        for table_name in self.delta_manager.tables.keys():
            # Determine table type based on characteristics
            table_type = self._classify_table_type(table_name)

            # Create maintenance plan
            maintenance_plan = DeltaMaintenanceScheduler.create_maintenance_plan(
                table_type=table_type,
                table_names=[table_name],
            )

            scheduling_plan["tables"][table_name] = {
                "table_type": table_type,
                "maintenance_plan": maintenance_plan,
            }

            # Add to next runs
            scheduling_plan["next_runs"]["optimize"].append(
                {
                    "table_name": table_name,
                    "next_run": maintenance_plan["next_optimize"],
                    "frequency": maintenance_plan["schedule"]["optimize_frequency"],
                }
            )

            scheduling_plan["next_runs"]["vacuum"].append(
                {
                    "table_name": table_name,
                    "next_run": maintenance_plan["next_vacuum"],
                    "frequency": maintenance_plan["schedule"]["vacuum_frequency"],
                }
            )

        logger.info(
            f"Scheduled maintenance for {len(scheduling_plan['tables'])} tables"
        )
        return scheduling_plan

    def get_maintenance_recommendations(
        self,
        table_name: str,
    ) -> Dict[str, Any]:
        """
        Get maintenance recommendations for a specific table.

        Args:
            table_name: Table name to analyze

        Returns:
            Maintenance recommendations and rationale
        """
        logger.info(f"Analyzing maintenance recommendations for '{table_name}'")

        # Collect current table metrics
        metrics = self._collect_table_metrics(table_name)

        # Analyze table characteristics
        analysis = self._analyze_table_characteristics(table_name, metrics)

        recommendations = {
            "table_name": table_name,
            "analysis_date": datetime.now(),
            "current_metrics": metrics,
            "analysis": analysis,
            "recommendations": [],
            "priority_score": 0,
        }

        # File count recommendations
        if metrics.get("file_count", 0) > 1000:
            recommendations["recommendations"].append(
                {
                    "type": "optimization",
                    "action": "compact",
                    "priority": "high",
                    "reason": f"High file count ({metrics['file_count']}) affects query performance",
                    "expected_benefit": "Improved query performance and reduced metadata overhead",
                }
            )
            recommendations["priority_score"] += 30

        # File size recommendations
        avg_file_size_mb = metrics.get("avg_file_size_mb", 0)
        if avg_file_size_mb < 50:
            recommendations["recommendations"].append(
                {
                    "type": "optimization",
                    "action": "compact",
                    "priority": "medium",
                    "reason": f"Small average file size ({avg_file_size_mb:.1f}MB) affects efficiency",
                    "expected_benefit": "Better I/O efficiency and reduced overhead",
                }
            )
            recommendations["priority_score"] += 15

        # Z-order recommendations
        if analysis.get("query_patterns", {}).get("needs_zorder", False):
            recommendations["recommendations"].append(
                {
                    "type": "optimization",
                    "action": "zorder",
                    "priority": "medium",
                    "reason": "Query patterns indicate benefit from Z-ordering",
                    "expected_benefit": "Improved data skipping and query performance",
                    "suggested_columns": analysis["query_patterns"][
                        "suggested_zorder_columns"
                    ],
                }
            )
            recommendations["priority_score"] += 20

        # Vacuum recommendations
        if metrics.get("deleted_files_count", 0) > 100:
            recommendations["recommendations"].append(
                {
                    "type": "vacuum",
                    "action": "vacuum",
                    "priority": "low",
                    "reason": f"High number of deleted files ({metrics['deleted_files_count']}) consuming storage",
                    "expected_benefit": "Reduced storage costs and improved metadata performance",
                }
            )
            recommendations["priority_score"] += 10

        # Determine overall priority
        if recommendations["priority_score"] >= 40:
            recommendations["overall_priority"] = "high"
        elif recommendations["priority_score"] >= 20:
            recommendations["overall_priority"] = "medium"
        else:
            recommendations["overall_priority"] = "low"

        logger.info(
            f"Generated {len(recommendations['recommendations'])} recommendations"
        )
        logger.info(f"Overall priority: {recommendations['overall_priority']}")

        return recommendations

    def _collect_table_metrics(self, table_name: str) -> Dict[str, Any]:
        """Collect comprehensive metrics for a Delta table."""
        if table_name not in self.delta_manager.tables:
            return {}

        try:
            # Get table info
            table_info = self.delta_manager.get_table_info(table_name)

            # Get detailed file metrics using Delta Lake APIs
            table_path = self.delta_manager.tables[table_name]
            detail_df = self.spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
            detail_info = detail_df.collect()[0] if not detail_df.isEmpty() else None

            metrics = {
                "table_name": table_name,
                "record_count": table_info.get("current_count", 0),
                "file_count": detail_info["numFiles"] if detail_info else 0,
                "table_size_bytes": detail_info["sizeInBytes"] if detail_info else 0,
                "latest_version": table_info.get("latest_version", 0),
                "collection_timestamp": datetime.now(),
            }

            # Calculate derived metrics
            if metrics["file_count"] > 0 and metrics["table_size_bytes"] > 0:
                metrics["avg_file_size_mb"] = (
                    metrics["table_size_bytes"] / metrics["file_count"]
                ) / (1024 * 1024)
            else:
                metrics["avg_file_size_mb"] = 0

            # Get history-based metrics
            history_df = self.delta_manager.get_table_history(table_name, limit=10)
            if not history_df.isEmpty():
                recent_operations = [row.asDict() for row in history_df.collect()]
                metrics["recent_operations"] = len(recent_operations)
                metrics["last_optimization"] = None

                # Find last optimization
                for op in recent_operations:
                    if op.get("operation") in ["OPTIMIZE", "ZORDER"]:
                        metrics["last_optimization"] = op.get("timestamp")
                        break

            return metrics

        except Exception as e:
            logger.error(f"Failed to collect metrics for table '{table_name}': {e}")
            return {"error": str(e), "table_name": table_name}

    def _calculate_optimization_improvements(
        self, pre_metrics: Dict[str, Any], post_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate improvements from optimization."""
        improvements = {}

        # File count improvement
        pre_files = pre_metrics.get("file_count", 0)
        post_files = post_metrics.get("file_count", 0)
        improvements["file_count_reduction"] = pre_files - post_files
        improvements["file_count_reduction_percent"] = (
            ((pre_files - post_files) / pre_files * 100) if pre_files > 0 else 0
        )

        # File size improvement
        pre_avg_size = pre_metrics.get("avg_file_size_mb", 0)
        post_avg_size = post_metrics.get("avg_file_size_mb", 0)
        improvements["avg_file_size_improvement_mb"] = post_avg_size - pre_avg_size
        improvements["avg_file_size_improvement_percent"] = (
            ((post_avg_size - pre_avg_size) / pre_avg_size * 100)
            if pre_avg_size > 0
            else 0
        )

        return improvements

    def _assess_maintenance_needs(
        self, table_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess if a table needs maintenance based on metrics and configuration."""
        metrics = self._collect_table_metrics(table_name)

        assessment = {
            "table_name": table_name,
            "needs_maintenance": False,
            "needs_optimization": False,
            "needs_vacuum": False,
            "reasons": [],
        }

        # Check optimization needs
        file_count_threshold = config.get("max_files", 1000)
        if metrics.get("file_count", 0) > file_count_threshold:
            assessment["needs_optimization"] = True
            assessment["needs_maintenance"] = True
            assessment["reasons"].append(
                f"File count ({metrics['file_count']}) exceeds threshold ({file_count_threshold})"
            )

        # Check file size
        min_file_size_mb = config.get("min_avg_file_size_mb", 64)
        if metrics.get("avg_file_size_mb", 0) < min_file_size_mb:
            assessment["needs_optimization"] = True
            assessment["needs_maintenance"] = True
            assessment["reasons"].append(
                f"Average file size ({metrics.get('avg_file_size_mb', 0):.1f}MB) below threshold ({min_file_size_mb}MB)"
            )

        # Check vacuum needs
        vacuum_interval_hours = config.get("vacuum_interval_hours", 168)  # 7 days
        last_vacuum = config.get("last_vacuum")
        if last_vacuum:
            hours_since_vacuum = (datetime.now() - last_vacuum).total_seconds() / 3600
            if hours_since_vacuum > vacuum_interval_hours:
                assessment["needs_vacuum"] = True
                assessment["needs_maintenance"] = True
                assessment["reasons"].append(
                    f"Vacuum needed: {hours_since_vacuum:.1f} hours since last vacuum"
                )
        else:
            # No vacuum history, consider it needed
            assessment["needs_vacuum"] = True
            assessment["needs_maintenance"] = True
            assessment["reasons"].append("No vacuum history found")

        return assessment

    def _get_default_maintenance_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get default maintenance configurations for all tables."""
        configs = {}

        # Transaction table (high frequency)
        configs["transactions"] = {
            "max_files": 500,
            "min_avg_file_size_mb": 128,
            "optimization_type": "both",
            "z_order_columns": ["customer_id", "timestamp"],
            "vacuum_interval_hours": 72,  # 3 days
            "retention_hours": 168,  # 7 days
        }

        # User events table (very high frequency)
        configs["user_events"] = {
            "max_files": 1000,
            "min_avg_file_size_mb": 64,
            "optimization_type": "both",
            "z_order_columns": ["user_id", "event_type"],
            "vacuum_interval_hours": 48,  # 2 days
            "retention_hours": 72,  # 3 days
        }

        # Customer profiles (lower frequency, more stable)
        configs["customer_profiles"] = {
            "max_files": 100,
            "min_avg_file_size_mb": 256,
            "optimization_type": "compact",
            "vacuum_interval_hours": 168,  # 7 days
            "retention_hours": 720,  # 30 days
        }

        # Default configuration for other tables
        default_config = {
            "max_files": 200,
            "min_avg_file_size_mb": 128,
            "optimization_type": "compact",
            "vacuum_interval_hours": 168,
            "retention_hours": 168,
        }

        # Apply default config to tables not explicitly configured
        for table_name in self.delta_manager.tables.keys():
            if table_name not in configs:
                configs[table_name] = default_config.copy()

        return configs

    def _classify_table_type(self, table_name: str) -> str:
        """Classify table type based on name and characteristics."""
        if "transaction" in table_name.lower():
            return "high_frequency_streaming"
        elif "user_event" in table_name.lower() or "event" in table_name.lower():
            return "high_frequency_streaming"
        elif "profile" in table_name.lower() or "customer" in table_name.lower():
            return "moderate_frequency_streaming"
        elif "analytics" in table_name.lower() or "result" in table_name.lower():
            return "analytical_workload"
        else:
            return "batch_processing"

    def _analyze_table_characteristics(
        self, table_name: str, metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze table characteristics for optimization recommendations."""
        analysis = {
            "table_size_category": "small",
            "file_organization": "needs_improvement",
            "query_patterns": {
                "needs_zorder": False,
                "suggested_zorder_columns": [],
            },
        }

        # Categorize table size
        table_size_gb = metrics.get("table_size_bytes", 0) / (1024**3)
        if table_size_gb > 100:
            analysis["table_size_category"] = "large"
        elif table_size_gb > 10:
            analysis["table_size_category"] = "medium"

        # Analyze file organization
        file_count = metrics.get("file_count", 0)
        avg_file_size_mb = metrics.get("avg_file_size_mb", 0)

        if file_count < 100 and avg_file_size_mb > 128:
            analysis["file_organization"] = "good"
        elif file_count > 1000 or avg_file_size_mb < 32:
            analysis["file_organization"] = "poor"

        # Suggest Z-order columns based on table type
        if table_name in ["transactions", "user_events"]:
            analysis["query_patterns"]["needs_zorder"] = True
            if table_name == "transactions":
                analysis["query_patterns"]["suggested_zorder_columns"] = [
                    "customer_id",
                    "timestamp",
                ]
            elif table_name == "user_events":
                analysis["query_patterns"]["suggested_zorder_columns"] = [
                    "user_id",
                    "event_type",
                ]

        return analysis

    def _log_maintenance_operation(self, operation_result: Dict[str, Any]) -> None:
        """Log maintenance operation to persistent storage."""
        try:
            # Create maintenance log entry
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "operation": operation_result,
            }

            # In a real implementation, this would write to a persistent log
            # For now, we'll just log to the application logger
            logger.info(
                f"Maintenance operation logged: {json.dumps(log_entry, default=str, indent=2)}"
            )

        except Exception as e:
            logger.error(f"Failed to log maintenance operation: {e}")

    def get_maintenance_history(
        self,
        table_name: Optional[str] = None,
        operation_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get maintenance operation history.

        Args:
            table_name: Filter by table name
            operation_type: Filter by operation type
            limit: Maximum number of results

        Returns:
            List of maintenance operations
        """
        history = self.maintenance_history.copy()

        # Apply filters
        if table_name:
            history = [op for op in history if op.get("table_name") == table_name]

        if operation_type:
            history = [
                op
                for op in history
                if op.get("optimization_type") == operation_type
                or op.get("operation_type") == operation_type
            ]

        # Sort by timestamp (most recent first)
        history.sort(key=lambda x: x.get("start_time", datetime.min), reverse=True)

        return history[:limit]

    def close(self) -> None:
        """Clean up resources."""
        logger.info("Delta Maintenance Manager closed")

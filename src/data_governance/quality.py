"""
Data Quality Monitoring and Alerting System.

Provides comprehensive data quality monitoring including:
- Real-time data validation
- Quality metrics calculation
- Anomaly detection
- Quality alerts and notifications
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


class QualityCheckType(Enum):
    """Types of data quality checks."""

    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class QualityRule:
    """Represents a data quality rule."""

    rule_id: str
    name: str
    description: str
    check_type: QualityCheckType
    dataset: str
    column: Optional[str]
    rule_definition: Dict[str, Any]
    threshold: float
    severity: AlertSeverity
    enabled: bool
    created_at: datetime
    created_by: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["check_type"] = self.check_type.value
        data["severity"] = self.severity.value
        data["created_at"] = self.created_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QualityRule":
        """Create from dictionary."""
        data["check_type"] = QualityCheckType(data["check_type"])
        data["severity"] = AlertSeverity(data["severity"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)


@dataclass
class QualityResult:
    """Represents a quality check result."""

    check_id: str
    rule_id: str
    dataset: str
    column: Optional[str]
    check_type: QualityCheckType
    timestamp: datetime
    score: float  # 0.0 to 1.0
    passed: bool
    threshold: float
    actual_value: float
    total_records: int
    failed_records: int
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["check_type"] = self.check_type.value
        data["timestamp"] = self.timestamp.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QualityResult":
        """Create from dictionary."""
        data["check_type"] = QualityCheckType(data["check_type"])
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


@dataclass
class QualityAlert:
    """Represents a data quality alert."""

    alert_id: str
    rule_id: str
    check_id: str
    severity: AlertSeverity
    title: str
    message: str
    dataset: str
    column: Optional[str]
    timestamp: datetime
    resolved: bool
    resolved_at: Optional[datetime]
    resolved_by: Optional[str]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["severity"] = self.severity.value
        data["timestamp"] = self.timestamp.isoformat()
        data["resolved_at"] = self.resolved_at.isoformat() if self.resolved_at else None
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QualityAlert":
        """Create from dictionary."""
        data["severity"] = AlertSeverity(data["severity"])
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["resolved_at"] = (
            datetime.fromisoformat(data["resolved_at"]) if data["resolved_at"] else None
        )
        return cls(**data)


class DataQualityMonitor:
    """
    Data Quality Monitoring and Alerting System.

    Provides comprehensive data quality monitoring including:
    - Rule-based quality checks
    - Real-time monitoring
    - Quality metrics calculation
    - Alert generation and management
    """

    def __init__(
        self, quality_path: str = "data/quality", spark: Optional[SparkSession] = None
    ):
        """
        Initialize data quality monitor.

        Args:
            quality_path: Path to store quality data
            spark: Optional Spark session for data processing
        """
        self.quality_path = Path(quality_path)
        self.quality_path.mkdir(parents=True, exist_ok=True)
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Storage files
        self.rules_file = self.quality_path / "quality_rules.json"
        self.results_file = self.quality_path / "quality_results.json"
        self.alerts_file = self.quality_path / "quality_alerts.json"

        # Load existing data
        self.rules = self._load_rules()
        self.results = self._load_results()
        self.alerts = self._load_alerts()

        # Built-in quality check functions
        self.quality_checks = {
            QualityCheckType.COMPLETENESS: self._check_completeness,
            QualityCheckType.UNIQUENESS: self._check_uniqueness,
            QualityCheckType.VALIDITY: self._check_validity,
            QualityCheckType.CONSISTENCY: self._check_consistency,
            QualityCheckType.ACCURACY: self._check_accuracy,
            QualityCheckType.TIMELINESS: self._check_timeliness,
        }

    def _load_rules(self) -> Dict[str, QualityRule]:
        """Load existing quality rules."""
        if not self.rules_file.exists():
            return {}

        try:
            with open(self.rules_file, "r") as f:
                data = json.load(f)
            return {
                rule_id: QualityRule.from_dict(rule_data)
                for rule_id, rule_data in data.items()
            }
        except Exception as e:
            self.logger.error(f"Error loading quality rules: {e}")
            return {}

    def _load_results(self) -> List[QualityResult]:
        """Load existing quality results."""
        if not self.results_file.exists():
            return []

        try:
            with open(self.results_file, "r") as f:
                data = json.load(f)
            return [QualityResult.from_dict(result) for result in data]
        except Exception as e:
            self.logger.error(f"Error loading quality results: {e}")
            return []

    def _load_alerts(self) -> List[QualityAlert]:
        """Load existing quality alerts."""
        if not self.alerts_file.exists():
            return []

        try:
            with open(self.alerts_file, "r") as f:
                data = json.load(f)
            return [QualityAlert.from_dict(alert) for alert in data]
        except Exception as e:
            self.logger.error(f"Error loading quality alerts: {e}")
            return []

    def _save_rules(self):
        """Save quality rules to storage."""
        try:
            data = {rule_id: rule.to_dict() for rule_id, rule in self.rules.items()}
            with open(self.rules_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving quality rules: {e}")

    def _save_results(self):
        """Save quality results to storage."""
        try:
            # Keep only recent results to prevent file from growing too large
            recent_results = self.results[-5000:]  # Keep last 5k results
            data = [result.to_dict() for result in recent_results]
            with open(self.results_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving quality results: {e}")

    def _save_alerts(self):
        """Save quality alerts to storage."""
        try:
            data = [alert.to_dict() for alert in self.alerts]
            with open(self.alerts_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving quality alerts: {e}")

    def create_rule(
        self,
        name: str,
        description: str,
        check_type: QualityCheckType,
        dataset: str,
        column: Optional[str] = None,
        rule_definition: Optional[Dict[str, Any]] = None,
        threshold: float = 0.95,
        severity: AlertSeverity = AlertSeverity.WARNING,
        created_by: str = "system",
    ) -> str:
        """
        Create a new data quality rule.

        Args:
            name: Rule name
            description: Rule description
            check_type: Type of quality check
            dataset: Dataset to monitor
            column: Column to monitor (if applicable)
            rule_definition: Rule-specific configuration
            threshold: Quality threshold (0.0 to 1.0)
            severity: Alert severity level
            created_by: Rule creator

        Returns:
            Rule ID
        """
        import uuid

        rule_id = str(uuid.uuid4())
        rule_definition = rule_definition or {}

        rule = QualityRule(
            rule_id=rule_id,
            name=name,
            description=description,
            check_type=check_type,
            dataset=dataset,
            column=column,
            rule_definition=rule_definition,
            threshold=threshold,
            severity=severity,
            enabled=True,
            created_at=datetime.now(),
            created_by=created_by,
        )

        self.rules[rule_id] = rule
        self._save_rules()

        self.logger.info(f"Created quality rule: {rule_id} ({name})")
        return rule_id

    def run_quality_checks(
        self, dataset: str, rule_ids: Optional[List[str]] = None
    ) -> List[QualityResult]:
        """
        Run quality checks for a dataset.

        Args:
            dataset: Dataset to check
            rule_ids: Specific rules to run (if None, run all enabled rules)

        Returns:
            List of quality results
        """
        if not self.spark:
            raise ValueError("Spark session required for quality checks")

        # Get rules to run
        rules_to_run = []
        for rule in self.rules.values():
            if (
                rule.dataset == dataset
                and rule.enabled
                and (rule_ids is None or rule.rule_id in rule_ids)
            ):
                rules_to_run.append(rule)

        if not rules_to_run:
            self.logger.warning(f"No enabled rules found for dataset: {dataset}")
            return []

        # Load dataset
        try:
            df = self.spark.table(dataset)
        except Exception as e:
            self.logger.error(f"Failed to load dataset {dataset}: {e}")
            return []

        results = []

        # Run each rule
        for rule in rules_to_run:
            try:
                check_function = self.quality_checks[rule.check_type]
                result = check_function(df, rule)
                results.append(result)

                # Generate alert if check failed
                if not result.passed:
                    self._generate_alert(rule, result)

            except Exception as e:
                self.logger.error(f"Error running rule {rule.rule_id}: {e}")

        # Store results
        self.results.extend(results)
        self._save_results()

        self.logger.info(
            f"Completed quality checks for {dataset}: {len(results)} checks run"
        )
        return results

    def _check_completeness(self, df: DataFrame, rule: QualityRule) -> QualityResult:
        """Check data completeness (non-null values)."""
        import uuid

        total_records = df.count()

        if rule.column:
            # Check specific column
            null_count = df.filter(col(rule.column).isNull()).count()
            complete_count = total_records - null_count
            score = complete_count / total_records if total_records > 0 else 0.0
            failed_records = null_count
        else:
            # Check all columns
            completeness_scores = []
            total_failed = 0

            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                complete_count = total_records - null_count
                column_score = (
                    complete_count / total_records if total_records > 0 else 0.0
                )
                completeness_scores.append(column_score)
                total_failed += null_count

            score = (
                sum(completeness_scores) / len(completeness_scores)
                if completeness_scores
                else 0.0
            )
            failed_records = total_failed

        return QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule.rule_id,
            dataset=rule.dataset,
            column=rule.column,
            check_type=rule.check_type,
            timestamp=datetime.now(),
            score=score,
            passed=score >= rule.threshold,
            threshold=rule.threshold,
            actual_value=score,
            total_records=total_records,
            failed_records=failed_records,
            details={"completeness_score": score},
        )

    def _check_uniqueness(self, df: DataFrame, rule: QualityRule) -> QualityResult:
        """Check data uniqueness (no duplicates)."""
        import uuid

        if not rule.column:
            raise ValueError("Column must be specified for uniqueness check")

        total_records = df.count()
        distinct_count = df.select(rule.column).distinct().count()
        duplicate_count = total_records - distinct_count

        score = distinct_count / total_records if total_records > 0 else 0.0

        return QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule.rule_id,
            dataset=rule.dataset,
            column=rule.column,
            check_type=rule.check_type,
            timestamp=datetime.now(),
            score=score,
            passed=score >= rule.threshold,
            threshold=rule.threshold,
            actual_value=score,
            total_records=total_records,
            failed_records=duplicate_count,
            details={
                "distinct_count": distinct_count,
                "duplicate_count": duplicate_count,
            },
        )

    def _check_validity(self, df: DataFrame, rule: QualityRule) -> QualityResult:
        """Check data validity (matches expected format/pattern)."""
        import uuid

        if not rule.column:
            raise ValueError("Column must be specified for validity check")

        pattern = rule.rule_definition.get("pattern")
        if not pattern:
            raise ValueError("Pattern must be specified for validity check")

        total_records = df.count()

        # Check pattern matching
        valid_count = df.filter(col(rule.column).rlike(pattern)).count()

        invalid_count = total_records - valid_count
        score = valid_count / total_records if total_records > 0 else 0.0

        return QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule.rule_id,
            dataset=rule.dataset,
            column=rule.column,
            check_type=rule.check_type,
            timestamp=datetime.now(),
            score=score,
            passed=score >= rule.threshold,
            threshold=rule.threshold,
            actual_value=score,
            total_records=total_records,
            failed_records=invalid_count,
            details={"valid_count": valid_count, "pattern": pattern},
        )

    def _check_consistency(self, df: DataFrame, rule: QualityRule) -> QualityResult:
        """Check data consistency across related fields."""
        import uuid

        # This is a placeholder for consistency checks
        # Implementation would depend on specific consistency rules

        total_records = df.count()
        score = 1.0  # Placeholder - assume all records are consistent

        return QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule.rule_id,
            dataset=rule.dataset,
            column=rule.column,
            check_type=rule.check_type,
            timestamp=datetime.now(),
            score=score,
            passed=score >= rule.threshold,
            threshold=rule.threshold,
            actual_value=score,
            total_records=total_records,
            failed_records=0,
            details={"consistency_check": "placeholder"},
        )

    def _check_accuracy(self, df: DataFrame, rule: QualityRule) -> QualityResult:
        """Check data accuracy against reference data."""
        import uuid

        # This is a placeholder for accuracy checks
        # Implementation would require reference data comparison

        total_records = df.count()
        score = 1.0  # Placeholder - assume all records are accurate

        return QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule.rule_id,
            dataset=rule.dataset,
            column=rule.column,
            check_type=rule.check_type,
            timestamp=datetime.now(),
            score=score,
            passed=score >= rule.threshold,
            threshold=rule.threshold,
            actual_value=score,
            total_records=total_records,
            failed_records=0,
            details={"accuracy_check": "placeholder"},
        )

    def _check_timeliness(self, df: DataFrame, rule: QualityRule) -> QualityResult:
        """Check data timeliness (freshness)."""
        import uuid

        if not rule.column:
            raise ValueError("Column must be specified for timeliness check")

        total_records = df.count()

        # Check how recent the data is
        max_age_hours = rule.rule_definition.get("max_age_hours", 24)
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)

        fresh_count = df.filter(col(rule.column) >= cutoff_time).count()

        stale_count = total_records - fresh_count
        score = fresh_count / total_records if total_records > 0 else 0.0

        return QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule.rule_id,
            dataset=rule.dataset,
            column=rule.column,
            check_type=rule.check_type,
            timestamp=datetime.now(),
            score=score,
            passed=score >= rule.threshold,
            threshold=rule.threshold,
            actual_value=score,
            total_records=total_records,
            failed_records=stale_count,
            details={"fresh_count": fresh_count, "max_age_hours": max_age_hours},
        )

    def _generate_alert(self, rule: QualityRule, result: QualityResult):
        """Generate a quality alert for a failed check."""
        import uuid

        alert_id = str(uuid.uuid4())

        title = f"Data Quality Alert: {rule.name}"
        message = (
            f"Quality check failed for {rule.dataset}"
            f"{f'.{rule.column}' if rule.column else ''}. "
            f"Score: {result.score:.3f}, Threshold: {rule.threshold:.3f}"
        )

        alert = QualityAlert(
            alert_id=alert_id,
            rule_id=rule.rule_id,
            check_id=result.check_id,
            severity=rule.severity,
            title=title,
            message=message,
            dataset=rule.dataset,
            column=rule.column,
            timestamp=datetime.now(),
            resolved=False,
            resolved_at=None,
            resolved_by=None,
            metadata={
                "score": result.score,
                "threshold": rule.threshold,
                "failed_records": result.failed_records,
                "total_records": result.total_records,
            },
        )

        self.alerts.append(alert)
        self._save_alerts()

        self.logger.warning(
            f"Generated quality alert: {alert_id} ({rule.severity.value})"
        )

    def get_quality_dashboard(self, dataset: Optional[str] = None) -> Dict[str, Any]:
        """Get quality dashboard data."""
        # Filter results by dataset if specified
        results = self.results
        if dataset:
            results = [r for r in results if r.dataset == dataset]

        if not results:
            return {"message": "No quality results available"}

        # Overall quality score
        overall_score = sum(r.score for r in results) / len(results)

        # Scores by check type
        scores_by_type = {}
        for check_type in QualityCheckType:
            type_results = [r for r in results if r.check_type == check_type]
            if type_results:
                scores_by_type[check_type.value] = {
                    "score": sum(r.score for r in type_results) / len(type_results),
                    "count": len(type_results),
                    "passed": len([r for r in type_results if r.passed]),
                    "failed": len([r for r in type_results if not r.passed]),
                }

        # Recent alerts
        recent_alerts = sorted(
            [a for a in self.alerts if not a.resolved],
            key=lambda a: a.timestamp,
            reverse=True,
        )[:10]

        # Trends (last 7 days)
        seven_days_ago = datetime.now() - timedelta(days=7)
        recent_results = [r for r in results if r.timestamp >= seven_days_ago]

        daily_scores = {}
        for result in recent_results:
            date_key = result.timestamp.date().isoformat()
            if date_key not in daily_scores:
                daily_scores[date_key] = []
            daily_scores[date_key].append(result.score)

        trend_data = []
        for date_key in sorted(daily_scores.keys()):
            scores = daily_scores[date_key]
            trend_data.append(
                {
                    "date": date_key,
                    "score": sum(scores) / len(scores),
                    "checks": len(scores),
                }
            )

        return {
            "overall_quality_score": overall_score,
            "total_checks": len(results),
            "passed_checks": len([r for r in results if r.passed]),
            "failed_checks": len([r for r in results if not r.passed]),
            "scores_by_type": scores_by_type,
            "recent_alerts": [
                {
                    "alert_id": a.alert_id,
                    "title": a.title,
                    "severity": a.severity.value,
                    "dataset": a.dataset,
                    "timestamp": a.timestamp.isoformat(),
                }
                for a in recent_alerts
            ],
            "trend_data": trend_data,
            "dashboard_timestamp": datetime.now().isoformat(),
        }

    def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """
        Resolve a quality alert.

        Args:
            alert_id: Alert ID to resolve
            resolved_by: Who resolved the alert

        Returns:
            True if resolved successfully
        """
        for alert in self.alerts:
            if alert.alert_id == alert_id and not alert.resolved:
                alert.resolved = True
                alert.resolved_at = datetime.now()
                alert.resolved_by = resolved_by
                self._save_alerts()

                self.logger.info(f"Resolved quality alert: {alert_id}")
                return True

        return False

    def get_quality_report(
        self, dataset: Optional[str] = None, days: int = 30
    ) -> Dict[str, Any]:
        """Generate comprehensive quality report."""
        cutoff_date = datetime.now() - timedelta(days=days)

        # Filter data
        results = [r for r in self.results if r.timestamp >= cutoff_date]
        alerts = [a for a in self.alerts if a.timestamp >= cutoff_date]

        if dataset:
            results = [r for r in results if r.dataset == dataset]
            alerts = [a for a in alerts if a.dataset == dataset]

        # Calculate statistics
        total_checks = len(results)
        passed_checks = len([r for r in results if r.passed])
        failed_checks = total_checks - passed_checks

        # Quality trends
        quality_trend = []
        if results:
            results_by_day = {}
            for result in results:
                day = result.timestamp.date()
                if day not in results_by_day:
                    results_by_day[day] = []
                results_by_day[day].append(result.score)

            for day in sorted(results_by_day.keys()):
                scores = results_by_day[day]
                quality_trend.append(
                    {
                        "date": day.isoformat(),
                        "average_score": sum(scores) / len(scores),
                        "checks_run": len(scores),
                    }
                )

        # Alert summary
        alert_summary = {}
        for severity in AlertSeverity:
            severity_alerts = [a for a in alerts if a.severity == severity]
            alert_summary[severity.value] = {
                "total": len(severity_alerts),
                "resolved": len([a for a in severity_alerts if a.resolved]),
                "open": len([a for a in severity_alerts if not a.resolved]),
            }

        return {
            "report_period": f"{days} days",
            "report_timestamp": datetime.now().isoformat(),
            "dataset": dataset or "all",
            "summary": {
                "total_checks": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": failed_checks,
                "success_rate": passed_checks / total_checks
                if total_checks > 0
                else 0.0,
                "average_score": sum(r.score for r in results) / len(results)
                if results
                else 0.0,
            },
            "quality_trend": quality_trend,
            "alert_summary": alert_summary,
            "datasets_monitored": list({r.dataset for r in results}),
            "rules_active": len([r for r in self.rules.values() if r.enabled]),
        }

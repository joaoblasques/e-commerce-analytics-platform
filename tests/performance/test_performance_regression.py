"""
Performance regression detection system.

This module provides automated detection of performance regressions
by comparing current performance against historical baselines.
"""

import hashlib
import json
import logging
import os
import pickle
import statistics
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pytest
import requests
from scipy import stats

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PerformanceBaseline:
    """Performance baseline data structure."""

    test_name: str
    git_commit: str
    timestamp: datetime
    metrics: Dict[str, float]
    environment: Dict[str, str]
    sample_size: int
    confidence_interval_95: Dict[str, Tuple[float, float]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "test_name": self.test_name,
            "git_commit": self.git_commit,
            "timestamp": self.timestamp.isoformat(),
            "metrics": self.metrics,
            "environment": self.environment,
            "sample_size": self.sample_size,
            "confidence_interval_95": {
                k: list(v) for k, v in self.confidence_interval_95.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PerformanceBaseline":
        """Create from dictionary."""
        return cls(
            test_name=data["test_name"],
            git_commit=data["git_commit"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metrics=data["metrics"],
            environment=data["environment"],
            sample_size=data["sample_size"],
            confidence_interval_95={
                k: tuple(v) for k, v in data["confidence_interval_95"].items()
            },
        )


@dataclass
class RegressionAnalysis:
    """Performance regression analysis result."""

    test_name: str
    regression_detected: bool
    severity: str  # "none", "minor", "major", "critical"
    affected_metrics: List[str]
    performance_changes: Dict[str, float]  # Percentage changes
    statistical_significance: Dict[str, float]  # p-values
    confidence_level: float
    baseline_commit: str
    current_commit: str
    recommendation: str
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            "test_name": self.test_name,
            "regression_detected": self.regression_detected,
            "severity": self.severity,
            "affected_metrics": self.affected_metrics,
            "performance_changes": self.performance_changes,
            "statistical_significance": self.statistical_significance,
            "confidence_level": self.confidence_level,
            "baseline_commit": self.baseline_commit,
            "current_commit": self.current_commit,
            "recommendation": self.recommendation,
            "timestamp": self.timestamp.isoformat(),
        }


class PerformanceRegressionDetector:
    """Automated performance regression detection system."""

    def __init__(self, baseline_dir: str = "tests/performance/baselines"):
        self.baseline_dir = Path(baseline_dir)
        self.baseline_dir.mkdir(parents=True, exist_ok=True)

        # Regression thresholds
        self.thresholds = {
            "minor": 10.0,  # 10% degradation
            "major": 25.0,  # 25% degradation
            "critical": 50.0,  # 50% degradation
        }

        # Statistical significance level
        self.alpha = 0.05  # 95% confidence level

    def get_git_commit(self) -> str:
        """Get current git commit hash."""
        try:
            import subprocess

            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                cwd=os.path.dirname(__file__),
            )
            if result.returncode == 0:
                return result.stdout.strip()[:8]  # Short hash
            else:
                return "unknown"
        except Exception:
            return "unknown"

    def get_environment_info(self) -> Dict[str, str]:
        """Get environment information for baseline comparison."""
        import platform

        import psutil

        return {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": str(psutil.cpu_count()),
            "memory_gb": str(round(psutil.virtual_memory().total / (1024**3), 1)),
            "hostname": platform.node(),
        }

    def calculate_statistics(self, measurements: List[float]) -> Dict[str, Any]:
        """Calculate statistical measures for performance measurements."""
        if len(measurements) < 2:
            raise ValueError("Need at least 2 measurements for statistical analysis")

        mean_val = statistics.mean(measurements)
        std_val = statistics.stdev(measurements)

        # 95% confidence interval
        confidence_level = 0.95
        degrees_freedom = len(measurements) - 1
        t_critical = stats.t.ppf((1 + confidence_level) / 2, degrees_freedom)
        margin_error = t_critical * (std_val / np.sqrt(len(measurements)))

        ci_lower = mean_val - margin_error
        ci_upper = mean_val + margin_error

        return {
            "mean": mean_val,
            "std": std_val,
            "min": min(measurements),
            "max": max(measurements),
            "median": statistics.median(measurements),
            "confidence_interval_95": (ci_lower, ci_upper),
            "sample_size": len(measurements),
        }

    def create_baseline(
        self, test_name: str, measurements: Dict[str, List[float]]
    ) -> PerformanceBaseline:
        """Create a new performance baseline from measurements."""
        logger.info(f"Creating baseline for {test_name}")

        # Calculate statistics for each metric
        metrics = {}
        confidence_intervals = {}

        for metric_name, values in measurements.items():
            if not values:
                continue

            stats_data = self.calculate_statistics(values)
            metrics[metric_name] = stats_data["mean"]
            confidence_intervals[metric_name] = stats_data["confidence_interval_95"]

        baseline = PerformanceBaseline(
            test_name=test_name,
            git_commit=self.get_git_commit(),
            timestamp=datetime.now(),
            metrics=metrics,
            environment=self.get_environment_info(),
            sample_size=len(next(iter(measurements.values()))),
            confidence_interval_95=confidence_intervals,
        )

        return baseline

    def save_baseline(self, baseline: PerformanceBaseline):
        """Save baseline to disk."""
        baseline_file = self.baseline_dir / f"{baseline.test_name}_baseline.json"

        with open(baseline_file, "w") as f:
            json.dump(baseline.to_dict(), f, indent=2)

        logger.info(f"Baseline saved: {baseline_file}")

    def load_baseline(self, test_name: str) -> Optional[PerformanceBaseline]:
        """Load baseline from disk."""
        baseline_file = self.baseline_dir / f"{test_name}_baseline.json"

        if not baseline_file.exists():
            return None

        try:
            with open(baseline_file, "r") as f:
                data = json.load(f)
            return PerformanceBaseline.from_dict(data)
        except Exception as e:
            logger.error(f"Error loading baseline {baseline_file}: {e}")
            return None

    def compare_performance(
        self,
        baseline: PerformanceBaseline,
        current_measurements: Dict[str, List[float]],
    ) -> RegressionAnalysis:
        """Compare current performance against baseline."""
        logger.info(f"Comparing performance for {baseline.test_name}")

        affected_metrics = []
        performance_changes = {}
        statistical_significance = {}
        max_degradation = 0.0

        current_commit = self.get_git_commit()

        for metric_name, current_values in current_measurements.items():
            if metric_name not in baseline.metrics or not current_values:
                continue

            baseline_mean = baseline.metrics[metric_name]
            current_stats = self.calculate_statistics(current_values)
            current_mean = current_stats["mean"]

            # Calculate percentage change (positive = worse performance for response times)
            if baseline_mean > 0:
                change_percent = ((current_mean - baseline_mean) / baseline_mean) * 100
            else:
                change_percent = 0.0

            performance_changes[metric_name] = change_percent

            # Statistical significance test (Welch's t-test)
            try:
                # Create baseline sample from confidence interval (approximation)
                baseline_ci = baseline.confidence_interval_95[metric_name]
                baseline_std = (baseline_ci[1] - baseline_ci[0]) / (
                    2 * 1.96
                )  # Approximate std

                # Generate synthetic baseline sample
                np.random.seed(42)  # Reproducible
                baseline_sample = np.random.normal(
                    baseline_mean, baseline_std, baseline.sample_size
                )

                # Perform t-test
                t_stat, p_value = stats.ttest_ind(
                    current_values, baseline_sample, equal_var=False
                )
                statistical_significance[metric_name] = p_value

                # Check if this is a significant regression
                is_significant = p_value < self.alpha
                is_degradation = change_percent > 0  # Assuming higher values = worse

                if (
                    is_significant
                    and is_degradation
                    and abs(change_percent) > self.thresholds["minor"]
                ):
                    affected_metrics.append(metric_name)
                    max_degradation = max(max_degradation, abs(change_percent))

            except Exception as e:
                logger.warning(f"Statistical test failed for {metric_name}: {e}")
                statistical_significance[metric_name] = 1.0  # No significance

        # Determine regression severity
        regression_detected = len(affected_metrics) > 0

        if not regression_detected:
            severity = "none"
        elif max_degradation > self.thresholds["critical"]:
            severity = "critical"
        elif max_degradation > self.thresholds["major"]:
            severity = "major"
        else:
            severity = "minor"

        # Generate recommendation
        recommendation = self._generate_recommendation(
            severity, affected_metrics, performance_changes
        )

        return RegressionAnalysis(
            test_name=baseline.test_name,
            regression_detected=regression_detected,
            severity=severity,
            affected_metrics=affected_metrics,
            performance_changes=performance_changes,
            statistical_significance=statistical_significance,
            confidence_level=1 - self.alpha,
            baseline_commit=baseline.git_commit,
            current_commit=current_commit,
            recommendation=recommendation,
        )

    def _generate_recommendation(
        self, severity: str, affected_metrics: List[str], changes: Dict[str, float]
    ) -> str:
        """Generate actionable recommendations based on regression analysis."""
        if severity == "none":
            return "No performance regression detected. Performance is within expected range."

        recommendations = []

        if severity == "critical":
            recommendations.append(
                "🚨 CRITICAL: Immediate investigation required. Consider reverting changes."
            )
        elif severity == "major":
            recommendations.append(
                "⚠️ MAJOR: Significant performance degradation detected. Review before deployment."
            )
        else:
            recommendations.append(
                "ℹ️ MINOR: Performance degradation detected. Monitor in production."
            )

        # Metric-specific recommendations
        if "response_time_ms" in affected_metrics:
            recommendations.append(
                "• Review API endpoint optimization and database query performance"
            )

        if "throughput_requests_per_second" in affected_metrics:
            recommendations.append(
                "• Check for bottlenecks in request processing pipeline"
            )

        if "error_rate_percent" in affected_metrics:
            recommendations.append(
                "• Investigate error causes and improve error handling"
            )

        if "cpu_usage_percent" in affected_metrics:
            recommendations.append(
                "• Profile CPU usage and optimize computationally intensive operations"
            )

        if "memory_usage_mb" in affected_metrics:
            recommendations.append(
                "• Check for memory leaks and optimize memory allocation"
            )

        # Add change details
        worst_changes = sorted(
            [(k, v) for k, v in changes.items() if k in affected_metrics],
            key=lambda x: abs(x[1]),
            reverse=True,
        )[
            :3
        ]  # Top 3 worst changes

        if worst_changes:
            recommendations.append(
                f"• Worst affected metrics: {', '.join([f'{k} (+{v:.1f}%)' for k, v in worst_changes])}"
            )

        return " ".join(recommendations)

    def run_regression_test(
        self,
        test_name: str,
        performance_test_function: callable,
        create_new_baseline: bool = False,
    ) -> RegressionAnalysis:
        """Run a complete regression test."""
        logger.info(f"Running regression test: {test_name}")

        # Run the performance test multiple times for statistical validity
        measurements = {}
        num_runs = 5  # Number of test runs

        for run in range(num_runs):
            logger.info(f"Performance test run {run + 1}/{num_runs}")

            try:
                metrics = performance_test_function()

                # Collect measurements
                for metric_name, value in metrics.items():
                    if metric_name not in measurements:
                        measurements[metric_name] = []
                    measurements[metric_name].append(value)

                # Brief pause between runs
                time.sleep(2)

            except Exception as e:
                logger.error(f"Performance test run {run + 1} failed: {e}")
                continue

        if not measurements:
            raise RuntimeError("All performance test runs failed")

        # Load or create baseline
        baseline = self.load_baseline(test_name)

        if baseline is None or create_new_baseline:
            logger.info(f"Creating new baseline for {test_name}")
            baseline = self.create_baseline(test_name, measurements)
            self.save_baseline(baseline)

            # Return "no regression" analysis for new baseline
            return RegressionAnalysis(
                test_name=test_name,
                regression_detected=False,
                severity="none",
                affected_metrics=[],
                performance_changes={},
                statistical_significance={},
                confidence_level=0.95,
                baseline_commit=baseline.git_commit,
                current_commit=baseline.git_commit,
                recommendation="New baseline created. No regression analysis performed.",
            )

        # Compare against baseline
        return self.compare_performance(baseline, measurements)

    def batch_regression_analysis(
        self, test_results: Dict[str, Dict[str, float]]
    ) -> Dict[str, RegressionAnalysis]:
        """Analyze multiple test results for regressions."""
        results = {}

        for test_name, metrics in test_results.items():
            try:
                # Convert single metrics to lists for analysis
                measurements = {k: [v] for k, v in metrics.items()}

                baseline = self.load_baseline(test_name)
                if baseline:
                    analysis = self.compare_performance(baseline, measurements)
                    results[test_name] = analysis
                else:
                    logger.warning(f"No baseline found for {test_name}")

            except Exception as e:
                logger.error(f"Regression analysis failed for {test_name}: {e}")

        return results

    def generate_report(self, analyses: Dict[str, RegressionAnalysis]) -> str:
        """Generate a comprehensive regression analysis report."""
        report_lines = [
            "# Performance Regression Analysis Report",
            f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Git Commit**: {self.get_git_commit()}",
            "",
        ]

        # Summary
        total_tests = len(analyses)
        regressions = [a for a in analyses.values() if a.regression_detected]
        critical_regressions = [a for a in regressions if a.severity == "critical"]
        major_regressions = [a for a in regressions if a.severity == "major"]
        minor_regressions = [a for a in regressions if a.severity == "minor"]

        report_lines.extend(
            [
                "## Summary",
                f"- **Total Tests**: {total_tests}",
                f"- **Regressions Detected**: {len(regressions)}",
                f"- **Critical**: {len(critical_regressions)}",
                f"- **Major**: {len(major_regressions)}",
                f"- **Minor**: {len(minor_regressions)}",
                "",
            ]
        )

        # Critical regressions first
        if critical_regressions:
            report_lines.extend(["## 🚨 Critical Regressions", ""])

            for analysis in critical_regressions:
                report_lines.extend(self._format_analysis_section(analysis))

        # Major regressions
        if major_regressions:
            report_lines.extend(["## ⚠️ Major Regressions", ""])

            for analysis in major_regressions:
                report_lines.extend(self._format_analysis_section(analysis))

        # Minor regressions
        if minor_regressions:
            report_lines.extend(["## ℹ️ Minor Regressions", ""])

            for analysis in minor_regressions:
                report_lines.extend(self._format_analysis_section(analysis))

        # All test results summary
        report_lines.extend(["## All Test Results", ""])

        for test_name, analysis in analyses.items():
            status = "❌" if analysis.regression_detected else "✅"
            report_lines.append(f"- {status} **{test_name}**: {analysis.severity}")

        return "\n".join(report_lines)

    def _format_analysis_section(self, analysis: RegressionAnalysis) -> List[str]:
        """Format an individual analysis for the report."""
        lines = [
            f"### {analysis.test_name}",
            f"**Severity**: {analysis.severity.upper()}",
            f"**Affected Metrics**: {', '.join(analysis.affected_metrics)}",
            "",
        ]

        if analysis.performance_changes:
            lines.append("**Performance Changes**:")
            for metric, change in analysis.performance_changes.items():
                if metric in analysis.affected_metrics:
                    lines.append(f"- {metric}: +{change:.1f}% (worse)")
                else:
                    sign = "+" if change > 0 else ""
                    lines.append(f"- {metric}: {sign}{change:.1f}%")
            lines.append("")

        lines.extend(
            [
                f"**Recommendation**: {analysis.recommendation}",
                f"**Baseline**: {analysis.baseline_commit} → **Current**: {analysis.current_commit}",
                "",
            ]
        )

        return lines


# Integration with existing performance tests
class PerformanceTestRunner:
    """Helper class to run performance tests with regression detection."""

    def __init__(self):
        self.regression_detector = PerformanceRegressionDetector()

    def simple_api_performance_test(self) -> Dict[str, float]:
        """Simple API performance test for regression detection."""
        try:
            # Make multiple requests and measure performance
            response_times = []
            error_count = 0

            for _ in range(10):
                start_time = time.time()
                try:
                    response = requests.get("http://localhost:8000/health", timeout=5)
                    response_time = (time.time() - start_time) * 1000
                    response_times.append(response_time)

                    if response.status_code != 200:
                        error_count += 1
                except:
                    error_count += 1
                    response_times.append(5000)  # Timeout value

            return {
                "response_time_ms": statistics.mean(response_times),
                "error_rate_percent": (error_count / 10) * 100,
                "p95_response_time_ms": sorted(response_times)[8]
                if len(response_times) >= 9
                else max(response_times),
            }
        except Exception as e:
            logger.error(f"API performance test failed: {e}")
            return {
                "response_time_ms": 10000,
                "error_rate_percent": 100,
                "p95_response_time_ms": 10000,
            }


# Test fixtures and pytest integration
@pytest.fixture
def regression_detector():
    """Regression detector fixture."""
    return PerformanceRegressionDetector()


@pytest.fixture
def performance_runner():
    """Performance test runner fixture."""
    return PerformanceTestRunner()


class TestPerformanceRegression:
    """Performance regression detection tests."""

    def test_baseline_creation_and_loading(self, regression_detector):
        """Test baseline creation and loading functionality."""
        test_name = "test_baseline_creation"

        # Create sample measurements
        measurements = {
            "response_time_ms": [100, 105, 95, 110, 98],
            "error_rate_percent": [0.1, 0.0, 0.2, 0.1, 0.0],
            "throughput_rps": [1000, 1050, 980, 1020, 990],
        }

        # Create baseline
        baseline = regression_detector.create_baseline(test_name, measurements)

        # Validate baseline
        assert baseline.test_name == test_name
        assert len(baseline.metrics) == 3
        assert baseline.sample_size == 5
        assert "response_time_ms" in baseline.metrics

        # Save and load baseline
        regression_detector.save_baseline(baseline)
        loaded_baseline = regression_detector.load_baseline(test_name)

        assert loaded_baseline is not None
        assert loaded_baseline.test_name == baseline.test_name
        assert loaded_baseline.metrics == baseline.metrics

    def test_regression_detection_no_regression(self, regression_detector):
        """Test regression detection when no regression exists."""
        test_name = "test_no_regression"

        # Create baseline
        baseline_measurements = {
            "response_time_ms": [100, 105, 95, 110, 98],
            "error_rate_percent": [0.1, 0.0, 0.2, 0.1, 0.0],
        }
        baseline = regression_detector.create_baseline(test_name, baseline_measurements)

        # Similar current measurements (no regression)
        current_measurements = {
            "response_time_ms": [102, 108, 93, 107, 101],
            "error_rate_percent": [0.0, 0.1, 0.1, 0.0, 0.2],
        }

        analysis = regression_detector.compare_performance(
            baseline, current_measurements
        )

        assert not analysis.regression_detected
        assert analysis.severity == "none"
        assert len(analysis.affected_metrics) == 0

    def test_regression_detection_minor_regression(self, regression_detector):
        """Test regression detection for minor performance degradation."""
        test_name = "test_minor_regression"

        # Create baseline
        baseline_measurements = {
            "response_time_ms": [100, 105, 95, 110, 98],  # ~102ms avg
            "error_rate_percent": [0.1, 0.0, 0.2, 0.1, 0.0],  # ~0.08% avg
        }
        baseline = regression_detector.create_baseline(test_name, baseline_measurements)

        # Degraded current measurements (15% slower)
        current_measurements = {
            "response_time_ms": [115, 120, 110, 125, 113],  # ~117ms avg (15% increase)
            "error_rate_percent": [0.1, 0.0, 0.2, 0.1, 0.0],  # Same error rate
        }

        analysis = regression_detector.compare_performance(
            baseline, current_measurements
        )

        # Should detect minor regression in response time
        assert analysis.regression_detected
        assert analysis.severity == "minor"
        assert "response_time_ms" in analysis.affected_metrics
        assert analysis.performance_changes["response_time_ms"] > 10.0  # >10% increase

    def test_regression_detection_major_regression(self, regression_detector):
        """Test regression detection for major performance degradation."""
        test_name = "test_major_regression"

        # Create baseline
        baseline_measurements = {
            "response_time_ms": [100, 105, 95, 110, 98],
            "throughput_rps": [1000, 1050, 980, 1020, 990],
        }
        baseline = regression_detector.create_baseline(test_name, baseline_measurements)

        # Severely degraded measurements (30% worse)
        current_measurements = {
            "response_time_ms": [130, 140, 125, 145, 135],  # ~35% slower
            "throughput_rps": [700, 750, 680, 720, 690],  # ~30% lower throughput
        }

        analysis = regression_detector.compare_performance(
            baseline, current_measurements
        )

        assert analysis.regression_detected
        assert analysis.severity == "major"
        assert len(analysis.affected_metrics) >= 1
        assert analysis.performance_changes["response_time_ms"] > 25.0

    def test_full_regression_test_workflow(self, performance_runner):
        """Test complete regression test workflow."""
        test_name = "test_api_regression"

        # Run regression test (this will create baseline if none exists)
        analysis = performance_runner.regression_detector.run_regression_test(
            test_name=test_name,
            performance_test_function=performance_runner.simple_api_performance_test,
            create_new_baseline=True,  # Force new baseline for test
        )

        # New baseline should show no regression
        assert not analysis.regression_detected
        assert analysis.severity == "none"

        # Run again without creating new baseline (should compare to existing)
        analysis2 = performance_runner.regression_detector.run_regression_test(
            test_name=test_name,
            performance_test_function=performance_runner.simple_api_performance_test,
        )

        # Should perform comparison (may or may not detect regression depending on actual performance)
        assert analysis2.baseline_commit is not None
        assert analysis2.current_commit is not None

    def test_batch_regression_analysis(self, regression_detector):
        """Test batch analysis of multiple test results."""
        # Create baselines for multiple tests
        test_data = {
            "api_health_test": {
                "response_time_ms": [50, 55, 45, 60, 48],
                "error_rate_percent": [0, 0, 0, 0, 0],
            },
            "database_test": {
                "query_time_ms": [10, 12, 8, 15, 9],
                "connection_errors": [0, 0, 1, 0, 0],
            },
        }

        # Create baselines
        for test_name, measurements in test_data.items():
            baseline = regression_detector.create_baseline(test_name, measurements)
            regression_detector.save_baseline(baseline)

        # Test batch analysis with current results
        current_results = {
            "api_health_test": {
                "response_time_ms": 52,  # Within range
                "error_rate_percent": 0,
            },
            "database_test": {
                "query_time_ms": 25,  # Significantly slower
                "connection_errors": 0,
            },
        }

        analyses = regression_detector.batch_regression_analysis(current_results)

        assert len(analyses) == 2
        assert "api_health_test" in analyses
        assert "database_test" in analyses

        # Database test should show regression due to slower query time
        db_analysis = analyses["database_test"]
        assert (
            db_analysis.performance_changes["query_time_ms"] > 0
        )  # Positive change = worse

    def test_report_generation(self, regression_detector):
        """Test regression analysis report generation."""
        # Create sample analyses
        analyses = {
            "test_normal": RegressionAnalysis(
                test_name="test_normal",
                regression_detected=False,
                severity="none",
                affected_metrics=[],
                performance_changes={"response_time_ms": -2.5},  # 2.5% better
                statistical_significance={"response_time_ms": 0.3},
                confidence_level=0.95,
                baseline_commit="abc123",
                current_commit="def456",
                recommendation="No regression detected.",
            ),
            "test_critical": RegressionAnalysis(
                test_name="test_critical",
                regression_detected=True,
                severity="critical",
                affected_metrics=["response_time_ms"],
                performance_changes={"response_time_ms": 75.0},  # 75% worse
                statistical_significance={"response_time_ms": 0.001},
                confidence_level=0.95,
                baseline_commit="abc123",
                current_commit="def456",
                recommendation="Critical regression detected.",
            ),
        }

        report = regression_detector.generate_report(analyses)

        assert "Performance Regression Analysis Report" in report
        assert "Critical Regressions" in report
        assert "test_critical" in report
        assert "test_normal" in report
        assert "75.0%" in report  # Critical regression percentage

    def test_statistical_significance_calculation(self, regression_detector):
        """Test statistical significance calculation in regression detection."""
        test_name = "test_statistics"

        # Create baseline with known statistics
        baseline_measurements = {
            "response_time_ms": [100] * 20  # Very consistent baseline
        }
        baseline = regression_detector.create_baseline(test_name, baseline_measurements)

        # Current measurements with clear difference
        current_measurements = {
            "response_time_ms": [150] * 20  # 50% increase, very consistent
        }

        analysis = regression_detector.compare_performance(
            baseline, current_measurements
        )

        # Should detect significant regression
        assert analysis.regression_detected
        assert "response_time_ms" in analysis.statistical_significance
        assert (
            analysis.statistical_significance["response_time_ms"] < 0.05
        )  # Significant
        assert analysis.performance_changes["response_time_ms"] > 45.0  # ~50% increase


if __name__ == "__main__":
    # Example usage for standalone testing
    logging.basicConfig(level=logging.INFO)

    print("Performance Regression Detection System")
    print("Run with pytest for full test suite:")
    print("pytest tests/performance/test_performance_regression.py -v --tb=short")

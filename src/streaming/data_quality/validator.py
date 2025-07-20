"""
Real-time data validation rules for streaming data.

This module provides comprehensive validation capabilities for different
types of streaming data including transactions, user behavior, and more.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    count,
    current_timestamp,
    isnan,
    isnull,
    length,
    lit,
    lower,
    regexp_extract,
    size,
    split,
    struct,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_json, trim, upper, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.utils.logger import get_logger


@dataclass
class ValidationRule:
    """Data validation rule definition."""

    name: str
    description: str
    condition: str  # SQL-like condition string
    severity: str  # "error", "warning", "info"
    category: str  # "format", "range", "business", "referential"
    enabled: bool = True


class StreamingDataValidator:
    """
    Real-time data validation engine for streaming data.

    Provides comprehensive validation rules for different stream types
    including format validation, range checks, business rules, and more.
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize streaming data validator.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary for validation settings
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        self.config = config or {}

        # Initialize validation rules for different stream types
        self.validation_rules = self._initialize_validation_rules()

    def _initialize_validation_rules(self) -> Dict[str, List[ValidationRule]]:
        """Initialize validation rules for different stream types."""

        return {
            "transaction": [
                ValidationRule(
                    name="transaction_id_format",
                    description="Transaction ID must be non-null and follow UUID format",
                    condition="transaction_id IS NOT NULL AND length(transaction_id) >= 10",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="user_id_valid",
                    description="User ID must be non-null and positive",
                    condition="user_id IS NOT NULL AND user_id > 0",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="amount_positive",
                    description="Transaction amount must be positive",
                    condition="total_amount IS NOT NULL AND total_amount > 0",
                    severity="error",
                    category="business",
                ),
                ValidationRule(
                    name="amount_reasonable",
                    description="Transaction amount should be within reasonable range",
                    condition="total_amount <= 10000",
                    severity="warning",
                    category="business",
                ),
                ValidationRule(
                    name="timestamp_recent",
                    description="Transaction timestamp should be recent",
                    condition="timestamp >= current_timestamp() - interval '1 hour'",
                    severity="warning",
                    category="temporal",
                ),
                ValidationRule(
                    name="payment_method_valid",
                    description="Payment method must be from allowed list",
                    condition="payment_method IN ('credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash')",
                    severity="error",
                    category="business",
                ),
                ValidationRule(
                    name="currency_valid",
                    description="Currency code must be 3 characters",
                    condition="currency IS NOT NULL AND length(currency) = 3",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="location_format",
                    description="Location should be non-empty if provided",
                    condition="location IS NULL OR trim(location) != ''",
                    severity="warning",
                    category="format",
                ),
            ],
            "user_behavior": [
                ValidationRule(
                    name="user_id_valid",
                    description="User ID must be non-null and positive",
                    condition="user_id IS NOT NULL AND user_id > 0",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="session_id_format",
                    description="Session ID must be non-null and non-empty",
                    condition="session_id IS NOT NULL AND trim(session_id) != ''",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="event_type_valid",
                    description="Event type must be from allowed list",
                    condition="event_type IN ('page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'checkout', 'purchase', 'search', 'login', 'logout')",
                    severity="error",
                    category="business",
                ),
                ValidationRule(
                    name="timestamp_recent",
                    description="Event timestamp should be recent",
                    condition="timestamp >= current_timestamp() - interval '2 hours'",
                    severity="warning",
                    category="temporal",
                ),
                ValidationRule(
                    name="page_url_format",
                    description="Page URL should be valid format if provided",
                    condition="page_url IS NULL OR page_url RLIKE '^https?://.*'",
                    severity="warning",
                    category="format",
                ),
                ValidationRule(
                    name="user_agent_present",
                    description="User agent should be present for web events",
                    condition="user_agent IS NOT NULL AND trim(user_agent) != ''",
                    severity="info",
                    category="format",
                ),
                ValidationRule(
                    name="product_id_valid",
                    description="Product ID should be positive if provided",
                    condition="product_id IS NULL OR product_id > 0",
                    severity="warning",
                    category="format",
                ),
            ],
            "customer_profile": [
                ValidationRule(
                    name="user_id_valid",
                    description="User ID must be non-null and positive",
                    condition="user_id IS NOT NULL AND user_id > 0",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="email_format",
                    description="Email must be valid format if provided",
                    condition="email IS NULL OR email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
                    severity="error",
                    category="format",
                ),
                ValidationRule(
                    name="age_reasonable",
                    description="Age should be between 13 and 120",
                    condition="age IS NULL OR (age >= 13 AND age <= 120)",
                    severity="warning",
                    category="business",
                ),
                ValidationRule(
                    name="registration_date_valid",
                    description="Registration date should not be in the future",
                    condition="registration_date IS NULL OR registration_date <= current_date()",
                    severity="error",
                    category="temporal",
                ),
                ValidationRule(
                    name="customer_tier_valid",
                    description="Customer tier must be from allowed list",
                    condition="customer_tier IS NULL OR customer_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')",
                    severity="warning",
                    category="business",
                ),
            ],
        }

    def validate_stream(
        self, df: DataFrame, stream_type: str = "transaction"
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Validate streaming data against defined rules.

        Args:
            df: Input DataFrame to validate
            stream_type: Type of stream (transaction, user_behavior, customer_profile)

        Returns:
            Tuple of (validated_dataframe, validation_results)
        """
        try:
            self.logger.info(f"Starting validation for {stream_type} stream")

            # Get validation rules for stream type
            rules = self.validation_rules.get(stream_type, [])
            if not rules:
                self.logger.warning(
                    f"No validation rules found for stream type: {stream_type}"
                )
                return df.withColumn("validation_passed", lit(True)), {
                    "valid_count": df.count(),
                    "invalid_count": 0,
                }

            # Apply validation rules
            validated_df = df
            validation_results = []

            for rule in rules:
                if not rule.enabled:
                    continue

                validated_df, rule_result = self._apply_validation_rule(
                    validated_df, rule
                )
                validation_results.append(rule_result)

            # Add overall validation flag
            validated_df = self._add_overall_validation_flag(validated_df, rules)

            # Calculate summary metrics
            total_count = validated_df.count()
            valid_count = validated_df.filter(col("validation_passed")).count()
            invalid_count = total_count - valid_count

            validation_summary = {
                "stream_type": stream_type,
                "total_count": total_count,
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "validation_rate": valid_count / max(total_count, 1),
                "rules_applied": len([r for r in rules if r.enabled]),
                "rule_results": validation_results,
                "errors": [
                    r
                    for r in validation_results
                    if r["severity"] == "error" and r["violation_count"] > 0
                ],
            }

            self.logger.info(
                f"Validation completed: {valid_count}/{total_count} records passed"
            )
            return validated_df, validation_summary

        except Exception as e:
            self.logger.error(f"Error in stream validation: {e}")
            raise

    def _apply_validation_rule(
        self, df: DataFrame, rule: ValidationRule
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Apply a single validation rule to the DataFrame."""

        try:
            # Create validation column name
            rule_column = f"rule_{rule.name}"

            # Apply the validation condition
            validated_df = df.withColumn(
                rule_column,
                when(
                    df.sql_ctx.sql(f"SELECT ({rule.condition}) as result").collect()[0][
                        0
                    ],
                    lit(True),
                ).otherwise(lit(False)),
            )

            # For safety, use a simpler approach with basic conditions
            # This is a simplified implementation - in production, you'd want more sophisticated SQL parsing
            validated_df = self._apply_simple_validation(df, rule)

            # Count violations
            violation_count = validated_df.filter(~col(f"rule_{rule.name}")).count()

            rule_result = {
                "rule_name": rule.name,
                "description": rule.description,
                "severity": rule.severity,
                "category": rule.category,
                "condition": rule.condition,
                "violation_count": violation_count,
                "pass_rate": 1 - (violation_count / max(df.count(), 1)),
            }

            return validated_df, rule_result

        except Exception as e:
            self.logger.error(f"Error applying validation rule {rule.name}: {e}")
            # Return original DataFrame with failed rule marked as False
            return df.withColumn(f"rule_{rule.name}", lit(False)), {
                "rule_name": rule.name,
                "description": rule.description,
                "severity": rule.severity,
                "category": rule.category,
                "condition": rule.condition,
                "violation_count": df.count(),
                "pass_rate": 0.0,
                "error": str(e),
            }

    def _apply_simple_validation(
        self, df: DataFrame, rule: ValidationRule
    ) -> DataFrame:
        """Apply simplified validation logic for common cases."""

        rule_column = f"rule_{rule.name}"

        # Handle common validation patterns
        if "transaction_id_format" in rule.name:
            return df.withColumn(
                rule_column,
                (col("transaction_id").isNotNull())
                & (length(col("transaction_id")) >= 10),
            )
        elif "user_id_valid" in rule.name:
            return df.withColumn(
                rule_column, (col("user_id").isNotNull()) & (col("user_id") > 0)
            )
        elif "amount_positive" in rule.name:
            return df.withColumn(
                rule_column,
                (col("total_amount").isNotNull()) & (col("total_amount") > 0),
            )
        elif "amount_reasonable" in rule.name:
            return df.withColumn(
                rule_column, coalesce(col("total_amount") <= 10000, lit(True))
            )
        elif "payment_method_valid" in rule.name:
            valid_methods = [
                "credit_card",
                "debit_card",
                "paypal",
                "bank_transfer",
                "cash",
            ]
            return df.withColumn(
                rule_column,
                coalesce(col("payment_method").isin(valid_methods), lit(False)),
            )
        elif "currency_valid" in rule.name:
            return df.withColumn(
                rule_column,
                (col("currency").isNotNull()) & (length(col("currency")) == 3),
            )
        elif "session_id_format" in rule.name:
            return df.withColumn(
                rule_column,
                (col("session_id").isNotNull()) & (length(trim(col("session_id"))) > 0),
            )
        elif "event_type_valid" in rule.name:
            valid_events = [
                "page_view",
                "product_view",
                "add_to_cart",
                "remove_from_cart",
                "checkout",
                "purchase",
                "search",
                "login",
                "logout",
            ]
            return df.withColumn(
                rule_column, coalesce(col("event_type").isin(valid_events), lit(False))
            )
        elif "email_format" in rule.name:
            return df.withColumn(
                rule_column,
                col("email").isNull()
                | col("email").rlike(
                    "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
                ),
            )
        elif "age_reasonable" in rule.name:
            return df.withColumn(
                rule_column,
                col("age").isNull() | ((col("age") >= 13) & (col("age") <= 120)),
            )
        elif "customer_tier_valid" in rule.name:
            valid_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
            return df.withColumn(
                rule_column,
                col("customer_tier").isNull() | col("customer_tier").isin(valid_tiers),
            )
        else:
            # Default: assume validation passes (for rules not implemented)
            self.logger.warning(
                f"Validation rule {rule.name} not implemented, defaulting to pass"
            )
            return df.withColumn(rule_column, lit(True))

    def _add_overall_validation_flag(
        self, df: DataFrame, rules: List[ValidationRule]
    ) -> DataFrame:
        """Add overall validation flag based on all rule results."""

        enabled_rules = [rule for rule in rules if rule.enabled]
        if not enabled_rules:
            return df.withColumn("validation_passed", lit(True))

        # Start with the first rule
        validation_expr = col(f"rule_{enabled_rules[0].name}")

        # AND all other error-level rules
        for rule in enabled_rules[1:]:
            if rule.severity == "error":
                validation_expr = validation_expr & col(f"rule_{rule.name}")

        return df.withColumn("validation_passed", validation_expr).withColumn(
            "validation_timestamp", current_timestamp()
        )

    def add_custom_validation_rule(
        self, stream_type: str, rule: ValidationRule
    ) -> None:
        """
        Add a custom validation rule for a stream type.

        Args:
            stream_type: Type of stream to add rule for
            rule: ValidationRule to add
        """
        if stream_type not in self.validation_rules:
            self.validation_rules[stream_type] = []

        self.validation_rules[stream_type].append(rule)
        self.logger.info(
            f"Added custom validation rule '{rule.name}' for {stream_type} stream"
        )

    def get_validation_summary(self, validated_df: DataFrame) -> Dict[str, Any]:
        """
        Generate validation summary statistics.

        Args:
            validated_df: DataFrame with validation results

        Returns:
            Dictionary with validation summary
        """
        try:
            total_records = validated_df.count()
            passed_records = validated_df.filter(col("validation_passed")).count()

            # Get rule-specific statistics
            rule_columns = [c for c in validated_df.columns if c.startswith("rule_")]
            rule_stats = {}

            for rule_col in rule_columns:
                rule_name = rule_col.replace("rule_", "")
                passed_count = validated_df.filter(col(rule_col)).count()
                rule_stats[rule_name] = {
                    "passed": passed_count,
                    "failed": total_records - passed_count,
                    "pass_rate": passed_count / max(total_records, 1),
                }

            return {
                "total_records": total_records,
                "passed_records": passed_records,
                "failed_records": total_records - passed_records,
                "overall_pass_rate": passed_records / max(total_records, 1),
                "rule_statistics": rule_stats,
            }

        except Exception as e:
            self.logger.error(f"Error generating validation summary: {e}")
            return {}

    def create_validation_report_stream(self, validated_df: DataFrame) -> DataFrame:
        """
        Create a stream for validation reporting and monitoring.

        Args:
            validated_df: DataFrame with validation results

        Returns:
            DataFrame with validation report data
        """
        # Get all rule columns
        rule_columns = [c for c in validated_df.columns if c.startswith("rule_")]

        # Create validation report with rule details
        return validated_df.withColumn(
            "validation_report",
            struct(
                col("validation_passed"),
                col("validation_timestamp"),
                *[
                    col(rule_col).alias(rule_col.replace("rule_", ""))
                    for rule_col in rule_columns
                ],
            ),
        ).select("validation_timestamp", "validation_passed", "validation_report")

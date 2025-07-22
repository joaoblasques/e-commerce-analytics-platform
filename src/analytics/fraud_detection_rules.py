from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType

class FraudRuleEngine:
    """
    A class to implement a rule-based fraud detection engine.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.rules = []

    def add_rule(self, name: str, condition: str, severity: str = "medium"):
        """
        Adds a fraud detection rule.

        Args:
            name: Name of the rule.
            condition: SQL-like condition for the rule (e.g., "amount > 1000 AND quantity > 5").
            severity: Severity of the rule (low, medium, high).
        """
        self.rules.append({"name": name, "condition": condition, "severity": severity})

    def apply_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies all defined rules to the input DataFrame.

        Args:
            df: The input DataFrame with transaction data.

        Returns:
            DataFrame with a 'fraud_score' and 'fraud_alerts' column.
        """
        df_with_alerts = df
        fraud_score_expr = F.lit(0.0)
        fraud_alerts_array = F.array()

        for rule in self.rules:
            rule_condition = F.expr(rule["condition"])
            rule_name = rule["name"]
            rule_severity = rule["severity"]

            # Add a column for each rule indicating if it was triggered
            df_with_alerts = df_with_alerts.withColumn(
                f"rule_{rule_name}_triggered", rule_condition.cast(BooleanType())
            )

            # Update fraud score based on severity
            if rule_severity == "high":
                fraud_score_expr = fraud_score_expr + F.when(rule_condition, 1.0).otherwise(0.0)
            elif rule_severity == "medium":
                fraud_score_expr = fraud_score_expr + F.when(rule_condition, 0.5).otherwise(0.0)
            elif rule_severity == "low":
                fraud_score_expr = fraud_score_expr + F.when(rule_condition, 0.1).otherwise(0.0)

            # Add rule name to fraud_alerts array if triggered
            fraud_alerts_array = F.when(
                rule_condition,
                F.array_union(fraud_alerts_array, F.array(F.lit(rule_name)))
            ).otherwise(fraud_alerts_array)

        df_with_alerts = df_with_alerts.withColumn("fraud_score", fraud_score_expr)
        df_with_alerts = df_with_alerts.withColumn("fraud_alerts", fraud_alerts_array)

        return df_with_alerts

    def run_detection(self, df: DataFrame) -> DataFrame:
        """
        Runs the fraud detection pipeline on a DataFrame.

        Args:
            df: The input DataFrame with transaction data.

        Returns:
            DataFrame with fraud detection results.
        """
        return self.apply_rules(df)
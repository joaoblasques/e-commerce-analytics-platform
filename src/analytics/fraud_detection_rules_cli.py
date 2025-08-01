import click
from pyspark.sql import SparkSession

from src.analytics.fraud_detection_rules import FraudRuleEngine


@click.group()
def fraud_rules():
    """Rule-based fraud detection commands."""
    pass


@fraud_rules.command()
@click.option(
    "--input-path",
    required=True,
    help="Path to the input data (e.g., Delta table of transactions).",
)
@click.option(
    "--output-path", required=True, help="Path to write the fraud detection results."
)
@click.option(
    "--rules-config",
    required=True,
    help="JSON string of rules or path to a JSON file with rules.",
)
def detect(input_path: str, output_path: str, rules_config: str):
    """
    Detects fraud using a set of predefined rules.
    """
    spark = SparkSession.builder.appName("RuleBasedFraudDetection").getOrCreate()
    engine = FraudRuleEngine(spark)

    # Load rules from config
    import json

    try:
        rules_data = json.loads(rules_config)
        if isinstance(rules_data, str):
            # Assume it's a file path if it's a string after initial load
            with open(rules_data, "r") as f:
                rules_list = json.load(f)
        else:
            rules_list = rules_data
    except json.JSONDecodeError:
        # Assume it's a file path if not a valid JSON string
        with open(rules_config, "r") as f:
            rules_list = json.load(f)

    for rule in rules_list:
        engine.add_rule(rule["name"], rule["condition"], rule.get("severity", "medium"))

    # Load input data
    input_df = spark.read.format("delta").load(input_path)

    # Apply rules and write results
    result_df = engine.run_detection(input_df)
    result_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"Fraud detection complete. Results written to {output_path}")


if __name__ == "__main__":
    fraud_rules()

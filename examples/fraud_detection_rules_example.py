
from pyspark.sql import SparkSession
from src.analytics.fraud_detection_rules import FraudRuleEngine
import pyspark.sql.functions as F

def main():
    """Main function to run the rule-based fraud detection example."""
    spark = SparkSession.builder.appName("RuleBasedFraudDetectionExample").getOrCreate()

    # Create a dummy DataFrame for demonstration
    data = [
        ("txn1", "user1", 100.0, 1, "credit_card"),
        ("txn2", "user1", 1500.0, 1, "credit_card"),  # High amount
        ("txn3", "user2", 50.0, 10, "debit_card"),   # High quantity
        ("txn4", "user3", 200.0, 1, "paypal"),
        ("txn5", "user4", 3000.0, 2, "credit_card"), # High amount and quantity
    ]
    schema = ["transaction_id", "user_id", "amount", "quantity", "payment_method"]
    df = spark.createDataFrame(data, schema)

    # Initialize the FraudRuleEngine
    engine = FraudRuleEngine(spark)

    # Add some rules
    engine.add_rule("high_amount", "amount > 1000", "high")
    engine.add_rule("high_quantity", "quantity > 5", "medium")
    engine.add_rule("paypal_payment", "payment_method = 'paypal'", "low")

    print("\n--- Applying Fraud Rules ---")
    result_df = engine.run_detection(df)

    print("Fraud Detection Results:")
    result_df.show()

    print("Transactions marked as fraudulent (score > 0):")
    result_df.filter(F.col("fraud_score") > 0).show()

if __name__ == "__main__":
    main()

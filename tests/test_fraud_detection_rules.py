import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from src.analytics.fraud_detection_rules import FraudRuleEngine


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("FraudDetectionRulesTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_transaction_data(spark):
    """Provides sample transaction data for testing fraud rules."""
    data = [
        ("txn1", "user1", 100.0, 1, "credit_card"),
        ("txn2", "user1", 1500.0, 1, "credit_card"),  # High amount
        ("txn3", "user2", 50.0, 10, "debit_card"),  # High quantity
        ("txn4", "user3", 200.0, 1, "paypal"),
        ("txn5", "user4", 3000.0, 2, "credit_card"),  # High amount and quantity
    ]
    schema = ["transaction_id", "user_id", "amount", "quantity", "payment_method"]
    return spark.createDataFrame(data, schema)


def test_add_rule(spark):
    """Test adding rules to the engine."""
    engine = FraudRuleEngine(spark)
    engine.add_rule("high_amount", "amount > 1000", "high")
    assert len(engine.rules) == 1
    assert engine.rules[0]["name"] == "high_amount"


def test_apply_rules(spark, sample_transaction_data):
    """Test applying rules to data and calculating fraud score."""
    engine = FraudRuleEngine(spark)
    engine.add_rule("high_amount", "amount > 1000", "high")
    engine.add_rule("high_quantity", "quantity > 5", "medium")
    engine.add_rule("paypal_payment", "payment_method = 'paypal'", "low")

    result_df = engine.apply_rules(sample_transaction_data)
    result_df.show()

    # Verify fraud scores and alerts
    results = result_df.orderBy("transaction_id").collect()

    # txn1: No rules triggered
    assert results[0]["fraud_score"] == 0.0
    assert results[0]["fraud_alerts"] == []

    # txn2: high_amount triggered (1.0 score)
    assert results[1]["fraud_score"] == 1.0
    assert "high_amount" in results[1]["fraud_alerts"]

    # txn3: high_quantity triggered (0.5 score)
    assert results[2]["fraud_score"] == 0.5
    assert "high_quantity" in results[2]["fraud_alerts"]

    # txn4: paypal_payment triggered (0.1 score)
    assert results[3]["fraud_score"] == 0.1
    assert "paypal_payment" in results[3]["fraud_alerts"]

    # txn5: high_amount (1.0) and high_quantity (0.5) triggered
    assert results[4]["fraud_score"] == 1.5
    assert "high_amount" in results[4]["fraud_alerts"]
    assert "high_quantity" in results[4]["fraud_alerts"]

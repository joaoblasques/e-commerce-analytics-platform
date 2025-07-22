
from pyspark.sql import SparkSession
import pytest
from src.analytics.ml_fraud_models import MLFraudModel
from pyspark.ml.pipeline import PipelineModel
import pyspark.sql.functions as F

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("MLFraudModelTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

@pytest.fixture
def sample_transactions_data(spark):
    """Provides sample transaction data for testing ML fraud models."""
    data = [
        ("txn1", "user1", 100.0, 1, F.lit(0)),
        ("txn2", "user1", 1500.0, 1, F.lit(1)),  # Fraud
        ("txn3", "user2", 50.0, 10, F.lit(0)),
        ("txn4", "user3", 200.0, 1, F.lit(0)),
        ("txn5", "user4", 3000.0, 2, F.lit(1)), # Fraud
    ]
    schema = ["transaction_id", "user_id", "amount", "quantity", "is_fraud"]
    return spark.createDataFrame(data, schema)

def test_feature_engineering(spark, sample_transactions_data):
    """Test feature engineering for ML fraud model."""
    model = MLFraudModel(spark)
    features_df = model._feature_engineering(sample_transactions_data)
    assert "features" in features_df.columns
    assert "scaled_features" in features_df.columns
    assert "is_fraud" in features_df.columns

def test_train_model(spark, sample_transactions_data):
    """Test training of the ML fraud model."""
    model_instance = MLFraudModel(spark)
    # Feature engineering is done inside train_model for simplicity in this test
    # In real scenario, it would be a separate step before passing to train_model
    df_features = model_instance._feature_engineering(sample_transactions_data)
    trained_model = model_instance.train_model(df_features)
    assert isinstance(trained_model, PipelineModel)

def test_predict_fraud(spark, sample_transactions_data):
    """Test fraud prediction with a trained model."""
    model_instance = MLFraudModel(spark)
    df_features = model_instance._feature_engineering(sample_transactions_data)
    trained_model = model_instance.train_model(df_features)

    predictions_df = model_instance.predict_fraud(trained_model, sample_transactions_data)
    assert "prediction" in predictions_df.columns
    assert "probability" in predictions_df.columns
    assert predictions_df.count() == sample_transactions_data.count()

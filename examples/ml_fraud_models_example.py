
from pyspark.sql import SparkSession
from src.analytics.ml_fraud_models import MLFraudModel
from pyspark.ml.pipeline import PipelineModel
import pyspark.sql.functions as F

def main():
    """Main function to run the ML fraud models example."""
    spark = SparkSession.builder.appName("MLFraudModelsExample").getOrCreate()

    # Create a dummy DataFrame for demonstration
    data = [
        ("txn1", "user1", 100.0, 1, 0), # Not fraud
        ("txn2", "user1", 1500.0, 1, 1), # Fraud
        ("txn3", "user2", 50.0, 10, 0),
        ("txn4", "user3", 200.0, 1, 0),
        ("txn5", "user4", 3000.0, 2, 1), # Fraud
    ]
    schema = ["transaction_id", "user_id", "amount", "quantity", "is_fraud"]
    df = spark.createDataFrame(data, schema)

    # For a real scenario, you would load data from a Delta table
    # For this example, we'll use the static DataFrame.

    ml_fraud_model = MLFraudModel(spark, transactions_path="dummy_path") # Path not used for static DF

    print("\n--- Training ML Fraud Model ---")
    # In a real scenario, you'd pass a DataFrame loaded from transactions_path
    trained_model = ml_fraud_model.train_model(df)

    # Save the trained model (optional, but good practice)
    # model_path = "/tmp/ml_fraud_model"
    # trained_model.write().overwrite().save(model_path)
    # print(f"Model saved to {model_path}")

    print("\n--- Predicting Fraud with ML Model ---")
    # In a real scenario, you'd load new data from input_path
    predictions_df = ml_fraud_model.predict_fraud(trained_model, df)

    print("ML Fraud Prediction Results:")
    predictions_df.show()

    print("Transactions predicted as fraudulent:")
    predictions_df.filter(F.col("prediction") == 1).show()

if __name__ == "__main__":
    main()

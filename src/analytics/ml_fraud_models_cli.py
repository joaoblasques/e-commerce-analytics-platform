
import click
from pyspark.sql import SparkSession
from src.analytics.ml_fraud_models import MLFraudModel
from pyspark.ml.pipeline import PipelineModel

@click.group()
def ml_fraud():
    """Machine Learning Fraud Detection commands."""
    pass

@ml_fraud.command()
@click.option("--transactions-path", default="data/delta/transactions", help="Path to the transactions Delta table for training.")
@click.option("--model-output-path", required=True, help="Path to save the trained ML model.")
def train(transactions_path: str, model_output_path: str):
    """
    Trains the ML fraud detection model.
    """
    spark = SparkSession.builder.appName("MLFraudModelTraining").getOrCreate()
    ml_fraud_model = MLFraudModel(spark, transactions_path)
    model = ml_fraud_model.run_training_pipeline()
    model.write().overwrite().save(model_output_path)
    print(f"ML Fraud model trained and saved to {model_output_path}")

@ml_fraud.command()
@click.option("--model-path", required=True, help="Path to the trained ML model.")
@click.option("--input-path", required=True, help="Path to the input data for prediction.")
@click.option("--output-path", required=True, help="Path to save the prediction results.")
def predict(model_path: str, input_path: str, output_path: str):
    """
    Uses a trained ML model to predict fraud on new data.
    """
    spark = SparkSession.builder.appName("MLFraudModelPrediction").getOrCreate()
    model = PipelineModel.load(model_path)
    ml_fraud_model = MLFraudModel(spark) # transactions_path is not needed for prediction if model is loaded
    ml_fraud_model.run_prediction_pipeline(model, input_path, output_path)
    print(f"ML Fraud predictions saved to {output_path}")

if __name__ == "__main__":
    ml_fraud()

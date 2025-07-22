
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

class MLFraudModel:
    """
    A class to integrate and manage machine learning models for fraud detection.
    """

    def __init__(self, spark: SparkSession, transactions_path: str = "data/delta/transactions"):
        self.spark = spark
        self.transactions_path = transactions_path

    def _load_transactions(self) -> DataFrame:
        """
        Loads transaction data from the Delta Lake table.
        In a real scenario, this would be enriched data with features.
        """
        return self.spark.read.format("delta").load(self.transactions_path)

    def _feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Engineers features for the ML fraud model.
        For simplicity, using 'amount' and 'quantity' as features.
        In a real scenario, this would involve many more complex features.
        """
        # Assume a 'is_fraud' column exists for training, or create a dummy one for example
        # In a real scenario, this would come from labeled data.
        if "is_fraud" not in df.columns:
            df = df.withColumn("is_fraud", F.when(F.rand() < 0.01, 1).otherwise(0)) # Dummy fraud label

        feature_cols = ["amount", "quantity"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_assembled = assembler.transform(df)

        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        # Fit scaler on a sample to avoid OOM for large datasets in real streaming scenarios
        scaler_model = scaler.fit(df_assembled.limit(1000))
        df_scaled = scaler_model.transform(df_assembled)

        return df_scaled

    def train_model(self, df: DataFrame) -> PipelineModel:
        """
        Trains a RandomForestClassifier model for fraud detection.

        Args:
            df: DataFrame with engineered features and a 'is_fraud' label.

        Returns:
            A trained ML pipeline model.
        """
        # Split data into training and test sets
        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

        # RandomForestClassifier
        rf = RandomForestClassifier(labelCol="is_fraud", featuresCol="scaled_features", numTrees=10)

        # Create pipeline
        pipeline = Pipeline(stages=[rf])

        # Train the model
        model = pipeline.fit(train_data)

        # Evaluate model
        evaluator = BinaryClassificationEvaluator(labelCol="is_fraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        predictions = model.transform(test_data)
        auc = evaluator.evaluate(predictions)
        print(f"Area Under ROC Curve (AUC) on test set: {auc}")

        return model

    def predict_fraud(self, model: PipelineModel, df: DataFrame) -> DataFrame:
        """
        Applies the trained model to predict fraud on new data.

        Args:
            model: The trained ML pipeline model.
            df: DataFrame with data to predict on.

        Returns:
            DataFrame with 'prediction' (0 or 1) and 'probability' columns.
        """
        # Ensure features are engineered before prediction
        df_features = self._feature_engineering(df)
        predictions = model.transform(df_features)
        return predictions.select("transaction_id", "user_id", "amount", "quantity", "probability", "prediction")

    def run_training_pipeline(self):
        """
        Runs the full ML model training pipeline.
        """
        transactions_df = self._load_transactions()
        features_df = self._feature_engineering(transactions_df)
        model = self.train_model(features_df)
        print("ML Fraud model training complete.")
        return model

    def run_prediction_pipeline(self, model: PipelineModel, input_path: str, output_path: str):
        """
        Runs the ML model prediction pipeline on input data and writes results.
        """
        input_df = self.spark.read.format("delta").load(input_path)
        predictions_df = self.predict_fraud(model, input_df)
        predictions_df.write.format("delta").mode("overwrite").save(output_path)
        print(f"ML Fraud predictions written to {output_path}")

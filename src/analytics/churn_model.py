import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, SparkSession


class ChurnModel:
    """
    A class to represent a churn prediction model.

    This model uses customer transaction and behavior data to predict
    the likelihood of a customer churning.
    """

    def __init__(
        self, spark: SparkSession, delta_path: str = "data/delta/transactions"
    ):
        self.spark = spark
        self.delta_path = delta_path

    def _load_data(self) -> DataFrame:
        """Loads transaction data from the Delta Lake table."""
        return self.spark.read.format("delta").load(self.delta_path)

    def _feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Engineers features for the churn model.

        Args:
            df: The input DataFrame with transaction data.

        Returns:
            A DataFrame with engineered features.
        """
        # Define a churn window (e.g., 30 days of inactivity)
        churn_window = F.date_sub(F.current_date(), 30)

        # Label customers as churned or active
        customer_activity = df.groupBy("user_id").agg(
            F.max("timestamp").alias("last_purchase_date")
        )
        labeled_df = customer_activity.withColumn(
            "churn", F.when(F.col("last_purchase_date") < churn_window, 1).otherwise(0)
        )

        # Calculate RFM features
        rfm_df = df.groupBy("user_id").agg(
            F.datediff(F.current_date(), F.max("timestamp")).alias("recency"),
            F.countDistinct("transaction_id").alias("frequency"),
            F.sum("price").alias("monetary"),
        )

        # Join labels with RFM features
        features_df = labeled_df.join(rfm_df, "user_id")

        return features_df

    def train_model(self, features_df: DataFrame) -> PipelineModel:
        """
        Trains a logistic regression model.

        Args:
            features_df: A DataFrame with engineered features and labels.

        Returns:
            A trained ML pipeline.
        """
        # Assemble feature vector
        feature_columns = ["recency", "frequency", "monetary"]
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

        # Logistic Regression model
        lr = LogisticRegression(featuresCol="scaled_features", labelCol="churn")

        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, lr])

        # Train the model
        model = pipeline.fit(features_df)

        return model

    def evaluate_model(self, model: PipelineModel, test_data: DataFrame) -> float:
        """
        Evaluates the trained model.

        Args:
            model: The trained ML pipeline.
            test_data: The test DataFrame.
        """
        predictions = model.transform(test_data)
        evaluator = BinaryClassificationEvaluator(labelCol="churn")
        auc = evaluator.evaluate(predictions)
        print(f"Area Under ROC Curve (AUC): {auc}")
        return auc

    def run(self) -> PipelineModel:
        """
        Runs the full churn prediction pipeline.
        """
        # Load data
        transactions_df = self._load_data()

        # Feature engineering
        features_df = self._feature_engineering(transactions_df)

        # Split data into training and test sets
        train_data, test_data = features_df.randomSplit([0.8, 0.2], seed=42)

        # Train model
        model = self.train_model(train_data)

        # Evaluate model
        self.evaluate_model(model, test_data)

        return model

"""
Machine learning models for fraud detection.

This module implements ensemble fraud detection models that integrate with the existing
rule-based fraud detection system to provide advanced ML-based fraud identification.
"""

import json
import logging
import pickle
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.stat import Correlation
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class ModelType(Enum):
    """ML model types for fraud detection."""

    RANDOM_FOREST = "random_forest"
    GRADIENT_BOOSTING = "gradient_boosting"
    LOGISTIC_REGRESSION = "logistic_regression"
    ENSEMBLE = "ensemble"


class ModelStatus(Enum):
    """Model training and deployment status."""

    TRAINING = "training"
    TRAINED = "trained"
    DEPLOYED = "deployed"
    RETIRED = "retired"
    FAILED = "failed"


@dataclass
class ModelMetrics:
    """Model performance metrics."""

    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_roc: float
    confusion_matrix: List[List[int]]
    feature_importance: Dict[str, float]
    training_time: float
    prediction_time: float
    model_size_mb: float


@dataclass
class ModelConfiguration:
    """Configuration for fraud detection models."""

    model_type: ModelType
    features: List[str]
    hyperparameters: Dict[str, Any]
    cross_validation_folds: int = 5
    test_split_ratio: float = 0.2
    random_seed: int = 42
    max_training_time_minutes: int = 60


class MLFraudModelTrainer:
    """
    Advanced machine learning model trainer for fraud detection.

    Features:
    - Ensemble model training with multiple algorithms
    - Automated feature engineering and selection
    - Hyperparameter tuning with cross-validation
    - Model performance evaluation and comparison
    - Integration with existing fraud detection pipeline
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the ML fraud model trainer."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)
        self.models: Dict[str, Any] = {}
        self.feature_columns = []
        self.target_column = "is_fraud"

    def _get_default_config(self) -> Dict:
        """Get default configuration for model training."""
        return {
            "feature_engineering": {
                "transaction_features": [
                    "price",
                    "hour",
                    "day_of_week",
                    "is_weekend",
                    "transaction_count_1h",
                    "total_amount_1h",
                    "distinct_merchants_1h",
                    "distinct_payment_methods_1h",
                ],
                "customer_features": [
                    "customer_avg_amount",
                    "customer_max_amount",
                    "amount_deviation_from_avg",
                    "amount_z_score",
                ],
                "merchant_features": [
                    "merchant_risk_score",
                    "merchant_transaction_count",
                    "merchant_avg_amount",
                ],
                "categorical_features": [
                    "payment_method",
                    "merchant_category",
                    "customer_segment",
                ],
            },
            "models": {
                "random_forest": {
                    "numTrees": [50, 100, 200],
                    "maxDepth": [5, 10, 15],
                    "minInstancesPerNode": [1, 5, 10],
                },
                "gradient_boosting": {
                    "maxIter": [50, 100, 150],
                    "maxDepth": [3, 5, 7],
                    "stepSize": [0.1, 0.2, 0.3],
                },
                "logistic_regression": {
                    "regParam": [0.01, 0.1, 1.0],
                    "elasticNetParam": [0.0, 0.5, 1.0],
                    "maxIter": [100, 200, 300],
                },
            },
            "evaluation": {
                "primary_metric": "areaUnderROC",
                "cross_validation_folds": 5,
                "test_split_ratio": 0.2,
            },
        }

    def prepare_features(self, df: DataFrame) -> DataFrame:
        """
        Prepare features for machine learning model training.

        Args:
            df: Input DataFrame with transaction data

        Returns:
            DataFrame with engineered features
        """
        self.logger.info("Starting feature engineering for ML models")

        # Start with the input DataFrame
        features_df = df

        # Add time-based features
        features_df = (
            features_df.withColumn("hour", F.hour("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))
            .withColumn("day_of_month", F.dayofmonth("timestamp"))
            .withColumn("month", F.month("timestamp"))
        )

        # Add derived time features
        features_df = (
            features_df.withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
            .withColumn(
                "is_business_hours", (F.col("hour") >= 9) & (F.col("hour") <= 17)
            )
            .withColumn("is_unusual_hour", (F.col("hour") >= 0) & (F.col("hour") <= 5))
        )

        # Add customer aggregation features (using window functions)
        customer_window = Window.partitionBy("customer_id")

        features_df = (
            features_df.withColumn(
                "customer_avg_amount", F.avg("price").over(customer_window)
            )
            .withColumn("customer_max_amount", F.max("price").over(customer_window))
            .withColumn("customer_min_amount", F.min("price").over(customer_window))
            .withColumn(
                "customer_stddev_amount", F.stddev("price").over(customer_window)
            )
        )

        # Add amount deviation features
        features_df = features_df.withColumn(
            "amount_deviation_from_avg",
            F.abs(F.col("price") - F.col("customer_avg_amount"))
            / F.col("customer_avg_amount"),
        ).withColumn(
            "amount_z_score",
            F.when(
                F.col("customer_stddev_amount") > 0,
                (F.col("price") - F.col("customer_avg_amount"))
                / F.col("customer_stddev_amount"),
            ).otherwise(0.0),
        )

        # Add velocity features (1-hour rolling window)
        velocity_window = (
            Window.partitionBy("customer_id")
            .orderBy("timestamp")
            .rangeBetween(-3600, 0)
        )

        features_df = (
            features_df.withColumn(
                "transaction_count_1h", F.count("*").over(velocity_window)
            )
            .withColumn("total_amount_1h", F.sum("price").over(velocity_window))
            .withColumn(
                "distinct_merchants_1h",
                F.size(F.collect_set("merchant_id").over(velocity_window)),
            )
            .withColumn(
                "distinct_payment_methods_1h",
                F.size(F.collect_set("payment_method").over(velocity_window)),
            )
        )

        # Add merchant features (if merchant risk scores are available)
        if "merchant_risk_score" not in features_df.columns:
            merchant_window = Window.partitionBy("merchant_id")
            features_df = (
                features_df.withColumn(
                    "merchant_transaction_count", F.count("*").over(merchant_window)
                )
                .withColumn("merchant_avg_amount", F.avg("price").over(merchant_window))
                .withColumn("merchant_risk_score", F.lit(0.5))
            )  # Default neutral risk

        # Add fraud indicators based on business rules (for training labels)
        features_df = features_df.withColumn(
            "is_fraud",
            F.when(
                (F.col("price") > 10000)
                | (F.col("transaction_count_1h") > 10)  # High amount
                | (F.col("amount_deviation_from_avg") > 3)  # High velocity
                | (F.col("is_unusual_hour") == True)  # Unusual amount
                | (  # Unusual time
                    F.col("distinct_payment_methods_1h") > 3
                ),  # Multiple payment methods
                1,
            ).otherwise(0),
        )

        self.logger.info("Feature engineering completed")
        return features_df

    def train_ensemble_models(self, df: DataFrame) -> Dict[str, Any]:
        """
        Train ensemble fraud detection models.

        Args:
            df: Training DataFrame with features and labels

        Returns:
            Dictionary containing trained models and metrics
        """
        self.logger.info("Starting ensemble model training")

        # Prepare features
        features_df = self.prepare_features(df)

        # Select feature columns
        feature_cols = (
            self.config["feature_engineering"]["transaction_features"]
            + self.config["feature_engineering"]["customer_features"]
            + self.config["feature_engineering"]["merchant_features"]
        )

        # Handle categorical features
        categorical_cols = self.config["feature_engineering"]["categorical_features"]

        # Create feature pipeline
        stages = []

        # String indexing for categorical features
        indexed_cols = []
        for cat_col in categorical_cols:
            if cat_col in features_df.columns:
                indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_index")
                stages.append(indexer)
                indexed_cols.append(f"{cat_col}_index")

        # One-hot encoding for categorical features
        encoded_cols = []
        for indexed_col in indexed_cols:
            encoder = OneHotEncoder(
                inputCol=indexed_col, outputCol=f"{indexed_col}_encoded"
            )
            stages.append(encoder)
            encoded_cols.append(f"{indexed_col}_encoded")

        # Combine all features
        all_feature_cols = [
            col for col in feature_cols if col in features_df.columns
        ] + encoded_cols

        # Vector assembler
        assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
        stages.append(assembler)

        # Feature scaling
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        stages.append(scaler)

        # Split data
        train_df, test_df = features_df.randomSplit(
            [0.8, 0.2], seed=self.config.get("random_seed", 42)
        )

        # Train individual models
        models = {}
        metrics = {}

        # Random Forest
        rf = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol=self.target_column,
            numTrees=100,
            maxDepth=10,
            seed=42,
        )

        rf_pipeline = Pipeline(stages=stages + [rf])
        rf_model = rf_pipeline.fit(train_df)
        models["random_forest"] = rf_model

        # Gradient Boosting
        gbt = GBTClassifier(
            featuresCol="scaled_features",
            labelCol=self.target_column,
            maxIter=100,
            maxDepth=5,
            seed=42,
        )

        gbt_pipeline = Pipeline(stages=stages + [gbt])
        gbt_model = gbt_pipeline.fit(train_df)
        models["gradient_boosting"] = gbt_model

        # Logistic Regression
        lr = LogisticRegression(
            featuresCol="scaled_features",
            labelCol=self.target_column,
            maxIter=100,
            regParam=0.1,
        )

        lr_pipeline = Pipeline(stages=stages + [lr])
        lr_model = lr_pipeline.fit(train_df)
        models["logistic_regression"] = lr_model

        # Evaluate models
        evaluator_auc = BinaryClassificationEvaluator(
            rawPredictionCol="prediction",
            labelCol=self.target_column,
            metricName="areaUnderROC",
        )

        evaluator_accuracy = MulticlassClassificationEvaluator(
            predictionCol="prediction",
            labelCol=self.target_column,
            metricName="accuracy",
        )

        for model_name, model in models.items():
            predictions = model.transform(test_df)
            auc = evaluator_auc.evaluate(predictions)
            accuracy = evaluator_accuracy.evaluate(predictions)

            metrics[model_name] = {
                "auc_roc": auc,
                "accuracy": accuracy,
                "model_size_mb": 1.0,  # Placeholder
            }

            self.logger.info(f"{model_name} - AUC: {auc:.3f}, Accuracy: {accuracy:.3f}")

        # Store models and feature columns
        self.models = models
        self.feature_columns = all_feature_cols

        self.logger.info("Ensemble model training completed")
        return {
            "models": models,
            "metrics": metrics,
            "feature_columns": all_feature_cols,
            "training_data_size": train_df.count(),
            "test_data_size": test_df.count(),
        }

    def predict_fraud_probability(
        self, df: DataFrame, model_name: str = "random_forest"
    ) -> DataFrame:
        """
        Predict fraud probability using trained ML models.

        Args:
            df: DataFrame to predict on
            model_name: Name of the model to use for prediction

        Returns:
            DataFrame with fraud predictions and probabilities
        """
        if model_name not in self.models:
            raise ValueError(
                f"Model {model_name} not found. Available models: {list(self.models.keys())}"
            )

        model = self.models[model_name]

        # Prepare features (same as training)
        features_df = self.prepare_features(df)

        # Make predictions
        predictions_df = model.transform(features_df)

        # Extract probability from probability vector (for binary classification)
        predictions_df = predictions_df.withColumn(
            "fraud_probability",
            F.col("probability")[1],  # Probability of fraud class (class 1)
        ).withColumn(
            "ml_fraud_prediction",
            F.when(F.col("fraud_probability") > 0.5, 1).otherwise(0),
        )

        return predictions_df

    def get_feature_importance(
        self, model_name: str = "random_forest"
    ) -> Dict[str, float]:
        """
        Get feature importance from trained model.

        Args:
            model_name: Name of the model

        Returns:
            Dictionary with feature names and their importance scores
        """
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        model = self.models[model_name]

        # Extract the classifier stage from the pipeline
        classifier_stage = None
        for stage in model.stages:
            if hasattr(stage, "featureImportances"):
                classifier_stage = stage
                break

        if classifier_stage is None:
            return {}

        # Get feature importances
        importances = classifier_stage.featureImportances.toArray()

        # Map to feature names
        feature_importance = {}
        for i, importance in enumerate(importances):
            if i < len(self.feature_columns):
                feature_importance[self.feature_columns[i]] = float(importance)

        # Sort by importance
        return dict(
            sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        )

    def save_models(self, output_path: str) -> None:
        """
        Save trained models to disk.

        Args:
            output_path: Path to save models
        """
        for model_name, model in self.models.items():
            model_path = f"{output_path}/{model_name}"
            model.write().overwrite().save(model_path)
            self.logger.info(f"Model {model_name} saved to {model_path}")

    def load_models(self, models_path: str) -> None:
        """
        Load trained models from disk.

        Args:
            models_path: Path to load models from
        """
        from pyspark.ml import Pipeline

        model_types = ["random_forest", "gradient_boosting", "logistic_regression"]

        for model_name in model_types:
            try:
                model_path = f"{models_path}/{model_name}"
                model = Pipeline.load(model_path)
                self.models[model_name] = model
                self.logger.info(f"Model {model_name} loaded from {model_path}")
            except Exception as e:
                self.logger.warning(f"Could not load model {model_name}: {e}")

    def cross_validate_model(
        self, df: DataFrame, model_name: str = "random_forest"
    ) -> Dict[str, float]:
        """
        Perform cross-validation on a model.

        Args:
            df: Training DataFrame
            model_name: Name of the model to cross-validate

        Returns:
            Cross-validation metrics
        """
        self.logger.info(f"Starting cross-validation for {model_name}")

        # Prepare features
        features_df = self.prepare_features(df)

        # Create model pipeline (same as in train_ensemble_models)
        stages = []

        # Feature engineering stages (simplified for CV)
        feature_cols = [
            col
            for col in self.config["feature_engineering"]["transaction_features"]
            + self.config["feature_engineering"]["customer_features"]
            + self.config["feature_engineering"]["merchant_features"]
            if col in features_df.columns
        ]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        stages.extend([assembler, scaler])

        # Add classifier
        if model_name == "random_forest":
            classifier = RandomForestClassifier(
                featuresCol="scaled_features", labelCol=self.target_column, seed=42
            )
        elif model_name == "gradient_boosting":
            classifier = GBTClassifier(
                featuresCol="scaled_features", labelCol=self.target_column, seed=42
            )
        else:  # logistic_regression
            classifier = LogisticRegression(
                featuresCol="scaled_features", labelCol=self.target_column
            )

        pipeline = Pipeline(stages=stages + [classifier])

        # Set up cross-validation
        param_grid = ParamGridBuilder().build()  # Using default parameters

        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="prediction",
            labelCol=self.target_column,
            metricName="areaUnderROC",
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=self.config["evaluation"]["cross_validation_folds"],
        )

        # Run cross-validation
        cv_model = cv.fit(features_df)

        # Get average metric
        avg_metric = float(np.mean(cv_model.avgMetrics))

        self.logger.info(
            f"Cross-validation completed for {model_name}. Average AUC: {avg_metric:.3f}"
        )

        return {
            "average_auc": avg_metric,
            "std_auc": float(np.std(cv_model.avgMetrics)),
            "best_model": cv_model.bestModel,
        }

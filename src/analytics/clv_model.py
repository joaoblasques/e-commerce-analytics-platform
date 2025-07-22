"""
Customer Lifetime Value (CLV) Model

This module implements comprehensive customer lifetime value analysis including:
- Historical CLV calculation
- Predictive CLV modeling using machine learning
- Cohort analysis for customer behavior tracking
- Integration with RFM segmentation

The CLV model provides both backward-looking (historical) and forward-looking
(predictive) insights to enable data-driven customer acquisition and retention strategies.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, datediff, desc, first, last, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import months_between
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import stddev
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class CLVModelType(Enum):
    """Supported CLV modeling approaches."""

    LINEAR_REGRESSION = "linear_regression"
    RANDOM_FOREST = "random_forest"
    GRADIENT_BOOSTING = "gradient_boosting"
    ENSEMBLE = "ensemble"


class CohortPeriod(Enum):
    """Supported cohort analysis periods."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


@dataclass
class CLVMetrics:
    """Customer lifetime value metrics and calculations."""

    customer_id: str
    historical_clv: float
    predicted_clv: float
    clv_confidence: float
    customer_lifespan_days: int
    avg_order_value: float
    purchase_frequency: float
    profit_margin: float
    acquisition_date: datetime
    last_purchase_date: datetime
    churn_probability: float
    segment: Optional[str] = None
    cohort: Optional[str] = None


@dataclass
class CohortMetrics:
    """Cohort analysis metrics for customer groups."""

    cohort_period: str
    cohort_size: int
    total_revenue: float
    avg_clv: float
    retention_rate: float
    churn_rate: float
    avg_orders_per_customer: float
    avg_order_value: float
    months_tracked: int


class CLVModelEngine:
    """
    Customer Lifetime Value modeling engine.

    Provides comprehensive CLV analysis including historical calculations,
    predictive modeling, and cohort analysis capabilities.
    """

    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        prediction_horizon_months: int = 12,
        profit_margin: float = 0.2,
        model_type: CLVModelType = CLVModelType.RANDOM_FOREST,
    ):
        """
        Initialize CLV model engine.

        Args:
            spark_session: Active Spark session
            prediction_horizon_months: Months to predict CLV forward
            profit_margin: Default profit margin for CLV calculations
            model_type: Machine learning model type for predictions
        """
        self.spark = spark_session or SparkSession.getActiveSession()
        if not self.spark:
            raise ValueError("No active Spark session found")

        self.prediction_horizon_months = prediction_horizon_months
        self.profit_margin = profit_margin
        self.model_type = model_type
        self.trained_model = None
        self.feature_cols = [
            "recency_days",
            "frequency",
            "monetary",
            "avg_order_value",
            "customer_lifespan_days",
            "days_between_orders",
            "total_orders",
            "months_since_first_purchase",
        ]

    def calculate_historical_clv(
        self,
        transactions_df: DataFrame,
        customer_id_col: str = "customer_id",
        transaction_date_col: str = "transaction_date",
        amount_col: str = "amount",
        profit_margin: Optional[float] = None,
    ) -> DataFrame:
        """
        Calculate historical customer lifetime value.

        Historical CLV = Total Revenue × Profit Margin

        Args:
            transactions_df: Transaction data with customer_id, date, amount
            customer_id_col: Customer identifier column name
            transaction_date_col: Transaction date column name
            amount_col: Transaction amount column name
            profit_margin: Profit margin override

        Returns:
            DataFrame with customer historical CLV metrics
        """
        margin = profit_margin or self.profit_margin

        # Calculate customer metrics
        customer_metrics = transactions_df.groupBy(customer_id_col).agg(
            spark_sum(amount_col).alias("total_revenue"),
            count("*").alias("total_orders"),
            avg(amount_col).alias("avg_order_value"),
            spark_min(transaction_date_col).alias("first_purchase_date"),
            spark_max(transaction_date_col).alias("last_purchase_date"),
            stddev(amount_col).alias("order_value_stddev"),
        )

        # Calculate derived metrics
        historical_clv_df = (
            customer_metrics.withColumn(
                "historical_clv", col("total_revenue") * lit(margin)
            )
            .withColumn(
                "customer_lifespan_days",
                datediff(col("last_purchase_date"), col("first_purchase_date")),
            )
            .withColumn(
                "purchase_frequency",
                when(
                    col("customer_lifespan_days") > 0,
                    col("total_orders") / (col("customer_lifespan_days") / 30.0),
                ).otherwise(col("total_orders")),
            )
            .withColumn(
                "days_between_orders",
                when(
                    col("total_orders") > 1,
                    col("customer_lifespan_days") / (col("total_orders") - 1),
                ).otherwise(lit(0)),
            )
            .withColumn(
                "order_value_consistency",
                when(
                    col("avg_order_value") > 0,
                    1 - (col("order_value_stddev") / col("avg_order_value")),
                ).otherwise(lit(0)),
            )
        )

        return historical_clv_df

    def prepare_features_for_prediction(
        self, customer_metrics_df: DataFrame, reference_date: Optional[datetime] = None
    ) -> DataFrame:
        """
        Prepare feature set for CLV prediction modeling.

        Args:
            customer_metrics_df: Customer metrics DataFrame
            reference_date: Reference date for recency calculations

        Returns:
            DataFrame with engineered features for ML modeling
        """
        ref_date = reference_date or datetime.now()

        # Add time-based features
        features_df = (
            customer_metrics_df.withColumn(
                "recency_days",
                datediff(lit(ref_date.date()), col("last_purchase_date")),
            )
            .withColumn(
                "months_since_first_purchase",
                months_between(lit(ref_date.date()), col("first_purchase_date")),
            )
            .withColumn("frequency", col("total_orders").cast(DoubleType()))
            .withColumn("monetary", col("total_revenue"))
        )

        # Add behavioral features
        features_df = (
            features_df.withColumn(
                "purchase_velocity",
                when(
                    col("customer_lifespan_days") > 0,
                    col("total_orders") / col("customer_lifespan_days") * 30,
                ).otherwise(lit(0)),
            )
            .withColumn(
                "revenue_trend",
                col("total_revenue") / col("months_since_first_purchase"),
            )
            .withColumn(
                "customer_maturity",
                when(col("months_since_first_purchase") <= 3, lit("New"))
                .when(col("months_since_first_purchase") <= 12, lit("Growing"))
                .when(col("months_since_first_purchase") <= 24, lit("Mature"))
                .otherwise(lit("Veteran")),
            )
        )

        return features_df

    def train_clv_prediction_model(
        self,
        training_data: DataFrame,
        target_col: str = "historical_clv",
        test_split: float = 0.2,
    ) -> Dict:
        """
        Train machine learning model for CLV prediction.

        Args:
            training_data: Training dataset with features and target
            target_col: Target variable column name
            test_split: Proportion of data for testing

        Returns:
            Dictionary with trained model and evaluation metrics
        """
        # Prepare feature vector
        vector_assembler = VectorAssembler(
            inputCols=self.feature_cols, outputCol="features", handleInvalid="skip"
        )

        # Split data
        train_df, test_df = training_data.randomSplit(
            [1 - test_split, test_split], seed=42
        )

        # Choose model based on type
        if self.model_type == CLVModelType.LINEAR_REGRESSION:
            model = LinearRegression(
                featuresCol="features",
                labelCol=target_col,
                predictionCol="predicted_clv",
            )
        elif self.model_type == CLVModelType.RANDOM_FOREST:
            model = RandomForestRegressor(
                featuresCol="features",
                labelCol=target_col,
                predictionCol="predicted_clv",
                numTrees=100,
                maxDepth=10,
                seed=42,
            )
        elif self.model_type == CLVModelType.GRADIENT_BOOSTING:
            model = GBTRegressor(
                featuresCol="features",
                labelCol=target_col,
                predictionCol="predicted_clv",
                maxIter=100,
                maxDepth=6,
                seed=42,
            )
        else:
            # Default to Random Forest
            model = RandomForestRegressor(
                featuresCol="features",
                labelCol=target_col,
                predictionCol="predicted_clv",
                numTrees=100,
                maxDepth=10,
                seed=42,
            )

        # Create pipeline
        pipeline = Pipeline(stages=[vector_assembler, model])

        # Train model
        trained_pipeline = pipeline.fit(train_df)
        self.trained_model = trained_pipeline

        # Evaluate model
        predictions = trained_pipeline.transform(test_df)
        evaluator = RegressionEvaluator(
            labelCol=target_col, predictionCol="predicted_clv", metricName="rmse"
        )

        rmse = evaluator.evaluate(predictions)

        # Calculate additional metrics
        mae_evaluator = RegressionEvaluator(
            labelCol=target_col, predictionCol="predicted_clv", metricName="mae"
        )
        mae = mae_evaluator.evaluate(predictions)

        r2_evaluator = RegressionEvaluator(
            labelCol=target_col, predictionCol="predicted_clv", metricName="r2"
        )
        r2 = r2_evaluator.evaluate(predictions)

        return {
            "model": trained_pipeline,
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "train_count": train_df.count(),
            "test_count": test_df.count(),
            "feature_importance": self._get_feature_importance(trained_pipeline),
        }

    def predict_clv(
        self, customer_features_df: DataFrame, model: Optional[Pipeline] = None
    ) -> DataFrame:
        """
        Predict customer lifetime value using trained model.

        Args:
            customer_features_df: Customer features for prediction
            model: Trained model pipeline (uses self.trained_model if None)

        Returns:
            DataFrame with CLV predictions and confidence scores
        """
        prediction_model = model or self.trained_model

        if not prediction_model:
            raise ValueError("No trained model available. Train model first.")

        # Generate predictions
        predictions_df = prediction_model.transform(customer_features_df)

        # Add confidence scores (simplified approach)
        predictions_with_confidence = predictions_df.withColumn(
            "clv_confidence",
            when(
                col("predicted_clv") > 0,
                spark_round(1.0 / (1.0 + col("predicted_clv") / 1000), 3),
            ).otherwise(lit(0.1)),
        ).withColumn(
            "predicted_clv",
            when(col("predicted_clv") < 0, lit(0)).otherwise(
                spark_round(col("predicted_clv"), 2)
            ),
        )

        return predictions_with_confidence

    def calculate_cohort_analysis(
        self,
        transactions_df: DataFrame,
        cohort_period: CohortPeriod = CohortPeriod.MONTHLY,
        customer_id_col: str = "customer_id",
        transaction_date_col: str = "transaction_date",
        amount_col: str = "amount",
    ) -> DataFrame:
        """
        Perform cohort analysis to track customer behavior over time.

        Args:
            transactions_df: Transaction data
            cohort_period: Grouping period for cohorts
            customer_id_col: Customer identifier column
            transaction_date_col: Transaction date column
            amount_col: Transaction amount column

        Returns:
            DataFrame with cohort analysis results
        """
        # Define cohort grouping
        if cohort_period == CohortPeriod.MONTHLY:
            period_format = "yyyy-MM"
        elif cohort_period == CohortPeriod.QUARTERLY:
            period_format = "yyyy-QQ"
        else:
            period_format = "yyyy"

        # Get customer first purchase dates (cohort assignment)
        customer_cohorts = (
            transactions_df.groupBy(customer_id_col)
            .agg(spark_min(transaction_date_col).alias("first_purchase_date"))
            .withColumn(
                "cohort_period",
                col("first_purchase_date")
                .cast(StringType())
                .substr(1, 7),  # YYYY-MM format
            )
        )

        # Join transactions with cohort data
        transactions_with_cohorts = transactions_df.join(
            customer_cohorts, customer_id_col, "left"
        ).withColumn(
            "transaction_period",
            col(transaction_date_col).cast(StringType()).substr(1, 7),
        )

        # Calculate cohort metrics
        cohort_metrics = (
            transactions_with_cohorts.groupBy("cohort_period")
            .agg(
                count(customer_id_col).alias("cohort_size"),
                spark_sum(amount_col).alias("total_revenue"),
                avg(amount_col).alias("avg_order_value"),
                count("*").alias("total_transactions"),
            )
            .withColumn("avg_clv", col("total_revenue") / col("cohort_size"))
            .withColumn(
                "avg_orders_per_customer",
                col("total_transactions") / col("cohort_size"),
            )
        )

        return cohort_metrics.orderBy("cohort_period")

    def integrate_with_rfm_segments(
        self,
        clv_df: DataFrame,
        rfm_segments_df: DataFrame,
        customer_id_col: str = "customer_id",
    ) -> DataFrame:
        """
        Integrate CLV predictions with RFM customer segments.

        Args:
            clv_df: CLV predictions DataFrame
            rfm_segments_df: RFM segments DataFrame
            customer_id_col: Customer identifier column

        Returns:
            Combined DataFrame with CLV and RFM insights
        """
        # Join CLV with RFM segments
        integrated_df = clv_df.join(
            rfm_segments_df.select(
                customer_id_col,
                "segment",
                "rfm_score",
                "customer_value_tier",
                "engagement_level",
            ),
            customer_id_col,
            "left",
        )

        # Add segment-based CLV insights
        segment_enhanced_df = integrated_df.withColumn(
            "clv_segment_alignment",
            when(
                (col("segment") == "Champions") & (col("predicted_clv") > 1000),
                "High Value Aligned",
            )
            .when(
                (col("segment") == "Lost") & (col("predicted_clv") < 100),
                "Low Value Aligned",
            )
            .when(
                (col("segment").isin("Champions", "Loyal Customers"))
                & (col("predicted_clv") < 500),
                "Segment Mismatch - Low CLV",
            )
            .when(
                (col("segment").isin("Lost", "Hibernating"))
                & (col("predicted_clv") > 800),
                "Segment Mismatch - High CLV",
            )
            .otherwise("Standard Alignment"),
        ).withColumn(
            "retention_priority",
            when(col("segment") == "Cannot Lose Them", "Critical")
            .when(col("segment") == "At Risk", "High")
            .when(col("segment").isin("Champions", "Loyal Customers"), "Medium")
            .otherwise("Low"),
        )

        return segment_enhanced_df

    def generate_clv_insights(
        self,
        transactions_df: DataFrame,
        rfm_segments_df: Optional[DataFrame] = None,
        customer_id_col: str = "customer_id",
        transaction_date_col: str = "transaction_date",
        amount_col: str = "amount",
    ) -> DataFrame:
        """
        Generate comprehensive CLV insights combining all analysis methods.

        Args:
            transactions_df: Transaction data
            rfm_segments_df: Optional RFM segments for integration
            customer_id_col: Customer identifier column
            transaction_date_col: Transaction date column
            amount_col: Transaction amount column

        Returns:
            Comprehensive CLV insights DataFrame
        """
        # Calculate historical CLV
        historical_clv = self.calculate_historical_clv(
            transactions_df, customer_id_col, transaction_date_col, amount_col
        )

        # Prepare features for prediction
        features_df = self.prepare_features_for_prediction(historical_clv)

        # Train prediction model if not already trained
        if not self.trained_model:
            model_results = self.train_clv_prediction_model(features_df)
            print(
                f"Model trained with R² = {model_results['r2']:.3f}, RMSE = {model_results['rmse']:.2f}"
            )

        # Generate predictions
        clv_predictions = self.predict_clv(features_df)

        # Integrate with RFM if available
        if rfm_segments_df:
            final_insights = self.integrate_with_rfm_segments(
                clv_predictions, rfm_segments_df, customer_id_col
            )
        else:
            final_insights = clv_predictions

        # Add business recommendations
        insights_with_recommendations = final_insights.withColumn(
            "recommended_action",
            when(
                col("predicted_clv") > 2000,
                "VIP Treatment: Offer premium services and exclusive benefits",
            )
            .when(
                (col("predicted_clv") > 1000) & (col("recency_days") > 90),
                "Retention Campaign: Re-engage with personalized offers",
            )
            .when(
                (col("predicted_clv") > 500) & (col("frequency") < 2),
                "Upsell Opportunity: Increase purchase frequency",
            )
            .when(
                col("predicted_clv") < 100,
                "Cost Management: Minimize acquisition costs",
            )
            .otherwise("Standard Marketing: Continue regular engagement"),
        )

        return insights_with_recommendations

    def get_clv_summary_statistics(self, clv_df: DataFrame) -> DataFrame:
        """
        Generate summary statistics for CLV analysis.

        Args:
            clv_df: CLV insights DataFrame

        Returns:
            Summary statistics DataFrame
        """
        summary_stats = clv_df.agg(
            count("*").alias("total_customers"),
            avg("historical_clv").alias("avg_historical_clv"),
            avg("predicted_clv").alias("avg_predicted_clv"),
            spark_max("predicted_clv").alias("max_predicted_clv"),
            spark_min("predicted_clv").alias("min_predicted_clv"),
            stddev("predicted_clv").alias("clv_stddev"),
            spark_sum("historical_clv").alias("total_historical_value"),
            spark_sum("predicted_clv").alias("total_predicted_value"),
        )

        return summary_stats

    def _get_feature_importance(self, trained_pipeline: Pipeline) -> Dict:
        """Extract feature importance from trained model."""
        try:
            # Get the model from pipeline
            model_stage = trained_pipeline.stages[-1]

            if hasattr(model_stage, "featureImportances"):
                importances = model_stage.featureImportances.toArray()
                return dict(zip(self.feature_cols, importances))
            else:
                return {"info": "Feature importance not available for this model type"}
        except Exception as e:
            return {"error": f"Could not extract feature importance: {str(e)}"}

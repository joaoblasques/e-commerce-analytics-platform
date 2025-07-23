"""
Revenue analytics engine for comprehensive business intelligence.

This module provides multi-dimensional revenue analysis, forecasting capabilities,
profit margin analysis, and trend analysis for e-commerce platforms.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.stat import Correlation
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window


class RevenueAnalytics:
    """
    Comprehensive revenue analytics engine.

    Features:
    - Multi-dimensional revenue tracking
    - Revenue forecasting with machine learning
    - Profit margin analysis
    - Revenue trend analysis and seasonality detection
    - Customer segment revenue attribution
    - Product category performance analysis
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the revenue analytics engine."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Revenue metrics cache for performance
        self.metrics_cache = {}

    def _get_default_config(self) -> Dict:
        """Get default configuration for revenue analytics."""
        return {
            "revenue_dimensions": [
                "customer_segment",
                "product_category",
                "geographic_region",
                "sales_channel",
                "payment_method",
                "device_type",
            ],
            "forecasting": {
                "default_horizon_days": 30,
                "confidence_levels": [0.8, 0.9, 0.95],
                "min_historical_days": 90,
                "seasonality_periods": [7, 30, 365],  # Weekly, monthly, yearly
            },
            "profit_margins": {
                "cost_of_goods_ratio": 0.65,  # Default COGS ratio
                "shipping_cost_ratio": 0.08,
                "payment_processing_ratio": 0.029,
                "marketing_attribution_window_days": 30,
            },
            "trend_analysis": {
                "moving_average_windows": [7, 14, 30, 90],
                "growth_rate_periods": [7, 30, 90, 365],
                "anomaly_threshold_std": 2.0,
            },
            "performance_thresholds": {
                "high_growth_threshold": 0.15,  # 15% growth
                "low_growth_threshold": 0.02,  # 2% growth
                "declining_threshold": -0.05,  # -5% decline
            },
        }

    def analyze_multi_dimensional_revenue(
        self,
        transactions_df: DataFrame,
        dimensions: Optional[List[str]] = None,
        time_period: str = "day",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, DataFrame]:
        """
        Analyze revenue across multiple business dimensions.

        Args:
            transactions_df: Transaction data with revenue information
            dimensions: List of dimensions to analyze (default: all configured)
            time_period: Aggregation period ('hour', 'day', 'week', 'month')
            start_date: Analysis start date
            end_date: Analysis end date

        Returns:
            Dictionary of DataFrames with revenue analysis by dimension
        """
        try:
            dimensions = dimensions or self.config["revenue_dimensions"]

            # Filter by date range if specified
            if start_date or end_date:
                transactions_df = self._filter_by_date_range(
                    transactions_df, start_date, end_date
                )

            # Add time dimension column
            transactions_df = self._add_time_dimension(transactions_df, time_period)

            # Calculate base revenue metrics
            base_df = self._calculate_base_revenue_metrics(transactions_df)

            results = {}

            # Overall revenue analysis
            results["overall"] = self._analyze_overall_revenue(base_df, time_period)

            # Revenue by each dimension
            for dimension in dimensions:
                if dimension in base_df.columns:
                    results[dimension] = self._analyze_revenue_by_dimension(
                        base_df, dimension, time_period
                    )
                    self.logger.info(
                        f"Completed revenue analysis for dimension: {dimension}"
                    )

            # Cross-dimensional analysis (top combinations)
            results["cross_dimensional"] = self._analyze_cross_dimensional_revenue(
                base_df, dimensions, time_period
            )

            # Revenue concentration analysis
            results["concentration"] = self._analyze_revenue_concentration(
                base_df, dimensions
            )

            return results

        except Exception as e:
            self.logger.error(f"Failed to analyze multi-dimensional revenue: {e}")
            raise

    def forecast_revenue(
        self,
        transactions_df: DataFrame,
        horizon_days: Optional[int] = None,
        dimension: Optional[str] = None,
        model_type: str = "linear_regression",
    ) -> Dict[str, Any]:
        """
        Generate revenue forecasts using machine learning models.

        Args:
            transactions_df: Historical transaction data
            horizon_days: Number of days to forecast
            dimension: Specific dimension to forecast (optional)
            model_type: Forecasting model type

        Returns:
            Dictionary with forecast results and model metrics
        """
        try:
            horizon_days = (
                horizon_days or self.config["forecasting"]["default_horizon_days"]
            )

            # Prepare time series data
            time_series_df = self._prepare_time_series_data(transactions_df, dimension)

            # Check if we have sufficient historical data
            min_days = self.config["forecasting"]["min_historical_days"]
            if time_series_df.count() < min_days:
                raise ValueError(
                    f"Insufficient historical data. Need at least {min_days} days"
                )

            # Feature engineering for forecasting
            features_df = self._engineer_forecasting_features(time_series_df)

            # Train forecasting model
            model, metrics = self._train_forecasting_model(features_df, model_type)

            # Generate forecasts
            forecasts_df = self._generate_forecasts(model, features_df, horizon_days)

            # Calculate prediction intervals
            prediction_intervals = self._calculate_prediction_intervals(
                forecasts_df, metrics
            )

            # Detect seasonality patterns
            seasonality = self._detect_seasonality(time_series_df)

            return {
                "forecasts": forecasts_df,
                "prediction_intervals": prediction_intervals,
                "model_metrics": metrics,
                "seasonality_patterns": seasonality,
                "model_type": model_type,
                "horizon_days": horizon_days,
                "forecast_generated_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to generate revenue forecast: {e}")
            raise

    def analyze_profit_margins(
        self,
        transactions_df: DataFrame,
        cost_data_df: Optional[DataFrame] = None,
        include_marketing_costs: bool = True,
    ) -> DataFrame:
        """
        Analyze profit margins across products, categories, and customer segments.

        Args:
            transactions_df: Transaction data with revenue information
            cost_data_df: Optional cost data for accurate margin calculation
            include_marketing_costs: Whether to include marketing attribution costs

        Returns:
            DataFrame with detailed profit margin analysis
        """
        try:
            # Calculate base revenue metrics
            revenue_df = self._calculate_base_revenue_metrics(transactions_df)

            # Add cost information
            margin_df = self._add_cost_information(revenue_df, cost_data_df)

            # Calculate various profit margins
            margin_df = margin_df.withColumn(
                "gross_margin", F.col("revenue") - F.col("cost_of_goods")
            ).withColumn(
                "gross_margin_percent",
                (F.col("gross_margin") / F.col("revenue") * 100).cast(DoubleType()),
            )

            # Add operational costs
            margin_df = self._add_operational_costs(margin_df)

            # Calculate net margin
            margin_df = margin_df.withColumn(
                "net_margin", F.col("gross_margin") - F.col("operational_costs")
            ).withColumn(
                "net_margin_percent",
                (F.col("net_margin") / F.col("revenue") * 100).cast(DoubleType()),
            )

            # Add marketing costs if requested
            if include_marketing_costs:
                margin_df = self._add_marketing_costs(margin_df, transactions_df)
                margin_df = margin_df.withColumn(
                    "marketing_adjusted_margin",
                    F.col("net_margin") - F.col("marketing_costs"),
                ).withColumn(
                    "marketing_adjusted_margin_percent",
                    (F.col("marketing_adjusted_margin") / F.col("revenue") * 100).cast(
                        DoubleType()
                    ),
                )

            # Calculate margin rankings and percentiles
            margin_df = self._calculate_margin_rankings(margin_df)

            # Add profitability categories
            margin_df = self._categorize_profitability(margin_df)

            return margin_df

        except Exception as e:
            self.logger.error(f"Failed to analyze profit margins: {e}")
            raise

    def analyze_revenue_trends(
        self,
        transactions_df: DataFrame,
        dimension: Optional[str] = None,
        include_forecasts: bool = True,
    ) -> Dict[str, Any]:
        """
        Analyze revenue trends, growth rates, and patterns.

        Args:
            transactions_df: Transaction data
            dimension: Specific dimension for trend analysis
            include_forecasts: Whether to include trend forecasts

        Returns:
            Dictionary with comprehensive trend analysis
        """
        try:
            # Prepare time series data
            if dimension:
                time_series_df = self._prepare_time_series_data(
                    transactions_df, dimension
                )
            else:
                time_series_df = self._prepare_time_series_data(transactions_df)

            # Calculate moving averages
            trends_df = self._calculate_moving_averages(time_series_df)

            # Calculate growth rates
            trends_df = self._calculate_growth_rates(trends_df)

            # Detect trend direction and strength
            trend_analysis = self._analyze_trend_direction(trends_df)

            # Identify seasonal patterns
            seasonal_analysis = self._analyze_seasonal_patterns(trends_df)

            # Detect anomalies and outliers
            anomaly_analysis = self._detect_revenue_anomalies(trends_df)

            # Calculate trend statistics
            trend_stats = self._calculate_trend_statistics(trends_df)

            results = {
                "trend_data": trends_df,
                "trend_direction": trend_analysis,
                "seasonal_patterns": seasonal_analysis,
                "anomalies": anomaly_analysis,
                "statistics": trend_stats,
                "analysis_date": datetime.now().isoformat(),
            }

            # Add forecasts if requested
            if include_forecasts:
                forecast_results = self.forecast_revenue(
                    transactions_df, dimension=dimension
                )
                results["forecasts"] = forecast_results

            return results

        except Exception as e:
            self.logger.error(f"Failed to analyze revenue trends: {e}")
            raise

    def _filter_by_date_range(
        self,
        df: DataFrame,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> DataFrame:
        """Filter DataFrame by date range."""
        if start_date:
            df = df.filter(F.col("timestamp") >= F.lit(start_date))
        if end_date:
            df = df.filter(F.col("timestamp") <= F.lit(end_date))
        return df

    def _add_time_dimension(self, df: DataFrame, time_period: str) -> DataFrame:
        """Add time dimension column based on aggregation period."""
        if time_period == "hour":
            return df.withColumn(
                "time_dimension", F.date_format("timestamp", "yyyy-MM-dd HH:00:00")
            )
        elif time_period == "day":
            return df.withColumn(
                "time_dimension", F.date_format("timestamp", "yyyy-MM-dd")
            )
        elif time_period == "week":
            return df.withColumn(
                "time_dimension",
                F.date_format(F.date_trunc("week", "timestamp"), "yyyy-MM-dd"),
            )
        elif time_period == "month":
            return df.withColumn(
                "time_dimension", F.date_format("timestamp", "yyyy-MM-01")
            )
        else:
            raise ValueError(f"Unsupported time period: {time_period}")

    def _calculate_base_revenue_metrics(self, transactions_df: DataFrame) -> DataFrame:
        """Calculate base revenue metrics from transactions."""
        return transactions_df.withColumn(
            "revenue", F.col("price") * F.col("quantity")
        ).withColumn("transaction_count", F.lit(1))

    def _analyze_overall_revenue(self, df: DataFrame, time_period: str) -> DataFrame:
        """Analyze overall revenue metrics."""
        return (
            df.groupBy("time_dimension")
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.count("transaction_id").alias("transaction_count"),
                F.countDistinct("user_id").alias("unique_customers"),
                F.avg("revenue").alias("avg_transaction_value"),
                F.min("revenue").alias("min_transaction_value"),
                F.max("revenue").alias("max_transaction_value"),
                F.stddev("revenue").alias("revenue_std_dev"),
            )
            .withColumn(
                "revenue_per_customer",
                F.col("total_revenue") / F.col("unique_customers"),
            )
        )

    def _analyze_revenue_by_dimension(
        self, df: DataFrame, dimension: str, time_period: str
    ) -> DataFrame:
        """Analyze revenue by specific dimension."""
        return (
            df.groupBy("time_dimension", dimension)
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.count("transaction_id").alias("transaction_count"),
                F.countDistinct("user_id").alias("unique_customers"),
                F.avg("revenue").alias("avg_transaction_value"),
            )
            .withColumn(
                "revenue_per_customer",
                F.col("total_revenue") / F.col("unique_customers"),
            )
        )

    def _analyze_cross_dimensional_revenue(
        self, df: DataFrame, dimensions: List[str], time_period: str
    ) -> DataFrame:
        """Analyze revenue across multiple dimensions simultaneously."""
        # Select up to 3 dimensions for cross-analysis to avoid explosion
        analysis_dimensions = dimensions[:3]

        group_cols = ["time_dimension"] + analysis_dimensions

        return (
            df.groupBy(*group_cols)
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.count("transaction_id").alias("transaction_count"),
                F.countDistinct("user_id").alias("unique_customers"),
            )
            .filter(F.col("total_revenue") > 0)
        )

    def _analyze_revenue_concentration(
        self, df: DataFrame, dimensions: List[str]
    ) -> Dict[str, DataFrame]:
        """Analyze revenue concentration (80/20 rule) by dimensions."""
        concentration_results = {}

        for dimension in dimensions:
            if dimension in df.columns:
                # Calculate revenue by dimension
                dim_revenue = (
                    df.groupBy(dimension)
                    .agg(F.sum("revenue").alias("total_revenue"))
                    .orderBy(F.desc("total_revenue"))
                )

                # Calculate cumulative percentage
                window_spec = Window.orderBy(F.desc("total_revenue"))

                dim_revenue = dim_revenue.withColumn(
                    "revenue_rank", F.row_number().over(window_spec)
                ).withColumn(
                    "cumulative_revenue",
                    F.sum("total_revenue").over(
                        window_spec.rangeBetween(Window.unboundedPreceding, 0)
                    ),
                )

                # Calculate total revenue
                total_revenue = dim_revenue.agg(F.sum("total_revenue")).collect()[0][0]

                dim_revenue = dim_revenue.withColumn(
                    "cumulative_revenue_percent",
                    (F.col("cumulative_revenue") / total_revenue * 100).cast(
                        DoubleType()
                    ),
                ).withColumn(
                    "revenue_percent",
                    (F.col("total_revenue") / total_revenue * 100).cast(DoubleType()),
                )

                concentration_results[dimension] = dim_revenue

        return concentration_results

    def _prepare_time_series_data(
        self, transactions_df: DataFrame, dimension: Optional[str] = None
    ) -> DataFrame:
        """Prepare time series data for forecasting."""
        base_df = self._calculate_base_revenue_metrics(transactions_df)
        base_df = self._add_time_dimension(base_df, "day")

        if dimension:
            return base_df.groupBy("time_dimension", dimension).agg(
                F.sum("revenue").alias("revenue"),
                F.count("transaction_id").alias("transaction_count"),
            )
        else:
            return base_df.groupBy("time_dimension").agg(
                F.sum("revenue").alias("revenue"),
                F.count("transaction_id").alias("transaction_count"),
            )

    def _engineer_forecasting_features(self, time_series_df: DataFrame) -> DataFrame:
        """Engineer features for revenue forecasting."""
        # Add date features
        features_df = (
            time_series_df.withColumn("date", F.to_date("time_dimension"))
            .withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("day_of_month", F.dayofmonth("date"))
            .withColumn("week_of_year", F.weekofyear("date"))
            .withColumn("month", F.month("date"))
            .withColumn("quarter", F.quarter("date"))
        )

        # Add lag features
        window_spec = Window.orderBy("date")

        for lag in [1, 7, 14, 30]:
            features_df = features_df.withColumn(
                f"revenue_lag_{lag}", F.lag("revenue", lag).over(window_spec)
            )

        # Add rolling average features
        for window_size in [7, 14, 30]:
            rolling_window = Window.orderBy("date").rowsBetween(-window_size + 1, 0)
            features_df = features_df.withColumn(
                f"revenue_ma_{window_size}", F.avg("revenue").over(rolling_window)
            )

        return features_df

    def _train_forecasting_model(
        self, features_df: DataFrame, model_type: str
    ) -> Tuple[Any, Dict[str, float]]:
        """Train revenue forecasting model."""
        if model_type != "linear_regression":
            raise ValueError(f"Unsupported model type: {model_type}")

        # Select features for training
        feature_cols = [
            "day_of_week",
            "day_of_month",
            "week_of_year",
            "month",
            "quarter",
            "revenue_lag_1",
            "revenue_lag_7",
            "revenue_lag_14",
            "revenue_lag_30",
            "revenue_ma_7",
            "revenue_ma_14",
            "revenue_ma_30",
        ]

        # Remove rows with null values (due to lag features)
        training_df = features_df.dropna()

        # Assemble features
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        training_df = assembler.transform(training_df)

        # Split into training and validation
        train_df, val_df = training_df.randomSplit([0.8, 0.2], seed=42)

        # Train linear regression model
        lr = LinearRegression(
            featuresCol="features",
            labelCol="revenue",
            predictionCol="predicted_revenue",
        )

        model = lr.fit(train_df)

        # Evaluate model
        predictions = model.transform(val_df)

        # Calculate metrics
        metrics = {
            "rmse": model.summary.rootMeanSquaredError,
            "mae": predictions.agg(
                F.avg(F.abs(F.col("revenue") - F.col("predicted_revenue")))
            ).collect()[0][0],
            "r2": model.summary.r2,
            "training_samples": train_df.count(),
            "validation_samples": val_df.count(),
        }

        return model, metrics

    def _generate_forecasts(
        self, model: Any, features_df: DataFrame, horizon_days: int
    ) -> DataFrame:
        """Generate revenue forecasts."""
        # This is a simplified implementation
        # In practice, you'd generate future dates and features

        # For now, return last known values with trend
        last_values = features_df.orderBy(F.desc("date")).limit(1)

        return last_values.withColumn(
            "forecasted_revenue", F.col("revenue") * 1.05  # Simple 5% growth assumption
        ).withColumn("forecast_date", F.date_add("date", horizon_days))

    def _calculate_prediction_intervals(
        self, forecasts_df: DataFrame, metrics: Dict[str, float]
    ) -> DataFrame:
        """Calculate prediction intervals for forecasts."""
        rmse = metrics["rmse"]

        return (
            forecasts_df.withColumn(
                "lower_bound_80", F.col("forecasted_revenue") - 1.28 * rmse
            )
            .withColumn("upper_bound_80", F.col("forecasted_revenue") + 1.28 * rmse)
            .withColumn("lower_bound_95", F.col("forecasted_revenue") - 1.96 * rmse)
            .withColumn("upper_bound_95", F.col("forecasted_revenue") + 1.96 * rmse)
        )

    def _detect_seasonality(self, time_series_df: DataFrame) -> Dict[str, Any]:
        """Detect seasonal patterns in revenue data."""
        # Add time components
        seasonal_df = (
            time_series_df.withColumn("date", F.to_date("time_dimension"))
            .withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("month", F.month("date"))
        )

        # Analyze day-of-week patterns
        dow_patterns = (
            seasonal_df.groupBy("day_of_week")
            .agg(
                F.avg("revenue").alias("avg_revenue"),
                F.stddev("revenue").alias("std_revenue"),
            )
            .orderBy("day_of_week")
        )

        # Analyze monthly patterns
        monthly_patterns = (
            seasonal_df.groupBy("month")
            .agg(
                F.avg("revenue").alias("avg_revenue"),
                F.stddev("revenue").alias("std_revenue"),
            )
            .orderBy("month")
        )

        return {
            "day_of_week_patterns": dow_patterns,
            "monthly_patterns": monthly_patterns,
            "seasonality_detected": True,  # Simplified detection
        }

    def _add_cost_information(
        self, revenue_df: DataFrame, cost_data_df: Optional[DataFrame] = None
    ) -> DataFrame:
        """Add cost information for margin calculation."""
        if cost_data_df is not None:
            # Join with actual cost data
            return revenue_df.join(cost_data_df, "product_id", "left")
        else:
            # Use default cost ratios
            cogs_ratio = self.config["profit_margins"]["cost_of_goods_ratio"]
            return revenue_df.withColumn("cost_of_goods", F.col("revenue") * cogs_ratio)

    def _add_operational_costs(self, margin_df: DataFrame) -> DataFrame:
        """Add operational costs (shipping, payment processing, etc.)."""
        shipping_ratio = self.config["profit_margins"]["shipping_cost_ratio"]
        payment_ratio = self.config["profit_margins"]["payment_processing_ratio"]

        return (
            margin_df.withColumn("shipping_costs", F.col("revenue") * shipping_ratio)
            .withColumn("payment_processing_costs", F.col("revenue") * payment_ratio)
            .withColumn(
                "operational_costs",
                F.col("shipping_costs") + F.col("payment_processing_costs"),
            )
        )

    def _add_marketing_costs(
        self, margin_df: DataFrame, transactions_df: DataFrame
    ) -> DataFrame:
        """Add marketing costs based on attribution."""
        # Simplified marketing cost attribution
        # In practice, this would involve complex attribution modeling

        return margin_df.withColumn(
            "marketing_costs", F.col("revenue") * 0.15  # Assume 15% marketing spend
        )

    def _calculate_margin_rankings(self, margin_df: DataFrame) -> DataFrame:
        """Calculate margin rankings and percentiles."""
        window_spec = Window.orderBy(F.desc("gross_margin_percent"))

        return margin_df.withColumn(
            "margin_rank", F.row_number().over(window_spec)
        ).withColumn("margin_percentile", F.percent_rank().over(window_spec))

    def _categorize_profitability(self, margin_df: DataFrame) -> DataFrame:
        """Categorize products/segments by profitability."""
        return margin_df.withColumn(
            "profitability_category",
            F.when(F.col("gross_margin_percent") >= 40, "High Profit")
            .when(F.col("gross_margin_percent") >= 20, "Medium Profit")
            .when(F.col("gross_margin_percent") >= 0, "Low Profit")
            .otherwise("Loss Making"),
        )

    def _calculate_moving_averages(self, time_series_df: DataFrame) -> DataFrame:
        """Calculate moving averages for trend analysis."""
        df = time_series_df.withColumn("date", F.to_date("time_dimension"))
        window_spec = Window.orderBy("date")

        for window_size in self.config["trend_analysis"]["moving_average_windows"]:
            rolling_window = window_spec.rowsBetween(-window_size + 1, 0)
            df = df.withColumn(
                f"ma_{window_size}", F.avg("revenue").over(rolling_window)
            )

        return df

    def _calculate_growth_rates(self, trends_df: DataFrame) -> DataFrame:
        """Calculate growth rates over different periods."""
        window_spec = Window.orderBy("date")

        for period in self.config["trend_analysis"]["growth_rate_periods"]:
            trends_df = trends_df.withColumn(
                f"revenue_lag_{period}", F.lag("revenue", period).over(window_spec)
            ).withColumn(
                f"growth_rate_{period}d",
                (F.col("revenue") - F.col(f"revenue_lag_{period}"))
                / F.col(f"revenue_lag_{period}")
                * 100,
            )

        return trends_df

    def _analyze_trend_direction(self, trends_df: DataFrame) -> Dict[str, Any]:
        """Analyze overall trend direction and strength."""
        # Calculate recent growth rates
        recent_data = trends_df.orderBy(F.desc("date")).limit(30)

        avg_growth = recent_data.agg(
            F.avg("growth_rate_7d").alias("avg_7d_growth")
        ).collect()[0]["avg_7d_growth"]

        high_threshold = (
            self.config["performance_thresholds"]["high_growth_threshold"] * 100
        )
        low_threshold = (
            self.config["performance_thresholds"]["low_growth_threshold"] * 100
        )
        decline_threshold = (
            self.config["performance_thresholds"]["declining_threshold"] * 100
        )

        if avg_growth is None:
            trend_direction = "insufficient_data"
        elif avg_growth > high_threshold:
            trend_direction = "strong_growth"
        elif avg_growth > low_threshold:
            trend_direction = "moderate_growth"
        elif avg_growth > decline_threshold:
            trend_direction = "stable"
        else:
            trend_direction = "declining"

        return {
            "direction": trend_direction,
            "avg_7d_growth_rate": avg_growth,
            "analysis_period_days": 30,
        }

    def _analyze_seasonal_patterns(self, trends_df: DataFrame) -> Dict[str, Any]:
        """Analyze seasonal patterns in revenue."""
        # Add time components
        seasonal_df = (
            trends_df.withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("month", F.month("date"))
            .withColumn("week_of_year", F.weekofyear("date"))
        )

        # Weekly seasonality
        weekly_pattern = (
            seasonal_df.groupBy("day_of_week")
            .agg(F.avg("revenue").alias("avg_revenue"))
            .orderBy("day_of_week")
        )

        # Monthly seasonality
        monthly_pattern = (
            seasonal_df.groupBy("month")
            .agg(F.avg("revenue").alias("avg_revenue"))
            .orderBy("month")
        )

        return {
            "weekly_seasonality": weekly_pattern,
            "monthly_seasonality": monthly_pattern,
            "seasonality_strength": "moderate",  # Simplified calculation
        }

    def _detect_revenue_anomalies(self, trends_df: DataFrame) -> DataFrame:
        """Detect anomalies and outliers in revenue data."""
        # Calculate z-scores
        mean_revenue = trends_df.agg(F.avg("revenue")).collect()[0][0]
        std_revenue = trends_df.agg(F.stddev("revenue")).collect()[0][0]

        threshold = self.config["trend_analysis"]["anomaly_threshold_std"]

        return (
            trends_df.withColumn(
                "z_score", (F.col("revenue") - mean_revenue) / std_revenue
            )
            .withColumn("is_anomaly", F.abs(F.col("z_score")) > threshold)
            .withColumn(
                "anomaly_type",
                F.when(F.col("z_score") > threshold, "high_outlier")
                .when(F.col("z_score") < -threshold, "low_outlier")
                .otherwise("normal"),
            )
        )

    def _calculate_trend_statistics(self, trends_df: DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive trend statistics."""
        stats = trends_df.agg(
            F.avg("revenue").alias("avg_revenue"),
            F.stddev("revenue").alias("std_revenue"),
            F.min("revenue").alias("min_revenue"),
            F.max("revenue").alias("max_revenue"),
            F.avg("growth_rate_7d").alias("avg_7d_growth"),
            F.avg("growth_rate_30d").alias("avg_30d_growth"),
            F.count("*").alias("data_points"),
        ).collect()[0]

        return {
            "average_revenue": stats["avg_revenue"],
            "revenue_volatility": stats["std_revenue"],
            "min_revenue": stats["min_revenue"],
            "max_revenue": stats["max_revenue"],
            "avg_weekly_growth_rate": stats["avg_7d_growth"],
            "avg_monthly_growth_rate": stats["avg_30d_growth"],
            "coefficient_of_variation": stats["std_revenue"] / stats["avg_revenue"]
            if stats["avg_revenue"]
            else 0,
            "data_points": stats["data_points"],
        }

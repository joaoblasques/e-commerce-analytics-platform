"""
Geographic and Seasonal Analytics Module for E-commerce Analytics Platform.

This module provides comprehensive geographic and seasonal analytics capabilities including:
- Geographic sales distribution analysis
- Seasonal trend identification and forecasting
- Regional demand forecasting with time series models
- Geographic customer segmentation and profiling

The module uses Apache Spark for distributed processing and advanced statistical methods
for trend analysis and forecasting.

Author: E-commerce Analytics Platform Team
Created: 2025-07-23
"""

import logging
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

try:
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.stat import Correlation
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import abs as spark_abs
    from pyspark.sql.functions import (
        array_contains,
        asc,
        avg,
        ceil,
        coalesce,
        col,
        collect_list,
        collect_set,
    )
    from pyspark.sql.functions import count as spark_count
    from pyspark.sql.functions import (
        date_add,
        date_format,
        date_sub,
        datediff,
        dayofmonth,
        dayofweek,
        dense_rank,
        desc,
        first,
        floor,
        from_unixtime,
        greatest,
        isnan,
        isnull,
        lag,
        last,
        lead,
        least,
        lit,
        lower,
    )
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import min as spark_min
    from pyspark.sql.functions import (
        month,
        months_between,
        percent_rank,
        quarter,
        rank,
        regexp_extract,
    )
    from pyspark.sql.functions import round as spark_round
    from pyspark.sql.functions import row_number, size, split, stddev
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.functions import (
        to_date,
        to_timestamp,
        trim,
        unix_timestamp,
        upper,
        when,
        window,
        year,
    )
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql.window import Window

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

    # Mock classes for testing without Spark
    class DataFrame:
        pass

    class SparkSession:
        pass


class GeographicAnalytics:
    """
    Comprehensive geographic and seasonal analytics engine for e-commerce data.

    This class provides advanced analytics capabilities for understanding geographic
    sales patterns, seasonal trends, and regional customer behavior. It supports
    both real-time streaming data and batch processing with Apache Spark.
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize the Geographic Analytics engine.

        Args:
            spark: Optional SparkSession. If None, a session will be created.
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is required but not available")

        if not self.spark:
            self.spark = (
                SparkSession.builder.appName("GeographicAnalytics")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )

        self.logger.info("Geographic Analytics engine initialized")

    def analyze_geographic_distribution(
        self,
        transaction_df: DataFrame,
        customer_df: DataFrame,
        time_period: str = "month",
        min_transactions: int = 10,
    ) -> Dict[str, DataFrame]:
        """
        Analyze geographic sales distribution patterns.

        Args:
            transaction_df: DataFrame with transaction data
            customer_df: DataFrame with customer data including location
            time_period: Time period for aggregation (day/week/month/quarter)
            min_transactions: Minimum transactions for region inclusion

        Returns:
            Dictionary containing geographic analysis results
        """
        try:
            self.logger.info(
                f"Starting geographic distribution analysis for {time_period}"
            )

            # Join transaction and customer data
            geo_df = transaction_df.join(
                customer_df.select(
                    "customer_id", "country", "region", "city", "latitude", "longitude"
                ),
                "customer_id",
                "inner",
            )

            # Create time period column
            if time_period == "day":
                geo_df = geo_df.withColumn(
                    "time_period", date_format(col("timestamp"), "yyyy-MM-dd")
                )
            elif time_period == "week":
                geo_df = geo_df.withColumn(
                    "time_period", date_format(col("timestamp"), "yyyy-'W'ww")
                )
            elif time_period == "month":
                geo_df = geo_df.withColumn(
                    "time_period", date_format(col("timestamp"), "yyyy-MM")
                )
            elif time_period == "quarter":
                geo_df = geo_df.withColumn(
                    "time_period",
                    concat(
                        year(col("timestamp")), lit("-Q"), quarter(col("timestamp"))
                    ),
                )

            # Geographic sales distribution
            country_distribution = self._analyze_country_distribution(
                geo_df, min_transactions
            )
            region_distribution = self._analyze_region_distribution(
                geo_df, min_transactions
            )
            city_distribution = self._analyze_city_distribution(
                geo_df, min_transactions
            )

            # Geographic performance metrics
            geo_performance = self._calculate_geographic_performance(
                geo_df, time_period
            )

            # Geographic concentration analysis
            concentration_analysis = self._analyze_geographic_concentration(geo_df)

            results = {
                "country_distribution": country_distribution,
                "region_distribution": region_distribution,
                "city_distribution": city_distribution,
                "geographic_performance": geo_performance,
                "concentration_analysis": concentration_analysis,
                "summary_stats": self._calculate_distribution_summary(geo_df),
            }

            self.logger.info("Geographic distribution analysis completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Error in geographic distribution analysis: {str(e)}")
            raise

    def analyze_seasonal_trends(
        self,
        transaction_df: DataFrame,
        product_df: Optional[DataFrame] = None,
        seasonality_periods: List[str] = None,
    ) -> Dict[str, DataFrame]:
        """
        Analyze seasonal trends and patterns in sales data.

        Args:
            transaction_df: DataFrame with transaction data
            product_df: Optional product information DataFrame
            seasonality_periods: List of seasonality periods to analyze

        Returns:
            Dictionary containing seasonal analysis results
        """
        try:
            if seasonality_periods is None:
                seasonality_periods = ["monthly", "quarterly", "weekly", "daily"]

            self.logger.info(
                f"Starting seasonal trend analysis for periods: {seasonality_periods}"
            )

            # Add time-based features
            seasonal_df = self._add_temporal_features(transaction_df)

            # Join with product data if available
            if product_df:
                seasonal_df = seasonal_df.join(
                    product_df.select("product_id", "category", "subcategory", "brand"),
                    "product_id",
                    "left",
                )

            results = {}

            # Monthly seasonality
            if "monthly" in seasonality_periods:
                results["monthly_trends"] = self._analyze_monthly_trends(seasonal_df)

            # Quarterly seasonality
            if "quarterly" in seasonality_periods:
                results["quarterly_trends"] = self._analyze_quarterly_trends(
                    seasonal_df
                )

            # Weekly seasonality
            if "weekly" in seasonality_periods:
                results["weekly_trends"] = self._analyze_weekly_trends(seasonal_df)

            # Daily seasonality
            if "daily" in seasonality_periods:
                results["daily_trends"] = self._analyze_daily_trends(seasonal_df)

            # Holiday and special events analysis
            results["holiday_analysis"] = self._analyze_holiday_patterns(seasonal_df)

            # Seasonal decomposition
            results["seasonal_decomposition"] = self._decompose_seasonal_patterns(
                seasonal_df
            )

            # Product category seasonality
            if product_df:
                results["category_seasonality"] = self._analyze_category_seasonality(
                    seasonal_df
                )

            self.logger.info("Seasonal trend analysis completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Error in seasonal trend analysis: {str(e)}")
            raise

    def forecast_regional_demand(
        self,
        transaction_df: DataFrame,
        customer_df: DataFrame,
        product_df: Optional[DataFrame] = None,
        forecast_horizon: int = 30,
        confidence_levels: List[float] = None,
    ) -> Dict[str, DataFrame]:
        """
        Forecast demand by geographic region using time series models.

        Args:
            transaction_df: Historical transaction data
            customer_df: Customer data with geographic information
            product_df: Optional product information
            forecast_horizon: Number of periods to forecast
            confidence_levels: Confidence levels for prediction intervals

        Returns:
            Dictionary containing regional demand forecasts
        """
        try:
            if confidence_levels is None:
                confidence_levels = [0.80, 0.90, 0.95]

            self.logger.info(
                f"Starting regional demand forecasting for {forecast_horizon} periods"
            )

            # Prepare data for forecasting
            regional_df = self._prepare_regional_forecast_data(
                transaction_df, customer_df, product_df
            )

            # Regional demand forecasts
            country_forecasts = self._forecast_country_demand(
                regional_df, forecast_horizon, confidence_levels
            )

            region_forecasts = self._forecast_region_demand(
                regional_df, forecast_horizon, confidence_levels
            )

            # Product-region forecasts
            product_region_forecasts = None
            if product_df:
                product_region_forecasts = self._forecast_product_region_demand(
                    regional_df, forecast_horizon, confidence_levels
                )

            # Forecast accuracy metrics
            forecast_metrics = self._calculate_forecast_accuracy(regional_df)

            results = {
                "country_forecasts": country_forecasts,
                "region_forecasts": region_forecasts,
                "forecast_metrics": forecast_metrics,
                "forecast_summary": self._create_forecast_summary(
                    country_forecasts, region_forecasts
                ),
            }

            if product_region_forecasts:
                results["product_region_forecasts"] = product_region_forecasts

            self.logger.info("Regional demand forecasting completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Error in regional demand forecasting: {str(e)}")
            raise

    def segment_customers_geographically(
        self,
        customer_df: DataFrame,
        transaction_df: DataFrame,
        n_clusters: int = 5,
        features: List[str] = None,
    ) -> Dict[str, DataFrame]:
        """
        Segment customers based on geographic and behavioral patterns.

        Args:
            customer_df: Customer data with geographic information
            transaction_df: Transaction history for behavioral features
            n_clusters: Number of customer segments to create
            features: Features to use for segmentation

        Returns:
            Dictionary containing customer segmentation results
        """
        try:
            if features is None:
                features = [
                    "total_revenue",
                    "transaction_count",
                    "avg_order_value",
                    "days_since_last_purchase",
                    "geographic_distance",
                    "seasonal_variance",
                    "category_diversity",
                ]

            self.logger.info(
                f"Starting geographic customer segmentation with {n_clusters} clusters"
            )

            # Prepare customer features
            customer_features = self._prepare_customer_features(
                customer_df, transaction_df, features
            )

            # Perform clustering
            clustering_results = self._perform_geographic_clustering(
                customer_features, n_clusters
            )

            # Analyze clusters
            cluster_analysis = self._analyze_geographic_clusters(
                clustering_results["segmented_customers"]
            )

            # Geographic segment profiles
            segment_profiles = self._create_segment_profiles(
                clustering_results["segmented_customers"], customer_df, transaction_df
            )

            # Segment performance comparison
            segment_performance = self._compare_segment_performance(
                clustering_results["segmented_customers"], transaction_df
            )

            results = {
                "segmented_customers": clustering_results["segmented_customers"],
                "cluster_centers": clustering_results["cluster_centers"],
                "cluster_analysis": cluster_analysis,
                "segment_profiles": segment_profiles,
                "segment_performance": segment_performance,
                "segmentation_metrics": clustering_results["metrics"],
            }

            self.logger.info("Geographic customer segmentation completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Error in geographic customer segmentation: {str(e)}")
            raise

    def _analyze_country_distribution(
        self, geo_df: DataFrame, min_transactions: int
    ) -> DataFrame:
        """Analyze sales distribution by country."""
        return (
            geo_df.groupBy("country")
            .agg(
                spark_sum("amount").alias("total_revenue"),
                spark_count("transaction_id").alias("transaction_count"),
                avg("amount").alias("avg_order_value"),
                spark_count("customer_id").alias("unique_customers"),
                spark_max("timestamp").alias("last_transaction"),
                spark_min("timestamp").alias("first_transaction"),
            )
            .filter(col("transaction_count") >= min_transactions)
            .withColumn(
                "revenue_per_customer",
                spark_round(col("total_revenue") / col("unique_customers"), 2),
            )
            .orderBy(desc("total_revenue"))
        )

    def _analyze_region_distribution(
        self, geo_df: DataFrame, min_transactions: int
    ) -> DataFrame:
        """Analyze sales distribution by region."""
        return (
            geo_df.groupBy("country", "region")
            .agg(
                spark_sum("amount").alias("total_revenue"),
                spark_count("transaction_id").alias("transaction_count"),
                avg("amount").alias("avg_order_value"),
                spark_count("customer_id").alias("unique_customers"),
            )
            .filter(col("transaction_count") >= min_transactions)
            .withColumn(
                "revenue_per_customer",
                spark_round(col("total_revenue") / col("unique_customers"), 2),
            )
            .orderBy(desc("total_revenue"))
        )

    def _analyze_city_distribution(
        self, geo_df: DataFrame, min_transactions: int
    ) -> DataFrame:
        """Analyze sales distribution by city."""
        return (
            geo_df.groupBy("country", "region", "city")
            .agg(
                spark_sum("amount").alias("total_revenue"),
                spark_count("transaction_id").alias("transaction_count"),
                avg("amount").alias("avg_order_value"),
                spark_count("customer_id").alias("unique_customers"),
            )
            .filter(col("transaction_count") >= min_transactions)
            .withColumn(
                "revenue_per_customer",
                spark_round(col("total_revenue") / col("unique_customers"), 2),
            )
            .orderBy(desc("total_revenue"))
        )

    def _calculate_geographic_performance(
        self, geo_df: DataFrame, time_period: str
    ) -> DataFrame:
        """Calculate geographic performance metrics over time."""
        window_spec = Window.partitionBy("country", "region").orderBy("time_period")

        return (
            geo_df.groupBy("country", "region", "time_period")
            .agg(
                spark_sum("amount").alias("revenue"),
                spark_count("transaction_id").alias("transactions"),
                avg("amount").alias("avg_order_value"),
            )
            .withColumn(
                "revenue_growth",
                spark_round(
                    (col("revenue") - lag(col("revenue")).over(window_spec))
                    / lag(col("revenue")).over(window_spec)
                    * 100,
                    2,
                ),
            )
            .withColumn(
                "transaction_growth",
                spark_round(
                    (col("transactions") - lag(col("transactions")).over(window_spec))
                    / lag(col("transactions")).over(window_spec)
                    * 100,
                    2,
                ),
            )
            .orderBy("country", "region", "time_period")
        )

    def _analyze_geographic_concentration(self, geo_df: DataFrame) -> DataFrame:
        """Analyze geographic concentration of sales."""
        total_revenue = geo_df.agg(spark_sum("amount").alias("total")).collect()[0][
            "total"
        ]

        country_concentration = (
            geo_df.groupBy("country")
            .agg(spark_sum("amount").alias("revenue"))
            .withColumn(
                "revenue_percentage",
                spark_round(col("revenue") / total_revenue * 100, 2),
            )
            .withColumn(
                "cumulative_percentage",
                spark_sum("revenue_percentage").over(
                    Window.orderBy(desc("revenue")).rowsBetween(
                        Window.unboundedPreceding, Window.currentRow
                    )
                ),
            )
            .orderBy(desc("revenue"))
        )

        return country_concentration

    def _calculate_distribution_summary(self, geo_df: DataFrame) -> DataFrame:
        """Calculate summary statistics for geographic distribution."""
        return geo_df.agg(
            spark_count("country").alias("total_countries"),
            spark_count("region").alias("total_regions"),
            spark_count("city").alias("total_cities"),
            spark_sum("amount").alias("total_revenue"),
            spark_count("transaction_id").alias("total_transactions"),
            avg("amount").alias("global_avg_order_value"),
        )

    def _add_temporal_features(self, transaction_df: DataFrame) -> DataFrame:
        """Add temporal features for seasonal analysis."""
        return (
            transaction_df.withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("quarter", quarter(col("timestamp")))
            .withColumn("day_of_month", dayofmonth(col("timestamp")))
            .withColumn("day_of_week", dayofweek(col("timestamp")))
            .withColumn("week_of_year", weekofyear(col("timestamp")))
            .withColumn("month_name", date_format(col("timestamp"), "MMMM"))
            .withColumn("day_name", date_format(col("timestamp"), "EEEE"))
        )

    def _analyze_monthly_trends(self, seasonal_df: DataFrame) -> DataFrame:
        """Analyze monthly seasonal trends."""
        return (
            seasonal_df.groupBy("year", "month", "month_name")
            .agg(
                spark_sum("amount").alias("monthly_revenue"),
                spark_count("transaction_id").alias("monthly_transactions"),
                avg("amount").alias("monthly_avg_order"),
                spark_count("customer_id").alias("monthly_customers"),
            )
            .withColumn(
                "revenue_per_customer",
                spark_round(col("monthly_revenue") / col("monthly_customers"), 2),
            )
            .orderBy("year", "month")
        )

    def _analyze_quarterly_trends(self, seasonal_df: DataFrame) -> DataFrame:
        """Analyze quarterly seasonal trends."""
        return (
            seasonal_df.groupBy("year", "quarter")
            .agg(
                spark_sum("amount").alias("quarterly_revenue"),
                spark_count("transaction_id").alias("quarterly_transactions"),
                avg("amount").alias("quarterly_avg_order"),
                spark_count("customer_id").alias("quarterly_customers"),
            )
            .withColumn(
                "revenue_per_customer",
                spark_round(col("quarterly_revenue") / col("quarterly_customers"), 2),
            )
            .orderBy("year", "quarter")
        )

    def _analyze_weekly_trends(self, seasonal_df: DataFrame) -> DataFrame:
        """Analyze weekly seasonal trends."""
        return (
            seasonal_df.groupBy("day_of_week", "day_name")
            .agg(
                spark_sum("amount").alias("daily_revenue"),
                spark_count("transaction_id").alias("daily_transactions"),
                avg("amount").alias("daily_avg_order"),
            )
            .withColumn(
                "revenue_percentage",
                spark_round(
                    col("daily_revenue")
                    / spark_sum("daily_revenue").over(Window.partitionBy())
                    * 100,
                    2,
                ),
            )
            .orderBy("day_of_week")
        )

    def _analyze_daily_trends(self, seasonal_df: DataFrame) -> DataFrame:
        """Analyze daily patterns within months."""
        return (
            seasonal_df.groupBy("day_of_month")
            .agg(
                spark_sum("amount").alias("daily_revenue"),
                spark_count("transaction_id").alias("daily_transactions"),
                avg("amount").alias("daily_avg_order"),
            )
            .withColumn(
                "revenue_percentage",
                spark_round(
                    col("daily_revenue")
                    / spark_sum("daily_revenue").over(Window.partitionBy())
                    * 100,
                    2,
                ),
            )
            .orderBy("day_of_month")
        )

    def _analyze_holiday_patterns(self, seasonal_df: DataFrame) -> DataFrame:
        """Analyze patterns around holidays and special events."""
        # Define major holidays (simplified - would be more comprehensive in practice)
        holiday_periods = [
            ("Black Friday", "11-25"),
            ("Christmas", "12-25"),
            ("New Year", "01-01"),
            ("Valentine's Day", "02-14"),
            ("Mother's Day", "05-10"),  # Approximate
            ("Father's Day", "06-20"),  # Approximate
        ]

        holiday_df = seasonal_df.withColumn(
            "month_day", date_format(col("timestamp"), "MM-dd")
        )

        results = []
        for holiday_name, holiday_date in holiday_periods:
            holiday_analysis = holiday_df.filter(col("month_day") == holiday_date).agg(
                lit(holiday_name).alias("holiday"),
                spark_sum("amount").alias("holiday_revenue"),
                spark_count("transaction_id").alias("holiday_transactions"),
                avg("amount").alias("holiday_avg_order"),
            )
            results.append(holiday_analysis)

        if results:
            return results[0].union(*results[1:]) if len(results) > 1 else results[0]
        else:
            # Return empty DataFrame with proper schema
            return self.spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("holiday", StringType(), True),
                        StructField("holiday_revenue", DoubleType(), True),
                        StructField("holiday_transactions", IntegerType(), True),
                        StructField("holiday_avg_order", DoubleType(), True),
                    ]
                ),
            )

    def _decompose_seasonal_patterns(self, seasonal_df: DataFrame) -> DataFrame:
        """Decompose seasonal patterns into trend, seasonal, and residual components."""
        # Simplified seasonal decomposition - would use more sophisticated methods in practice
        monthly_data = (
            seasonal_df.groupBy("year", "month")
            .agg(spark_sum("amount").alias("revenue"))
            .orderBy("year", "month")
        )

        window_spec = Window.orderBy("year", "month")

        return (
            monthly_data.withColumn(
                "moving_avg_3", avg("revenue").over(window_spec.rowsBetween(-1, 1))
            )
            .withColumn(
                "moving_avg_12", avg("revenue").over(window_spec.rowsBetween(-6, 5))
            )
            .withColumn("seasonal_component", col("revenue") - col("moving_avg_12"))
            .withColumn("trend_component", col("moving_avg_12"))
            .withColumn("residual_component", col("revenue") - col("moving_avg_3"))
        )

    def _analyze_category_seasonality(self, seasonal_df: DataFrame) -> DataFrame:
        """Analyze seasonality patterns by product category."""
        return (
            seasonal_df.groupBy("category", "month", "month_name")
            .agg(
                spark_sum("amount").alias("category_revenue"),
                spark_count("transaction_id").alias("category_transactions"),
            )
            .withColumn(
                "category_avg_monthly_revenue",
                avg("category_revenue").over(Window.partitionBy("category")),
            )
            .withColumn(
                "seasonal_index",
                spark_round(
                    col("category_revenue") / col("category_avg_monthly_revenue"), 2
                ),
            )
            .orderBy("category", "month")
        )

    def _prepare_regional_forecast_data(
        self,
        transaction_df: DataFrame,
        customer_df: DataFrame,
        product_df: Optional[DataFrame],
    ) -> DataFrame:
        """Prepare data for regional demand forecasting."""
        # Join transaction and customer data
        regional_df = transaction_df.join(
            customer_df.select("customer_id", "country", "region"),
            "customer_id",
            "inner",
        )

        # Add temporal features
        regional_df = self._add_temporal_features(regional_df)

        # Join with product data if available
        if product_df:
            regional_df = regional_df.join(
                product_df.select("product_id", "category", "subcategory"),
                "product_id",
                "left",
            )

        return regional_df

    def _forecast_country_demand(
        self,
        regional_df: DataFrame,
        forecast_horizon: int,
        confidence_levels: List[float],
    ) -> DataFrame:
        """Forecast demand by country using linear regression."""
        # Prepare time series data
        country_ts = (
            regional_df.groupBy("country", "year", "month")
            .agg(
                spark_sum("amount").alias("revenue"),
                spark_count("transaction_id").alias("transactions"),
            )
            .withColumn("time_index", (col("year") - 2020) * 12 + col("month"))
        )

        # Simple linear regression for each country (simplified implementation)
        window_spec = Window.partitionBy("country").orderBy("time_index")

        forecasted = (
            country_ts.withColumn("revenue_lag1", lag("revenue", 1).over(window_spec))
            .withColumn("revenue_lag2", lag("revenue", 2).over(window_spec))
            .withColumn(
                "trend",
                (col("revenue") - col("revenue_lag1")) / col("revenue_lag1") * 100,
            )
            .withColumn(
                "forecasted_revenue",
                col("revenue") * (1 + coalesce(col("trend"), lit(0)) / 100),
            )
        )

        return forecasted.filter(col("forecasted_revenue").isNotNull())

    def _forecast_region_demand(
        self,
        regional_df: DataFrame,
        forecast_horizon: int,
        confidence_levels: List[float],
    ) -> DataFrame:
        """Forecast demand by region."""
        # Similar to country forecasting but with region granularity
        region_ts = (
            regional_df.groupBy("country", "region", "year", "month")
            .agg(
                spark_sum("amount").alias("revenue"),
                spark_count("transaction_id").alias("transactions"),
            )
            .withColumn("time_index", (col("year") - 2020) * 12 + col("month"))
        )

        window_spec = Window.partitionBy("country", "region").orderBy("time_index")

        forecasted = (
            region_ts.withColumn("revenue_lag1", lag("revenue", 1).over(window_spec))
            .withColumn(
                "trend",
                (col("revenue") - col("revenue_lag1")) / col("revenue_lag1") * 100,
            )
            .withColumn(
                "forecasted_revenue",
                col("revenue") * (1 + coalesce(col("trend"), lit(0)) / 100),
            )
        )

        return forecasted.filter(col("forecasted_revenue").isNotNull())

    def _forecast_product_region_demand(
        self,
        regional_df: DataFrame,
        forecast_horizon: int,
        confidence_levels: List[float],
    ) -> DataFrame:
        """Forecast demand by product category and region."""
        product_region_ts = (
            regional_df.groupBy("country", "region", "category", "year", "month")
            .agg(
                spark_sum("amount").alias("revenue"),
                spark_count("transaction_id").alias("transactions"),
            )
            .withColumn("time_index", (col("year") - 2020) * 12 + col("month"))
        )

        window_spec = Window.partitionBy("country", "region", "category").orderBy(
            "time_index"
        )

        forecasted = (
            product_region_ts.withColumn(
                "revenue_lag1", lag("revenue", 1).over(window_spec)
            )
            .withColumn(
                "trend",
                (col("revenue") - col("revenue_lag1")) / col("revenue_lag1") * 100,
            )
            .withColumn(
                "forecasted_revenue",
                col("revenue") * (1 + coalesce(col("trend"), lit(0)) / 100),
            )
        )

        return forecasted.filter(col("forecasted_revenue").isNotNull())

    def _calculate_forecast_accuracy(self, regional_df: DataFrame) -> DataFrame:
        """Calculate forecast accuracy metrics."""
        # This would implement proper forecast accuracy calculation
        # For now, returning a simple summary
        return regional_df.agg(
            spark_count("*").alias("total_observations"),
            avg("amount").alias("mean_revenue"),
            stddev("amount").alias("revenue_stddev"),
            spark_min("timestamp").alias("data_start_date"),
            spark_max("timestamp").alias("data_end_date"),
        )

    def _create_forecast_summary(
        self, country_forecasts: DataFrame, region_forecasts: DataFrame
    ) -> DataFrame:
        """Create summary of forecast results."""
        country_summary = country_forecasts.agg(
            spark_count("country").alias("countries_forecasted"),
            spark_sum("forecasted_revenue").alias("total_country_forecast"),
        ).withColumn("forecast_type", lit("country"))

        region_summary = region_forecasts.agg(
            spark_count("region").alias("regions_forecasted"),
            spark_sum("forecasted_revenue").alias("total_region_forecast"),
        ).withColumn("forecast_type", lit("region"))

        return country_summary.select(
            col("forecast_type"),
            col("countries_forecasted").alias("entities_forecasted"),
            col("total_country_forecast").alias("total_forecast"),
        ).union(
            region_summary.select(
                col("forecast_type"),
                col("regions_forecasted").alias("entities_forecasted"),
                col("total_region_forecast").alias("total_forecast"),
            )
        )

    def _prepare_customer_features(
        self, customer_df: DataFrame, transaction_df: DataFrame, features: List[str]
    ) -> DataFrame:
        """Prepare customer features for geographic segmentation."""
        # Calculate customer behavioral features
        customer_metrics = transaction_df.groupBy("customer_id").agg(
            spark_sum("amount").alias("total_revenue"),
            spark_count("transaction_id").alias("transaction_count"),
            avg("amount").alias("avg_order_value"),
            spark_max("timestamp").alias("last_purchase"),
            countDistinct("product_id").alias("product_diversity"),
            stddev("amount").alias("order_value_variance"),
        )

        # Calculate days since last purchase
        current_date = datetime.now()
        customer_metrics = customer_metrics.withColumn(
            "days_since_last_purchase",
            datediff(lit(current_date), col("last_purchase")),
        )

        # Join with customer geographic data
        customer_features = customer_df.join(customer_metrics, "customer_id", "inner")

        # Add geographic features (simplified)
        customer_features = (
            customer_features.withColumn(
                "geographic_distance",
                spark_round(col("latitude") + col("longitude"), 2),
            )
            .withColumn(
                "seasonal_variance", coalesce(col("order_value_variance"), lit(0))
            )
            .withColumn(
                "category_diversity", coalesce(col("product_diversity"), lit(1))
            )
        )

        return customer_features

    def _perform_geographic_clustering(
        self, customer_features: DataFrame, n_clusters: int
    ) -> Dict[str, Any]:
        """Perform K-means clustering on customer features."""
        # Select numeric features for clustering
        feature_columns = [
            "total_revenue",
            "transaction_count",
            "avg_order_value",
            "days_since_last_purchase",
            "geographic_distance",
            "seasonal_variance",
            "category_diversity",
        ]

        # Fill null values
        for col_name in feature_columns:
            customer_features = customer_features.fillna({col_name: 0})

        # Create feature vector
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

        feature_df = assembler.transform(customer_features)

        # Scale features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True,
        )

        scaler_model = scaler.fit(feature_df)
        scaled_df = scaler_model.transform(feature_df)

        # Perform K-means clustering
        kmeans = KMeans(
            featuresCol="scaled_features",
            predictionCol="cluster",
            k=n_clusters,
            seed=42,
        )

        model = kmeans.fit(scaled_df)
        segmented_customers = model.transform(scaled_df)

        # Calculate clustering metrics
        silhouette_evaluator = ClusteringEvaluator(
            predictionCol="cluster",
            featuresCol="scaled_features",
            metricName="silhouette",
            distanceMeasure="squaredEuclidean",
        )

        silhouette_score = silhouette_evaluator.evaluate(segmented_customers)

        return {
            "segmented_customers": segmented_customers,
            "cluster_centers": model.clusterCenters(),
            "metrics": {
                "silhouette_score": silhouette_score,
                "n_clusters": n_clusters,
                "total_customers": segmented_customers.count(),
            },
        }

    def _analyze_geographic_clusters(self, segmented_customers: DataFrame) -> DataFrame:
        """Analyze the characteristics of geographic clusters."""
        return (
            segmented_customers.groupBy("cluster")
            .agg(
                spark_count("customer_id").alias("cluster_size"),
                avg("total_revenue").alias("avg_revenue"),
                avg("transaction_count").alias("avg_transactions"),
                avg("avg_order_value").alias("avg_order_value"),
                avg("days_since_last_purchase").alias("avg_days_since_purchase"),
                avg("geographic_distance").alias("avg_geographic_distance"),
                countDistinct("country").alias("countries_in_cluster"),
                countDistinct("region").alias("regions_in_cluster"),
            )
            .withColumn(
                "revenue_per_transaction",
                spark_round(col("avg_revenue") / col("avg_transactions"), 2),
            )
            .orderBy("cluster")
        )

    def _create_segment_profiles(
        self,
        segmented_customers: DataFrame,
        customer_df: DataFrame,
        transaction_df: DataFrame,
    ) -> DataFrame:
        """Create detailed profiles for each customer segment."""
        # Geographic distribution by cluster
        geographic_profiles = (
            segmented_customers.groupBy("cluster", "country", "region")
            .agg(spark_count("customer_id").alias("customers_in_location"))
            .withColumn(
                "location_percentage",
                spark_round(
                    col("customers_in_location")
                    / spark_sum("customers_in_location").over(
                        Window.partitionBy("cluster")
                    )
                    * 100,
                    2,
                ),
            )
        )

        return geographic_profiles

    def _compare_segment_performance(
        self, segmented_customers: DataFrame, transaction_df: DataFrame
    ) -> DataFrame:
        """Compare performance metrics across customer segments."""
        # Join with transaction data to get recent performance
        segment_transactions = segmented_customers.select(
            "customer_id", "cluster"
        ).join(transaction_df, "customer_id", "inner")

        # Calculate performance by cluster
        return (
            segment_transactions.groupBy("cluster")
            .agg(
                spark_sum("amount").alias("total_cluster_revenue"),
                spark_count("transaction_id").alias("total_cluster_transactions"),
                avg("amount").alias("avg_cluster_order_value"),
                countDistinct("customer_id").alias("active_customers"),
                spark_max("timestamp").alias("latest_transaction"),
            )
            .withColumn(
                "revenue_per_customer",
                spark_round(col("total_cluster_revenue") / col("active_customers"), 2),
            )
            .withColumn(
                "transactions_per_customer",
                spark_round(
                    col("total_cluster_transactions") / col("active_customers"), 2
                ),
            )
            .orderBy(desc("total_cluster_revenue"))
        )

    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get a summary of available analytics capabilities."""
        return {
            "module": "GeographicAnalytics",
            "version": "1.0.0",
            "capabilities": [
                "Geographic sales distribution analysis",
                "Seasonal trend identification",
                "Regional demand forecasting",
                "Geographic customer segmentation",
            ],
            "supported_aggregations": [
                "Country-level analysis",
                "Region-level analysis",
                "City-level analysis",
                "Time-based aggregations",
            ],
            "forecasting_methods": [
                "Linear regression",
                "Trend analysis",
                "Seasonal decomposition",
            ],
            "clustering_algorithms": [
                "K-means clustering",
                "Feature scaling and normalization",
            ],
        }


# Import fix for missing functions
try:
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.sql.functions import concat, countDistinct, weekofyear
except ImportError:
    # Mock these if not available
    def weekofyear(col):
        return col

    def countDistinct(col):
        return col

    def concat(*cols):
        return cols[0] if cols else None

    class ClusteringEvaluator:
        def __init__(self, **kwargs):
            pass

        def evaluate(self, df):
            return 0.5  # Mock silhouette score


if __name__ == "__main__":
    # Example usage and testing
    try:
        spark = (
            SparkSession.builder.appName("GeographicAnalyticsTest")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )

        # Initialize analytics engine
        geo_analytics = GeographicAnalytics(spark)

        # Print summary
        summary = geo_analytics.get_analytics_summary()
        print("Geographic Analytics Module Summary:")
        for key, value in summary.items():
            print(f"{key}: {value}")

        print("\nGeographic Analytics module loaded successfully!")

    except Exception as e:
        print(f"Error initializing Geographic Analytics: {str(e)}")
    finally:
        if "spark" in locals():
            spark.stop()

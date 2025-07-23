"""
Product performance analytics engine for comprehensive product intelligence.

This module provides product sales metrics, recommendation engine, market basket analysis,
and product lifecycle analytics for e-commerce platforms.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


class ProductAnalytics:
    """
    Comprehensive product performance analytics engine.

    Features:
    - Product sales and inventory tracking
    - Recommendation engine using collaborative filtering
    - Market basket analysis with association rules
    - Product lifecycle analysis and performance metrics
    - Product category and brand performance
    - Seasonal product performance analysis
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the product analytics engine."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Product metrics cache for performance
        self.metrics_cache = {}

    def _get_default_config(self) -> Dict:
        """Get default configuration for product analytics."""
        return {
            "product_dimensions": [
                "product_category",
                "brand",
                "price_range",
                "supplier",
                "geographic_region",
                "sales_channel",
            ],
            "recommendation": {
                "als_rank": 10,
                "als_max_iter": 10,
                "als_reg_param": 0.01,
                "min_interactions": 5,
                "top_n_recommendations": 10,
            },
            "market_basket": {
                "min_support": 0.01,
                "min_confidence": 0.3,
                "max_itemsets": 1000,
                "min_transaction_items": 2,
            },
            "lifecycle_analysis": {
                "stages": ["introduction", "growth", "maturity", "decline"],
                "growth_threshold": 0.15,  # 15% growth
                "decline_threshold": -0.10,  # -10% decline
                "maturity_variance_threshold": 0.05,  # 5% variance
            },
            "performance_metrics": {
                "velocity_windows": [7, 30, 90],  # Days
                "inventory_turnover_threshold": 6,  # Times per year
                "stockout_threshold": 0.02,  # 2% stockout rate
                "slow_moving_threshold": 30,  # Days without sales
            },
        }

    def analyze_product_performance(
        self,
        transactions_df: DataFrame,
        inventory_df: Optional[DataFrame] = None,
        time_period: str = "day",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, DataFrame]:
        """
        Analyze comprehensive product performance metrics.

        Args:
            transactions_df: Transaction data with product information
            inventory_df: Optional inventory data for stock analysis
            time_period: Aggregation period ('day', 'week', 'month')
            start_date: Analysis start date
            end_date: Analysis end date

        Returns:
            Dictionary of DataFrames with product performance analysis
        """
        try:
            # Filter by date range if specified
            if start_date or end_date:
                transactions_df = self._filter_by_date_range(
                    transactions_df, start_date, end_date
                )

            # Add time dimension column
            transactions_df = self._add_time_dimension(transactions_df, time_period)

            # Calculate base product metrics
            product_metrics_df = self._calculate_product_metrics(transactions_df)

            # Calculate all analysis components
            overall_performance = self._analyze_overall_product_performance(
                product_metrics_df, time_period
            )
            sales_metrics = self._analyze_product_sales_metrics(product_metrics_df)
            velocity_analysis = self._analyze_product_velocity(transactions_df)
            profitability = self._analyze_product_profitability(product_metrics_df)
            category_performance = self._analyze_category_performance(
                product_metrics_df
            )
            brand_performance = self._analyze_brand_performance(product_metrics_df)
            seasonal_performance = self._analyze_seasonal_performance(transactions_df)

            results = {
                "overall_performance": overall_performance,
                "sales_metrics": sales_metrics,
                "velocity_analysis": velocity_analysis,
                "profitability": profitability,
                "category_performance": category_performance,
                "brand_performance": brand_performance,
                "seasonal_performance": seasonal_performance,
            }

            # Add inventory analysis if data provided
            if inventory_df is not None:
                results["inventory_analysis"] = self._analyze_inventory_performance(
                    product_metrics_df, inventory_df
                )

            return results

        except Exception as e:
            self.logger.error(f"Failed to analyze product performance: {e}")
            raise

    def build_recommendation_engine(
        self,
        interactions_df: DataFrame,
        product_features_df: Optional[DataFrame] = None,
        user_features_df: Optional[DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        Build product recommendation engine using collaborative filtering.

        Args:
            interactions_df: User-product interaction data
            product_features_df: Optional product features for content-based filtering
            user_features_df: Optional user features for hybrid recommendations

        Returns:
            Dictionary with trained model and recommendation functions
        """
        try:
            # Prepare interaction data
            prepared_df = self._prepare_interaction_data(interactions_df)

            # Filter users and products with minimum interactions
            filtered_df = self._filter_interactions_by_frequency(prepared_df)

            # Train ALS model for collaborative filtering
            als_model, model_metrics = self._train_als_model(filtered_df)

            # Generate user recommendations
            user_recommendations = self._generate_user_recommendations(
                als_model, filtered_df
            )

            # Generate item similarities
            item_similarities = self._calculate_item_similarities(
                als_model, filtered_df
            )

            # Create recommendation functions
            recommendation_functions = self._create_recommendation_functions(
                als_model, filtered_df
            )

            # Add content-based recommendations if features provided
            content_recommendations = None
            if product_features_df is not None:
                content_recommendations = self._build_content_based_recommendations(
                    product_features_df, interactions_df
                )

            return {
                "model": als_model,
                "model_metrics": model_metrics,
                "user_recommendations": user_recommendations,
                "item_similarities": item_similarities,
                "recommendation_functions": recommendation_functions,
                "content_recommendations": content_recommendations,
                "training_data_stats": {
                    "total_interactions": filtered_df.count(),
                    "unique_users": filtered_df.select("user_id").distinct().count(),
                    "unique_products": filtered_df.select("product_id")
                    .distinct()
                    .count(),
                },
                "model_created_at": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to build recommendation engine: {e}")
            raise

    def perform_market_basket_analysis(
        self,
        transactions_df: DataFrame,
        min_support: Optional[float] = None,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, DataFrame]:
        """
        Perform market basket analysis to find product associations.

        Args:
            transactions_df: Transaction data with products
            min_support: Minimum support threshold for frequent itemsets
            min_confidence: Minimum confidence threshold for association rules

        Returns:
            Dictionary with frequent itemsets and association rules
        """
        try:
            min_support = min_support or self.config["market_basket"]["min_support"]
            min_confidence = (
                min_confidence or self.config["market_basket"]["min_confidence"]
            )

            # Prepare basket data
            basket_df = self._prepare_basket_data(transactions_df)

            # Filter baskets with minimum items
            filtered_baskets = self._filter_baskets_by_size(basket_df)

            # Train FP-Growth model
            fpgrowth = FPGrowth(
                itemsCol="items",
                minSupport=min_support,
                minConfidence=min_confidence,
            )

            model = fpgrowth.fit(filtered_baskets)

            # Get frequent itemsets
            frequent_itemsets = model.freqItemsets

            # Get association rules
            association_rules = model.associationRules

            # Calculate additional metrics
            itemset_metrics = self._calculate_itemset_metrics(frequent_itemsets)
            rule_metrics = self._calculate_rule_metrics(association_rules)

            # Analyze product affinity
            product_affinity = self._analyze_product_affinity(association_rules)

            # Create cross-sell recommendations
            cross_sell_recommendations = self._create_cross_sell_recommendations(
                association_rules
            )

            return {
                "frequent_itemsets": frequent_itemsets,
                "association_rules": association_rules,
                "itemset_metrics": itemset_metrics,
                "rule_metrics": rule_metrics,
                "product_affinity": product_affinity,
                "cross_sell_recommendations": cross_sell_recommendations,
                "analysis_summary": {
                    "total_baskets": filtered_baskets.count(),
                    "frequent_itemsets_count": frequent_itemsets.count(),
                    "association_rules_count": association_rules.count(),
                    "min_support_used": min_support,
                    "min_confidence_used": min_confidence,
                },
                "analysis_date": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to perform market basket analysis: {e}")
            raise

    def analyze_product_lifecycle(
        self,
        transactions_df: DataFrame,
        product_launch_dates_df: Optional[DataFrame] = None,
    ) -> Dict[str, DataFrame]:
        """
        Analyze product lifecycle stages and performance.

        Args:
            transactions_df: Transaction data with product sales
            product_launch_dates_df: Optional product launch date information

        Returns:
            Dictionary with product lifecycle analysis
        """
        try:
            # Prepare time series data for each product
            product_time_series = self._prepare_product_time_series(transactions_df)

            # Calculate growth rates and trends
            product_trends = self._calculate_product_trends(product_time_series)

            # Classify lifecycle stages
            lifecycle_stages = self._classify_lifecycle_stages(product_trends)

            # Analyze stage transitions
            stage_transitions = self._analyze_stage_transitions(lifecycle_stages)

            # Calculate lifecycle metrics
            lifecycle_metrics = self._calculate_lifecycle_metrics(
                product_trends, lifecycle_stages
            )

            # Identify products at risk
            at_risk_products = self._identify_at_risk_products(lifecycle_stages)

            # Growth opportunity analysis
            growth_opportunities = self._identify_growth_opportunities(lifecycle_stages)

            # Add launch date analysis if provided
            launch_analysis = None
            if product_launch_dates_df is not None:
                launch_analysis = self._analyze_product_launch_performance(
                    lifecycle_stages, product_launch_dates_df
                )

            return {
                "lifecycle_stages": lifecycle_stages,
                "stage_transitions": stage_transitions,
                "lifecycle_metrics": lifecycle_metrics,
                "at_risk_products": at_risk_products,
                "growth_opportunities": growth_opportunities,
                "launch_analysis": launch_analysis,
                "stage_distribution": self._calculate_stage_distribution(
                    lifecycle_stages
                ),
                "analysis_date": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to analyze product lifecycle: {e}")
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
        if time_period == "day":
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

    def _calculate_product_metrics(self, transactions_df: DataFrame) -> DataFrame:
        """Calculate base product metrics from transactions."""
        return (
            transactions_df.withColumn("revenue", F.col("price") * F.col("quantity"))
            .withColumn("transaction_count", F.lit(1))
            .withColumn("units_sold", F.col("quantity"))
        )

    def _analyze_overall_product_performance(
        self, product_metrics_df: DataFrame, time_period: str
    ) -> DataFrame:
        """Analyze overall product performance metrics."""
        return (
            product_metrics_df.groupBy("time_dimension")
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.sum("units_sold").alias("total_units_sold"),
                F.count("transaction_id").alias("total_transactions"),
                F.countDistinct("product_id").alias("unique_products_sold"),
                F.countDistinct("user_id").alias("unique_customers"),
                F.avg("price").alias("avg_selling_price"),
                F.stddev("price").alias("price_variance"),
            )
            .withColumn(
                "avg_revenue_per_product",
                F.col("total_revenue") / F.col("unique_products_sold"),
            )
            .withColumn(
                "avg_units_per_transaction",
                F.col("total_units_sold") / F.col("total_transactions"),
            )
        )

    def _analyze_product_sales_metrics(
        self, product_metrics_df: DataFrame
    ) -> DataFrame:
        """Analyze detailed product sales metrics."""
        return (
            product_metrics_df.groupBy("product_id")
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.sum("units_sold").alias("total_units_sold"),
                F.count("transaction_id").alias("transaction_count"),
                F.countDistinct("user_id").alias("unique_customers"),
                F.avg("price").alias("avg_selling_price"),
                F.min("price").alias("min_price"),
                F.max("price").alias("max_price"),
                F.stddev("price").alias("price_volatility"),
                F.first("product_category").alias("product_category"),
                F.first("brand").alias("brand"),
            )
            .withColumn(
                "avg_revenue_per_customer",
                F.col("total_revenue") / F.col("unique_customers"),
            )
            .withColumn(
                "avg_units_per_transaction",
                F.col("total_units_sold") / F.col("transaction_count"),
            )
            .withColumn(
                "customer_penetration_rate",
                F.col("unique_customers")
                / F.lit(1000),  # Assuming 1000 total customers
            )
        )

    def _analyze_product_velocity(self, transactions_df: DataFrame) -> DataFrame:
        """Analyze product sales velocity over different time windows."""
        # Add date column
        velocity_df = transactions_df.withColumn("date", F.to_date("timestamp"))

        window_spec = Window.partitionBy("product_id").orderBy("date")

        # Calculate velocity for different windows
        for window_days in self.config["performance_metrics"]["velocity_windows"]:
            rolling_window = window_spec.rowsBetween(-window_days + 1, 0)

            velocity_df = velocity_df.withColumn(
                f"units_sold_{window_days}d",
                F.sum("quantity").over(rolling_window),
            ).withColumn(
                f"revenue_{window_days}d",
                F.sum("price" * F.col("quantity")).over(rolling_window),
            )

        # Calculate velocity metrics
        return velocity_df.groupBy("product_id").agg(
            *[
                F.avg(f"units_sold_{window_days}d").alias(
                    f"avg_units_velocity_{window_days}d"
                )
                for window_days in self.config["performance_metrics"][
                    "velocity_windows"
                ]
            ],
            *[
                F.avg(f"revenue_{window_days}d").alias(
                    f"avg_revenue_velocity_{window_days}d"
                )
                for window_days in self.config["performance_metrics"][
                    "velocity_windows"
                ]
            ],
            F.first("product_category").alias("product_category"),
            F.first("brand").alias("brand"),
        )

    def _analyze_product_profitability(
        self, product_metrics_df: DataFrame
    ) -> DataFrame:
        """Analyze product profitability metrics."""
        # Assume cost is 60% of price (simplified)
        return (
            product_metrics_df.withColumn("cost", F.col("price") * 0.6)
            .withColumn("profit", F.col("revenue") - F.col("cost") * F.col("quantity"))
            .groupBy("product_id")
            .agg(
                F.sum("profit").alias("total_profit"),
                F.sum("revenue").alias("total_revenue"),
                F.sum("cost" * F.col("quantity")).alias("total_cost"),
                F.avg("profit" / F.col("revenue") * 100).alias("avg_profit_margin_pct"),
                F.first("product_category").alias("product_category"),
                F.first("brand").alias("brand"),
            )
            .withColumn(
                "overall_profit_margin_pct",
                (F.col("total_profit") / F.col("total_revenue") * 100).cast(
                    DoubleType()
                ),
            )
            .withColumn("roi", F.col("total_profit") / F.col("total_cost"))
        )

    def _analyze_category_performance(self, product_metrics_df: DataFrame) -> DataFrame:
        """Analyze performance by product category."""
        return (
            product_metrics_df.groupBy("product_category", "time_dimension")
            .agg(
                F.sum("revenue").alias("category_revenue"),
                F.sum("units_sold").alias("category_units_sold"),
                F.countDistinct("product_id").alias("products_in_category"),
                F.countDistinct("user_id").alias("unique_customers"),
                F.avg("price").alias("avg_category_price"),
            )
            .withColumn(
                "avg_revenue_per_product",
                F.col("category_revenue") / F.col("products_in_category"),
            )
        )

    def _analyze_brand_performance(self, product_metrics_df: DataFrame) -> DataFrame:
        """Analyze performance by brand."""
        return (
            product_metrics_df.groupBy("brand", "time_dimension")
            .agg(
                F.sum("revenue").alias("brand_revenue"),
                F.sum("units_sold").alias("brand_units_sold"),
                F.countDistinct("product_id").alias("products_in_brand"),
                F.countDistinct("user_id").alias("unique_customers"),
                F.avg("price").alias("avg_brand_price"),
            )
            .withColumn(
                "avg_revenue_per_product",
                F.col("brand_revenue") / F.col("products_in_brand"),
            )
        )

    def _analyze_seasonal_performance(self, transactions_df: DataFrame) -> DataFrame:
        """Analyze seasonal product performance patterns."""
        seasonal_df = (
            transactions_df.withColumn("month", F.month("timestamp"))
            .withColumn("quarter", F.quarter("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))
        )

        return (
            seasonal_df.groupBy("product_id", "month")
            .agg(
                F.sum("price" * F.col("quantity")).alias("monthly_revenue"),
                F.sum("quantity").alias("monthly_units_sold"),
                F.first("product_category").alias("product_category"),
            )
            .withColumn(
                "seasonal_index",
                F.col("monthly_revenue")
                / F.avg("monthly_revenue").over(Window.partitionBy("product_id")),
            )
        )

    def _analyze_inventory_performance(
        self, product_metrics_df: DataFrame, inventory_df: DataFrame
    ) -> DataFrame:
        """Analyze inventory performance and turnover."""
        # Join sales data with inventory data
        inventory_performance = product_metrics_df.join(
            inventory_df, "product_id", "left"
        )

        return (
            inventory_performance.groupBy("product_id")
            .agg(
                F.sum("units_sold").alias("total_units_sold"),
                F.avg("stock_quantity").alias("avg_stock_level"),
                F.min("stock_quantity").alias("min_stock_level"),
                F.max("stock_quantity").alias("max_stock_level"),
                F.first("product_category").alias("product_category"),
            )
            .withColumn(
                "inventory_turnover",
                F.col("total_units_sold") / F.col("avg_stock_level"),
            )
            .withColumn(
                "stockout_risk",
                F.when(F.col("min_stock_level") <= 5, "High")
                .when(F.col("min_stock_level") <= 20, "Medium")
                .otherwise("Low"),
            )
        )

    def _prepare_interaction_data(self, interactions_df: DataFrame) -> DataFrame:
        """Prepare interaction data for recommendation model."""
        return (
            interactions_df.groupBy("user_id", "product_id")
            .agg(
                F.count("*").alias("interaction_count"),
                F.sum("quantity").alias("total_quantity"),
                F.sum("price" * F.col("quantity")).alias("total_spent"),
            )
            .withColumn(
                "rating",
                F.least(
                    F.lit(5.0),
                    F.greatest(
                        F.lit(1.0),
                        F.log1p("interaction_count") + F.log1p("total_quantity"),
                    ),
                ),
            )
        )

    def _filter_interactions_by_frequency(
        self, interactions_df: DataFrame
    ) -> DataFrame:
        """Filter interactions by minimum frequency."""
        min_interactions = self.config["recommendation"]["min_interactions"]

        # Filter users and products with minimum interactions
        user_counts = interactions_df.groupBy("user_id").count()
        product_counts = interactions_df.groupBy("product_id").count()

        valid_users = user_counts.filter(F.col("count") >= min_interactions).select(
            "user_id"
        )
        valid_products = product_counts.filter(
            F.col("count") >= min_interactions
        ).select("product_id")

        return interactions_df.join(valid_users, "user_id", "inner").join(
            valid_products, "product_id", "inner"
        )

    def _train_als_model(self, interactions_df: DataFrame) -> Tuple[Any, Dict]:
        """Train ALS recommendation model."""
        # Split data for training and validation
        train_df, val_df = interactions_df.randomSplit([0.8, 0.2], seed=42)

        # Create ALS model
        als = ALS(
            maxIter=self.config["recommendation"]["als_max_iter"],
            regParam=self.config["recommendation"]["als_reg_param"],
            rank=self.config["recommendation"]["als_rank"],
            userCol="user_id",
            itemCol="product_id",
            ratingCol="rating",
            coldStartStrategy="drop",
            seed=42,
        )

        # Train model
        model = als.fit(train_df)

        # Evaluate model
        predictions = model.transform(val_df)
        rmse = predictions.agg(
            F.sqrt(F.avg((F.col("rating") - F.col("prediction")) ** 2))
        ).collect()[0][0]

        metrics = {
            "rmse": rmse,
            "training_samples": train_df.count(),
            "validation_samples": val_df.count(),
        }

        return model, metrics

    def _generate_user_recommendations(
        self, model: Any, interactions_df: DataFrame
    ) -> DataFrame:
        """Generate recommendations for all users."""
        top_n = self.config["recommendation"]["top_n_recommendations"]

        # Get unique users
        users = interactions_df.select("user_id").distinct()

        # Generate recommendations
        user_recs = model.recommendForUserSubset(users, top_n)

        return user_recs.select(
            "user_id",
            F.explode("recommendations").alias("rec"),
        ).select("user_id", "rec.product_id", "rec.rating")

    def _calculate_item_similarities(
        self, model: Any, interactions_df: DataFrame
    ) -> DataFrame:
        """Calculate item-to-item similarities."""
        # Get item factors
        item_factors = model.itemFactors

        # Calculate cosine similarity (simplified approach)
        # In practice, you'd use more sophisticated similarity calculations
        return (
            item_factors.alias("item1")
            .crossJoin(item_factors.alias("item2"))
            .select(
                F.col("item1.id").alias("product_id_1"),
                F.col("item2.id").alias("product_id_2"),
                # Simplified similarity calculation
                F.lit(0.5).alias("similarity_score"),  # Placeholder
            )
            .filter(F.col("product_id_1") != F.col("product_id_2"))
        )

    def _create_recommendation_functions(
        self, model: Any, interactions_df: DataFrame
    ) -> Dict:
        """Create recommendation functions for different use cases."""
        return {
            "user_recommendations": lambda user_id, n=10: model.recommendForUserSubset(
                self.spark.createDataFrame([(user_id,)], ["user_id"]), n
            ),
            "item_recommendations": lambda product_id, n=10: model.recommendForItemSubset(
                self.spark.createDataFrame([(product_id,)], ["product_id"]), n
            ),
        }

    def _build_content_based_recommendations(
        self, product_features_df: DataFrame, interactions_df: DataFrame
    ) -> DataFrame:
        """Build content-based recommendations using product features."""
        # This is a simplified implementation
        # In practice, you'd use TF-IDF, word embeddings, etc.

        return (
            product_features_df.join(interactions_df, "product_id")
            .groupBy("product_category")
            .agg(F.collect_list("product_id").alias("similar_products"))
        )

    def _prepare_basket_data(self, transactions_df: DataFrame) -> DataFrame:
        """Prepare transaction data for market basket analysis."""
        return (
            transactions_df.groupBy("transaction_id")
            .agg(F.collect_list("product_id").alias("items"))
            .filter(
                F.size("items") >= self.config["market_basket"]["min_transaction_items"]
            )
        )

    def _filter_baskets_by_size(self, basket_df: DataFrame) -> DataFrame:
        """Filter baskets by minimum size."""
        min_items = self.config["market_basket"]["min_transaction_items"]
        return basket_df.filter(F.size("items") >= min_items)

    def _calculate_itemset_metrics(self, frequent_itemsets: DataFrame) -> DataFrame:
        """Calculate additional metrics for frequent itemsets."""
        return frequent_itemsets.withColumn("itemset_size", F.size("items")).withColumn(
            "lift", F.col("freq") / F.lit(0.1)
        )  # Simplified lift calculation

    def _calculate_rule_metrics(self, association_rules: DataFrame) -> DataFrame:
        """Calculate additional metrics for association rules."""
        return association_rules.withColumn(
            "rule_strength", F.col("confidence") * F.col("lift")
        ).withColumn("conviction", F.lit(1.0) / (F.lit(1.0) - F.col("confidence")))

    def _analyze_product_affinity(self, association_rules: DataFrame) -> DataFrame:
        """Analyze product affinity from association rules."""
        return (
            association_rules.select(
                F.explode("antecedent").alias("product_1"),
                F.explode("consequent").alias("product_2"),
                "confidence",
                "lift",
            )
            .groupBy("product_1", "product_2")
            .agg(
                F.max("confidence").alias("max_confidence"),
                F.max("lift").alias("max_lift"),
            )
        )

    def _create_cross_sell_recommendations(
        self, association_rules: DataFrame
    ) -> DataFrame:
        """Create cross-sell recommendations from association rules."""
        return (
            association_rules.filter(F.col("confidence") > 0.5)
            .select(
                F.explode("antecedent").alias("anchor_product"),
                F.explode("consequent").alias("recommended_product"),
                "confidence",
                "lift",
            )
            .orderBy(F.desc("confidence"), F.desc("lift"))
        )

    def _prepare_product_time_series(self, transactions_df: DataFrame) -> DataFrame:
        """Prepare time series data for product lifecycle analysis."""
        return (
            transactions_df.withColumn("date", F.to_date("timestamp"))
            .groupBy("product_id", "date")
            .agg(
                F.sum("quantity").alias("daily_units_sold"),
                F.sum("price" * F.col("quantity")).alias("daily_revenue"),
                F.count("transaction_id").alias("daily_transactions"),
            )
        )

    def _calculate_product_trends(self, time_series_df: DataFrame) -> DataFrame:
        """Calculate product trends and growth rates."""
        window_spec = Window.partitionBy("product_id").orderBy("date")

        return (
            time_series_df.withColumn(
                "prev_units", F.lag("daily_units_sold", 7).over(window_spec)
            )
            .withColumn(
                "growth_rate",
                (F.col("daily_units_sold") - F.col("prev_units")) / F.col("prev_units"),
            )
            .withColumn(
                "units_ma_7d",
                F.avg("daily_units_sold").over(window_spec.rowsBetween(-6, 0)),
            )
            .withColumn(
                "units_ma_30d",
                F.avg("daily_units_sold").over(window_spec.rowsBetween(-29, 0)),
            )
        )

    def _classify_lifecycle_stages(self, product_trends: DataFrame) -> DataFrame:
        """Classify products into lifecycle stages."""
        growth_threshold = self.config["lifecycle_analysis"]["growth_threshold"]
        decline_threshold = self.config["lifecycle_analysis"]["decline_threshold"]

        return (
            product_trends.groupBy("product_id")
            .agg(
                F.avg("growth_rate").alias("avg_growth_rate"),
                F.stddev("growth_rate").alias("growth_volatility"),
                F.avg("daily_units_sold").alias("avg_daily_sales"),
                F.count("*").alias("days_active"),
            )
            .withColumn(
                "lifecycle_stage",
                F.when(F.col("avg_growth_rate") > growth_threshold, "growth")
                .when(F.col("avg_growth_rate") < decline_threshold, "decline")
                .when(F.col("growth_volatility") < 0.05, "maturity")
                .otherwise("introduction"),
            )
        )

    def _analyze_stage_transitions(self, lifecycle_stages: DataFrame) -> DataFrame:
        """Analyze transitions between lifecycle stages."""
        # This would typically involve time series analysis
        # Simplified implementation showing stage distribution
        return lifecycle_stages.groupBy("lifecycle_stage").agg(
            F.count("*").alias("product_count"),
            F.avg("avg_growth_rate").alias("avg_stage_growth_rate"),
            F.avg("avg_daily_sales").alias("avg_stage_sales"),
        )

    def _calculate_lifecycle_metrics(
        self, product_trends: DataFrame, lifecycle_stages: DataFrame
    ) -> DataFrame:
        """Calculate comprehensive lifecycle metrics."""
        return lifecycle_stages.withColumn(
            "stage_performance_score",
            F.when(F.col("lifecycle_stage") == "growth", F.col("avg_growth_rate") * 10)
            .when(
                F.col("lifecycle_stage") == "maturity", F.col("avg_daily_sales") / 100
            )
            .when(F.col("lifecycle_stage") == "decline", -F.col("avg_growth_rate") * 5)
            .otherwise(F.lit(5)),
        )

    def _identify_at_risk_products(self, lifecycle_stages: DataFrame) -> DataFrame:
        """Identify products at risk of declining."""
        return lifecycle_stages.filter(
            (F.col("lifecycle_stage") == "decline")
            | (
                (F.col("lifecycle_stage") == "maturity")
                & (F.col("avg_growth_rate") < 0)
            )
        )

    def _identify_growth_opportunities(self, lifecycle_stages: DataFrame) -> DataFrame:
        """Identify products with growth opportunities."""
        return lifecycle_stages.filter(
            (F.col("lifecycle_stage") == "introduction")
            | (
                (F.col("lifecycle_stage") == "growth")
                & (F.col("avg_growth_rate") > 0.2)
            )
        )

    def _analyze_product_launch_performance(
        self, lifecycle_stages: DataFrame, launch_dates_df: DataFrame
    ) -> DataFrame:
        """Analyze product performance since launch."""
        return lifecycle_stages.join(launch_dates_df, "product_id", "left").withColumn(
            "days_since_launch",
            F.datediff(F.current_date(), F.col("launch_date")),
        )

    def _calculate_stage_distribution(self, lifecycle_stages: DataFrame) -> DataFrame:
        """Calculate distribution of products across lifecycle stages."""
        total_products = lifecycle_stages.count()

        return (
            lifecycle_stages.groupBy("lifecycle_stage")
            .agg(F.count("*").alias("product_count"))
            .withColumn(
                "percentage",
                (F.col("product_count") / total_products * 100).cast(DoubleType()),
            )
        )

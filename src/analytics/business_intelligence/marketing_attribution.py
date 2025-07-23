"""
Marketing attribution engine for comprehensive marketing intelligence.

This module provides multi-touch attribution models, campaign performance tracking,
customer acquisition cost analysis, and marketing ROI calculations for e-commerce platforms.
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


class MarketingAttribution:
    """
    Comprehensive marketing attribution engine.

    Features:
    - Multi-touch attribution models (first-touch, last-touch, linear, time-decay, position-based)
    - Campaign performance tracking across channels
    - Customer acquisition cost (CAC) analysis
    - Marketing ROI calculations and optimization
    - Attribution path analysis and customer journey mapping
    - Channel contribution analysis and budget optimization
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the marketing attribution engine."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Attribution metrics cache for performance
        self.metrics_cache = {}

    def _get_default_config(self) -> Dict:
        """Get default configuration for marketing attribution."""
        return {
            "attribution_models": [
                "first_touch",
                "last_touch",
                "linear",
                "time_decay",
                "position_based",
            ],
            "marketing_channels": [
                "search_paid",
                "search_organic",
                "social_paid",
                "social_organic",
                "email",
                "display",
                "affiliate",
                "direct",
                "referral",
            ],
            "attribution_window_days": 30,
            "time_decay_halflife_days": 7,
            "position_based_weights": {
                "first_touch": 0.4,
                "last_touch": 0.4,
                "middle_touches": 0.2,
            },
            "conversion_events": ["purchase", "signup", "subscription", "trial_start"],
            "cac_calculation": {
                "include_organic": False,
                "attribution_period_days": 30,
                "cohort_analysis_periods": [7, 30, 90, 365],
            },
            "roi_metrics": {
                "revenue_attribution_models": ["last_touch", "linear"],
                "cost_allocation_method": "equal_split",
                "profit_margin": 0.25,
            },
        }

    def analyze_multi_touch_attribution(
        self,
        touchpoint_df: DataFrame,
        conversion_df: DataFrame,
        attribution_model: str = "linear",
        attribution_window_days: Optional[int] = None,
    ) -> Dict[str, DataFrame]:
        """
        Analyze multi-touch attribution for marketing campaigns.

        Args:
            touchpoint_df: Marketing touchpoint data with user journeys
            conversion_df: Conversion events data
            attribution_model: Attribution model to use
            attribution_window_days: Attribution window in days

        Returns:
            Dictionary of DataFrames with attribution analysis
        """
        try:
            attribution_window = (
                attribution_window_days or self.config["attribution_window_days"]
            )

            # Create attribution paths for each conversion
            attribution_paths = self._create_attribution_paths(
                touchpoint_df, conversion_df, attribution_window
            )

            # Apply selected attribution model
            attributed_conversions = self._apply_attribution_model(
                attribution_paths, attribution_model
            )

            # Calculate channel attribution metrics
            channel_attribution = self._calculate_channel_attribution(
                attributed_conversions
            )

            # Analyze attribution paths and customer journeys
            path_analysis = self._analyze_attribution_paths(attribution_paths)

            # Calculate conversion funnel by channel
            conversion_funnel = self._calculate_conversion_funnel(
                touchpoint_df, conversion_df
            )

            return {
                "attribution_paths": attribution_paths,
                "attributed_conversions": attributed_conversions,
                "channel_attribution": channel_attribution,
                "path_analysis": path_analysis,
                "conversion_funnel": conversion_funnel,
                "model_comparison": self._compare_attribution_models(attribution_paths),
                "attribution_summary": {
                    "model_used": attribution_model,
                    "attribution_window_days": attribution_window,
                    "total_conversions": conversion_df.count(),
                    "attributed_conversions": attributed_conversions.count(),
                    "attribution_rate": attributed_conversions.count()
                    / conversion_df.count(),
                },
            }

        except Exception as e:
            self.logger.error(f"Failed to analyze multi-touch attribution: {e}")
            raise

    def track_campaign_performance(
        self,
        campaign_df: DataFrame,
        touchpoint_df: DataFrame,
        conversion_df: DataFrame,
        time_period: str = "month",
    ) -> Dict[str, DataFrame]:
        """
        Track comprehensive campaign performance metrics.

        Args:
            campaign_df: Campaign metadata and spend data
            touchpoint_df: Marketing touchpoint interactions
            conversion_df: Conversion events
            time_period: Analysis time period

        Returns:
            Dictionary of DataFrames with campaign performance analysis
        """
        try:
            # Add time dimension
            campaign_df = self._add_time_dimension(campaign_df, time_period)
            touchpoint_df = self._add_time_dimension(touchpoint_df, time_period)
            conversion_df = self._add_time_dimension(conversion_df, time_period)

            # Calculate basic campaign metrics
            campaign_metrics = self._calculate_campaign_metrics(
                campaign_df, touchpoint_df, conversion_df
            )

            # Analyze campaign effectiveness
            campaign_effectiveness = self._analyze_campaign_effectiveness(
                campaign_metrics
            )

            # Calculate channel performance
            channel_performance = self._calculate_channel_performance(
                touchpoint_df, conversion_df
            )

            # Creative performance analysis
            creative_performance = self._analyze_creative_performance(
                touchpoint_df, conversion_df
            )

            # Budget efficiency analysis
            budget_efficiency = self._analyze_budget_efficiency(campaign_metrics)

            # Time-based performance trends
            performance_trends = self._calculate_performance_trends(
                campaign_metrics, time_period
            )

            return {
                "campaign_metrics": campaign_metrics,
                "campaign_effectiveness": campaign_effectiveness,
                "channel_performance": channel_performance,
                "creative_performance": creative_performance,
                "budget_efficiency": budget_efficiency,
                "performance_trends": performance_trends,
                "campaign_summary": self._generate_campaign_summary(campaign_metrics),
            }

        except Exception as e:
            self.logger.error(f"Failed to track campaign performance: {e}")
            raise

    def calculate_customer_acquisition_cost(
        self,
        marketing_spend_df: DataFrame,
        acquisition_df: DataFrame,
        attribution_df: Optional[DataFrame] = None,
    ) -> Dict[str, DataFrame]:
        """
        Calculate comprehensive customer acquisition cost analysis.

        Args:
            marketing_spend_df: Marketing spend by channel and campaign
            acquisition_df: New customer acquisition data
            attribution_df: Optional attribution data for more accurate CAC

        Returns:
            Dictionary of DataFrames with CAC analysis
        """
        try:
            # Calculate basic CAC by channel
            cac_by_channel = self._calculate_cac_by_channel(
                marketing_spend_df, acquisition_df
            )

            # Calculate attributed CAC if attribution data provided
            attributed_cac = None
            if attribution_df is not None:
                attributed_cac = self._calculate_attributed_cac(
                    marketing_spend_df, attribution_df
                )

            # CAC cohort analysis
            cac_cohorts = self._calculate_cac_cohorts(
                marketing_spend_df, acquisition_df
            )

            # CAC trends and forecasting
            cac_trends = self._calculate_cac_trends(cac_by_channel)

            # Channel efficiency analysis
            channel_efficiency = self._analyze_channel_efficiency(cac_by_channel)

            # CAC benchmarking and optimization
            cac_optimization = self._generate_cac_optimization_recommendations(
                cac_by_channel, cac_cohorts
            )

            return {
                "cac_by_channel": cac_by_channel,
                "attributed_cac": attributed_cac,
                "cac_cohorts": cac_cohorts,
                "cac_trends": cac_trends,
                "channel_efficiency": channel_efficiency,
                "cac_optimization": cac_optimization,
                "cac_summary": self._generate_cac_summary(cac_by_channel),
            }

        except Exception as e:
            self.logger.error(f"Failed to calculate customer acquisition cost: {e}")
            raise

    def calculate_marketing_roi(
        self,
        marketing_spend_df: DataFrame,
        revenue_df: DataFrame,
        attribution_df: DataFrame,
        time_period: str = "month",
    ) -> Dict[str, DataFrame]:
        """
        Calculate comprehensive marketing ROI analysis.

        Args:
            marketing_spend_df: Marketing spend data
            revenue_df: Revenue data attributed to marketing
            attribution_df: Attribution data linking spend to revenue
            time_period: Analysis time period

        Returns:
            Dictionary of DataFrames with ROI analysis
        """
        try:
            # Add time dimensions
            marketing_spend_df = self._add_time_dimension(
                marketing_spend_df, time_period
            )
            revenue_df = self._add_time_dimension(revenue_df, time_period)
            attribution_df = self._add_time_dimension(attribution_df, time_period)

            # Calculate basic ROI metrics
            roi_metrics = self._calculate_roi_metrics(
                marketing_spend_df, revenue_df, attribution_df
            )

            # Channel ROI analysis
            channel_roi = self._calculate_channel_roi(roi_metrics)

            # Campaign ROI analysis
            campaign_roi = self._calculate_campaign_roi(roi_metrics)

            # ROI trends and forecasting
            roi_trends = self._calculate_roi_trends(roi_metrics, time_period)

            # Marketing efficiency analysis
            marketing_efficiency = self._analyze_marketing_efficiency(roi_metrics)

            # Budget allocation optimization
            budget_optimization = self._optimize_budget_allocation(channel_roi)

            # Incremental impact analysis
            incremental_analysis = self._analyze_incremental_impact(
                marketing_spend_df, revenue_df
            )

            return {
                "roi_metrics": roi_metrics,
                "channel_roi": channel_roi,
                "campaign_roi": campaign_roi,
                "roi_trends": roi_trends,
                "marketing_efficiency": marketing_efficiency,
                "budget_optimization": budget_optimization,
                "incremental_analysis": incremental_analysis,
                "roi_summary": self._generate_roi_summary(roi_metrics),
            }

        except Exception as e:
            self.logger.error(f"Failed to calculate marketing ROI: {e}")
            raise

    def _create_attribution_paths(
        self,
        touchpoint_df: DataFrame,
        conversion_df: DataFrame,
        attribution_window_days: int,
    ) -> DataFrame:
        """Create attribution paths linking touchpoints to conversions."""
        # Define attribution window
        window_spec = Window.partitionBy("user_id").orderBy("timestamp")

        # Create conversion events with lookback window
        conversion_with_window = conversion_df.withColumn(
            "attribution_start",
            F.col("conversion_timestamp")
            - F.expr(f"INTERVAL {attribution_window_days} DAYS"),
        )

        # Join touchpoints with conversions within attribution window
        attribution_paths = touchpoint_df.join(
            conversion_with_window,
            (touchpoint_df.user_id == conversion_with_window.user_id)
            & (touchpoint_df.timestamp >= conversion_with_window.attribution_start)
            & (touchpoint_df.timestamp <= conversion_with_window.conversion_timestamp),
            "inner",
        )

        # Add path position and metrics
        attribution_paths = attribution_paths.withColumn(
            "path_position", F.row_number().over(window_spec)
        ).withColumn(
            "time_to_conversion_hours",
            (
                F.col("conversion_timestamp").cast("long")
                - F.col("timestamp").cast("long")
            )
            / 3600,
        )

        return attribution_paths

    def _apply_attribution_model(
        self, attribution_paths: DataFrame, model: str
    ) -> DataFrame:
        """Apply specific attribution model to assign conversion credit."""
        window_spec = Window.partitionBy("conversion_id")

        if model == "first_touch":
            return attribution_paths.withColumn(
                "attribution_weight",
                F.when(F.col("path_position") == 1, 1.0).otherwise(0.0),
            )

        elif model == "last_touch":
            max_position = attribution_paths.withColumn(
                "max_position", F.max("path_position").over(window_spec)
            )
            return max_position.withColumn(
                "attribution_weight",
                F.when(F.col("path_position") == F.col("max_position"), 1.0).otherwise(
                    0.0
                ),
            )

        elif model == "linear":
            path_count = attribution_paths.withColumn(
                "path_count", F.count("*").over(window_spec)
            )
            return path_count.withColumn(
                "attribution_weight", 1.0 / F.col("path_count")
            )

        elif model == "time_decay":
            halflife = self.config["time_decay_halflife_days"] * 24  # Convert to hours
            return attribution_paths.withColumn(
                "attribution_weight",
                F.pow(2, -F.col("time_to_conversion_hours") / halflife),
            )

        elif model == "position_based":
            weights = self.config["position_based_weights"]
            max_position_df = attribution_paths.withColumn(
                "max_position", F.max("path_position").over(window_spec)
            )

            return max_position_df.withColumn(
                "attribution_weight",
                F.when(F.col("path_position") == 1, weights["first_touch"])
                .when(
                    F.col("path_position") == F.col("max_position"),
                    weights["last_touch"],
                )
                .otherwise(
                    weights["middle_touches"]
                    / F.greatest(F.col("max_position") - 2, F.lit(1))
                ),
            )

        else:
            raise ValueError(f"Unsupported attribution model: {model}")

    def _calculate_channel_attribution(
        self, attributed_conversions: DataFrame
    ) -> DataFrame:
        """Calculate attribution metrics by marketing channel."""
        return (
            attributed_conversions.groupBy("channel", "campaign_id")
            .agg(
                F.sum("attribution_weight").alias("attributed_conversions"),
                F.sum(F.col("attribution_weight") * F.col("conversion_value")).alias(
                    "attributed_revenue"
                ),
                F.countDistinct("user_id").alias("unique_users"),
                F.avg("time_to_conversion_hours").alias("avg_time_to_conversion_hours"),
                F.count("*").alias("total_touchpoints"),
            )
            .withColumn(
                "avg_attribution_per_touchpoint",
                F.col("attributed_conversions") / F.col("total_touchpoints"),
            )
        )

    def _analyze_attribution_paths(self, attribution_paths: DataFrame) -> DataFrame:
        """Analyze customer journey paths and touchpoint sequences."""
        # Aggregate paths by user and conversion
        path_analysis = attribution_paths.groupBy("user_id", "conversion_id").agg(
            F.collect_list("channel").alias("channel_path"),
            F.collect_list("campaign_id").alias("campaign_path"),
            F.count("*").alias("path_length"),
            F.max("path_position").alias("total_touchpoints"),
            F.sum("time_to_conversion_hours").alias("total_journey_hours"),
        )

        # Calculate path metrics
        return path_analysis.withColumn(
            "path_efficiency", 1.0 / F.col("path_length")
        ).withColumn(
            "avg_touchpoint_interval_hours",
            F.col("total_journey_hours")
            / F.greatest(F.col("path_length") - 1, F.lit(1)),
        )

    def _calculate_conversion_funnel(
        self, touchpoint_df: DataFrame, conversion_df: DataFrame
    ) -> DataFrame:
        """Calculate conversion funnel metrics by channel."""
        # Calculate impressions, clicks, and conversions by channel
        impressions = (
            touchpoint_df.filter(F.col("event_type") == "impression")
            .groupBy("channel")
            .count()
            .withColumnRenamed("count", "impressions")
        )
        clicks = (
            touchpoint_df.filter(F.col("event_type") == "click")
            .groupBy("channel")
            .count()
            .withColumnRenamed("count", "clicks")
        )
        conversions = (
            conversion_df.groupBy("channel")
            .count()
            .withColumnRenamed("count", "conversions")
        )

        # Join funnel metrics
        funnel = (
            impressions.join(clicks, "channel", "outer")
            .join(conversions, "channel", "outer")
            .fillna(0)
        )

        # Calculate rates
        return (
            funnel.withColumn(
                "click_through_rate", F.col("clicks") / F.col("impressions")
            )
            .withColumn("conversion_rate", F.col("conversions") / F.col("clicks"))
            .withColumn(
                "overall_conversion_rate", F.col("conversions") / F.col("impressions")
            )
        )

    def _compare_attribution_models(self, attribution_paths: DataFrame) -> DataFrame:
        """Compare results across different attribution models."""
        models = self.config["attribution_models"]
        model_comparison = []

        for model in models:
            attributed = self._apply_attribution_model(attribution_paths, model)
            channel_results = self._calculate_channel_attribution(attributed)

            model_summary = channel_results.agg(
                F.sum("attributed_conversions").alias("total_attributed_conversions"),
                F.sum("attributed_revenue").alias("total_attributed_revenue"),
                F.count("*").alias("channels_with_attribution"),
            ).withColumn("attribution_model", F.lit(model))

            model_comparison.append(model_summary)

        # Union all model results
        if model_comparison:
            result = model_comparison[0]
            for df in model_comparison[1:]:
                result = result.union(df)
            return result
        else:
            return self.spark.createDataFrame(
                [],
                "attribution_model STRING, total_attributed_conversions DOUBLE, total_attributed_revenue DOUBLE, channels_with_attribution LONG",
            )

    def _add_time_dimension(self, df: DataFrame, time_period: str) -> DataFrame:
        """Add time dimension column based on period."""
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
        elif time_period == "quarter":
            return df.withColumn(
                "time_dimension",
                F.concat(F.year("timestamp"), F.lit("-Q"), F.quarter("timestamp")),
            )
        else:
            raise ValueError(f"Unsupported time period: {time_period}")

    def _calculate_campaign_metrics(
        self, campaign_df: DataFrame, touchpoint_df: DataFrame, conversion_df: DataFrame
    ) -> DataFrame:
        """Calculate comprehensive campaign performance metrics."""
        # Join campaign data with touchpoints and conversions
        campaign_touchpoints = campaign_df.join(
            touchpoint_df, ["campaign_id", "time_dimension"], "left"
        )
        campaign_conversions = campaign_df.join(
            conversion_df, ["campaign_id", "time_dimension"], "left"
        )

        # Calculate campaign metrics
        return (
            campaign_df.join(
                campaign_touchpoints.groupBy("campaign_id", "time_dimension").agg(
                    F.count("*").alias("total_touchpoints"),
                    F.countDistinct("user_id").alias("unique_users_reached"),
                ),
                ["campaign_id", "time_dimension"],
                "left",
            )
            .join(
                campaign_conversions.groupBy("campaign_id", "time_dimension").agg(
                    F.count("*").alias("total_conversions"),
                    F.sum("conversion_value").alias("total_conversion_value"),
                    F.countDistinct("user_id").alias("unique_converting_users"),
                ),
                ["campaign_id", "time_dimension"],
                "left",
            )
            .fillna(0)
            .withColumn(
                "conversion_rate",
                F.col("total_conversions") / F.col("unique_users_reached"),
            )
            .withColumn(
                "cost_per_conversion",
                F.col("campaign_spend") / F.col("total_conversions"),
            )
            .withColumn(
                "return_on_ad_spend",
                F.col("total_conversion_value") / F.col("campaign_spend"),
            )
        )

    def _analyze_campaign_effectiveness(self, campaign_metrics: DataFrame) -> DataFrame:
        """Analyze campaign effectiveness and performance."""
        # Calculate effectiveness scores and rankings
        window_spec = Window.orderBy(F.desc("return_on_ad_spend"))

        return (
            campaign_metrics.withColumn("roas_rank", F.row_number().over(window_spec))
            .withColumn(
                "efficiency_score",
                (F.col("conversion_rate") * F.col("return_on_ad_spend"))
                / F.col("cost_per_conversion"),
            )
            .withColumn(
                "effectiveness_category",
                F.when(F.col("return_on_ad_spend") >= 4.0, "High Performer")
                .when(F.col("return_on_ad_spend") >= 2.0, "Good Performer")
                .when(F.col("return_on_ad_spend") >= 1.0, "Break Even")
                .otherwise("Underperformer"),
            )
        )

    def _calculate_channel_performance(
        self, touchpoint_df: DataFrame, conversion_df: DataFrame
    ) -> DataFrame:
        """Calculate performance metrics by marketing channel."""
        # Channel touchpoint metrics
        channel_touchpoints = touchpoint_df.groupBy("channel").agg(
            F.count("*").alias("total_touchpoints"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("engagement_score").alias("avg_engagement_score"),
        )

        # Channel conversion metrics
        channel_conversions = conversion_df.groupBy("channel").agg(
            F.count("*").alias("total_conversions"),
            F.sum("conversion_value").alias("total_revenue"),
            F.avg("conversion_value").alias("avg_order_value"),
        )

        # Join and calculate performance metrics
        return (
            channel_touchpoints.join(channel_conversions, "channel", "outer")
            .fillna(0)
            .withColumn(
                "conversion_rate", F.col("total_conversions") / F.col("unique_users")
            )
            .withColumn(
                "revenue_per_user", F.col("total_revenue") / F.col("unique_users")
            )
        )

    def _analyze_creative_performance(
        self, touchpoint_df: DataFrame, conversion_df: DataFrame
    ) -> DataFrame:
        """Analyze creative asset performance."""
        # Creative performance metrics
        creative_metrics = touchpoint_df.groupBy("creative_id", "channel").agg(
            F.count("*").alias("impressions"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias(
                "clicks"
            ),
            F.avg("engagement_score").alias("avg_engagement"),
        )

        # Creative conversion metrics
        creative_conversions = conversion_df.groupBy("creative_id", "channel").agg(
            F.count("*").alias("conversions"),
            F.sum("conversion_value").alias("revenue"),
        )

        # Join and calculate creative performance
        return (
            creative_metrics.join(
                creative_conversions, ["creative_id", "channel"], "outer"
            )
            .fillna(0)
            .withColumn("click_through_rate", F.col("clicks") / F.col("impressions"))
            .withColumn("conversion_rate", F.col("conversions") / F.col("clicks"))
            .withColumn(
                "revenue_per_impression", F.col("revenue") / F.col("impressions")
            )
        )

    def _analyze_budget_efficiency(self, campaign_metrics: DataFrame) -> DataFrame:
        """Analyze budget allocation efficiency."""
        total_spend = campaign_metrics.agg(F.sum("campaign_spend")).collect()[0][0]
        total_revenue = campaign_metrics.agg(F.sum("total_conversion_value")).collect()[
            0
        ][0]

        return (
            campaign_metrics.withColumn(
                "spend_share", F.col("campaign_spend") / total_spend
            )
            .withColumn(
                "revenue_share", F.col("total_conversion_value") / total_revenue
            )
            .withColumn(
                "efficiency_ratio", F.col("revenue_share") / F.col("spend_share")
            )
            .withColumn(
                "budget_recommendation",
                F.when(F.col("efficiency_ratio") > 1.2, "Increase Budget")
                .when(F.col("efficiency_ratio") < 0.8, "Decrease Budget")
                .otherwise("Maintain Budget"),
            )
        )

    def _calculate_performance_trends(
        self, campaign_metrics: DataFrame, time_period: str
    ) -> DataFrame:
        """Calculate performance trends over time."""
        window_spec = Window.partitionBy("campaign_id").orderBy("time_dimension")

        return (
            campaign_metrics.withColumn(
                "prev_roas", F.lag("return_on_ad_spend").over(window_spec)
            )
            .withColumn("roas_change", F.col("return_on_ad_spend") - F.col("prev_roas"))
            .withColumn(
                "prev_conversion_rate", F.lag("conversion_rate").over(window_spec)
            )
            .withColumn(
                "conversion_rate_change",
                F.col("conversion_rate") - F.col("prev_conversion_rate"),
            )
            .withColumn(
                "trend_direction",
                F.when(F.col("roas_change") > 0.1, "Improving")
                .when(F.col("roas_change") < -0.1, "Declining")
                .otherwise("Stable"),
            )
        )

    def _generate_campaign_summary(self, campaign_metrics: DataFrame) -> Dict[str, Any]:
        """Generate campaign performance summary."""
        summary_stats = campaign_metrics.agg(
            F.sum("campaign_spend").alias("total_spend"),
            F.sum("total_conversion_value").alias("total_revenue"),
            F.sum("total_conversions").alias("total_conversions"),
            F.avg("return_on_ad_spend").alias("avg_roas"),
            F.count("*").alias("total_campaigns"),
        ).collect()[0]

        return {
            "total_marketing_spend": float(summary_stats["total_spend"]),
            "total_attributed_revenue": float(summary_stats["total_revenue"]),
            "total_conversions": int(summary_stats["total_conversions"]),
            "overall_roas": float(
                summary_stats["total_revenue"] / summary_stats["total_spend"]
            )
            if summary_stats["total_spend"] > 0
            else 0,
            "average_campaign_roas": float(summary_stats["avg_roas"]),
            "total_campaigns": int(summary_stats["total_campaigns"]),
            "analysis_date": datetime.now().isoformat(),
        }

    def _calculate_cac_by_channel(
        self, marketing_spend_df: DataFrame, acquisition_df: DataFrame
    ) -> DataFrame:
        """Calculate customer acquisition cost by marketing channel."""
        # Aggregate spend by channel
        channel_spend = marketing_spend_df.groupBy("channel").agg(
            F.sum("spend_amount").alias("total_spend")
        )

        # Aggregate acquisitions by channel
        channel_acquisitions = acquisition_df.groupBy("channel").agg(
            F.countDistinct("customer_id").alias("new_customers"),
            F.sum("customer_value").alias("total_customer_value"),
        )

        # Calculate CAC metrics
        return (
            channel_spend.join(channel_acquisitions, "channel", "outer")
            .fillna(0)
            .withColumn(
                "customer_acquisition_cost",
                F.col("total_spend") / F.col("new_customers"),
            )
            .withColumn(
                "customer_lifetime_value",
                F.col("total_customer_value") / F.col("new_customers"),
            )
            .withColumn(
                "ltv_cac_ratio",
                F.col("customer_lifetime_value") / F.col("customer_acquisition_cost"),
            )
            .withColumn(
                "payback_period_months",
                F.col("customer_acquisition_cost")
                / (F.col("customer_lifetime_value") / 12),
            )
        )

    def _calculate_attributed_cac(
        self, marketing_spend_df: DataFrame, attribution_df: DataFrame
    ) -> DataFrame:
        """Calculate CAC using attribution data for more accuracy."""
        # Aggregate attributed acquisitions by channel
        attributed_acquisitions = (
            attribution_df.filter(F.col("conversion_type") == "acquisition")
            .groupBy("channel")
            .agg(
                F.sum("attribution_weight").alias("attributed_acquisitions"),
                F.sum(F.col("attribution_weight") * F.col("customer_value")).alias(
                    "attributed_customer_value"
                ),
            )
        )

        # Aggregate spend by channel
        channel_spend = marketing_spend_df.groupBy("channel").agg(
            F.sum("spend_amount").alias("total_spend")
        )

        # Calculate attributed CAC
        return (
            channel_spend.join(attributed_acquisitions, "channel", "outer")
            .fillna(0)
            .withColumn(
                "attributed_cac",
                F.col("total_spend") / F.col("attributed_acquisitions"),
            )
            .withColumn(
                "attributed_ltv",
                F.col("attributed_customer_value") / F.col("attributed_acquisitions"),
            )
            .withColumn(
                "attributed_ltv_cac_ratio",
                F.col("attributed_ltv") / F.col("attributed_cac"),
            )
        )

    def _calculate_cac_cohorts(
        self, marketing_spend_df: DataFrame, acquisition_df: DataFrame
    ) -> DataFrame:
        """Calculate CAC by acquisition cohorts."""
        # Add cohort information
        cohort_acquisitions = acquisition_df.withColumn(
            "acquisition_cohort", F.date_format("acquisition_date", "yyyy-MM")
        )

        # Aggregate by channel and cohort
        cohort_spend = (
            marketing_spend_df.withColumn(
                "spend_cohort", F.date_format("spend_date", "yyyy-MM")
            )
            .groupBy("channel", "spend_cohort")
            .agg(F.sum("spend_amount").alias("cohort_spend"))
        )

        cohort_acquisitions_agg = cohort_acquisitions.groupBy(
            "channel", "acquisition_cohort"
        ).agg(
            F.countDistinct("customer_id").alias("cohort_acquisitions"),
            F.sum("customer_value").alias("cohort_customer_value"),
        )

        # Join and calculate cohort CAC
        return (
            cohort_spend.join(
                cohort_acquisitions_agg,
                (cohort_spend.channel == cohort_acquisitions_agg.channel)
                & (
                    cohort_spend.spend_cohort
                    == cohort_acquisitions_agg.acquisition_cohort
                ),
                "outer",
            )
            .fillna(0)
            .withColumn(
                "cohort_cac", F.col("cohort_spend") / F.col("cohort_acquisitions")
            )
            .withColumn(
                "cohort_ltv",
                F.col("cohort_customer_value") / F.col("cohort_acquisitions"),
            )
        )

    def _calculate_cac_trends(self, cac_by_channel: DataFrame) -> DataFrame:
        """Calculate CAC trends and forecasting."""
        # This would typically involve time series analysis
        # Simplified implementation showing trend direction
        return cac_by_channel.withColumn(
            "cac_efficiency_score",
            F.when(F.col("ltv_cac_ratio") >= 3.0, "Excellent")
            .when(F.col("ltv_cac_ratio") >= 2.0, "Good")
            .when(F.col("ltv_cac_ratio") >= 1.0, "Acceptable")
            .otherwise("Poor"),
        ).withColumn(
            "channel_recommendation",
            F.when(F.col("ltv_cac_ratio") >= 3.0, "Scale Up")
            .when(F.col("ltv_cac_ratio") >= 2.0, "Maintain")
            .when(F.col("ltv_cac_ratio") >= 1.0, "Optimize")
            .otherwise("Consider Pausing"),
        )

    def _analyze_channel_efficiency(self, cac_by_channel: DataFrame) -> DataFrame:
        """Analyze channel efficiency for CAC optimization."""
        # Calculate efficiency metrics
        avg_cac = cac_by_channel.agg(F.avg("customer_acquisition_cost")).collect()[0][0]
        avg_ltv = cac_by_channel.agg(F.avg("customer_lifetime_value")).collect()[0][0]

        return (
            cac_by_channel.withColumn(
                "cac_vs_avg", F.col("customer_acquisition_cost") / avg_cac
            )
            .withColumn("ltv_vs_avg", F.col("customer_lifetime_value") / avg_ltv)
            .withColumn("efficiency_index", F.col("ltv_vs_avg") / F.col("cac_vs_avg"))
            .withColumn(
                "channel_priority",
                F.when(F.col("efficiency_index") > 1.2, "High Priority")
                .when(F.col("efficiency_index") > 0.8, "Medium Priority")
                .otherwise("Low Priority"),
            )
        )

    def _generate_cac_optimization_recommendations(
        self, cac_by_channel: DataFrame, cac_cohorts: DataFrame
    ) -> DataFrame:
        """Generate CAC optimization recommendations."""
        # Best performing channels
        top_channels = cac_by_channel.orderBy(F.desc("ltv_cac_ratio")).limit(3)

        # Underperforming channels
        bottom_channels = cac_by_channel.filter(F.col("ltv_cac_ratio") < 1.5)

        # Generate recommendations
        recommendations = cac_by_channel.withColumn(
            "optimization_action",
            F.when(
                F.col("ltv_cac_ratio") >= 3.0, "Increase investment - highly profitable"
            )
            .when(F.col("ltv_cac_ratio") >= 2.0, "Maintain current investment")
            .when(F.col("ltv_cac_ratio") >= 1.0, "Optimize targeting and creative")
            .otherwise("Reduce investment or pause channel"),
        ).withColumn(
            "investment_priority",
            F.row_number().over(Window.orderBy(F.desc("ltv_cac_ratio"))),
        )

        return recommendations

    def _generate_cac_summary(self, cac_by_channel: DataFrame) -> Dict[str, Any]:
        """Generate CAC analysis summary."""
        summary_stats = cac_by_channel.agg(
            F.sum("total_spend").alias("total_marketing_spend"),
            F.sum("new_customers").alias("total_new_customers"),
            F.avg("customer_acquisition_cost").alias("avg_cac"),
            F.avg("customer_lifetime_value").alias("avg_ltv"),
            F.avg("ltv_cac_ratio").alias("avg_ltv_cac_ratio"),
        ).collect()[0]

        return {
            "total_marketing_spend": float(summary_stats["total_marketing_spend"]),
            "total_new_customers": int(summary_stats["total_new_customers"]),
            "blended_cac": float(
                summary_stats["total_marketing_spend"]
                / summary_stats["total_new_customers"]
            )
            if summary_stats["total_new_customers"] > 0
            else 0,
            "average_ltv": float(summary_stats["avg_ltv"]),
            "average_ltv_cac_ratio": float(summary_stats["avg_ltv_cac_ratio"]),
            "channels_analyzed": cac_by_channel.count(),
            "analysis_date": datetime.now().isoformat(),
        }

    def _calculate_roi_metrics(
        self,
        marketing_spend_df: DataFrame,
        revenue_df: DataFrame,
        attribution_df: DataFrame,
    ) -> DataFrame:
        """Calculate comprehensive ROI metrics."""
        # Join spend with attributed revenue
        roi_base = marketing_spend_df.join(
            attribution_df.groupBy("channel", "campaign_id", "time_dimension").agg(
                F.sum(F.col("attribution_weight") * F.col("conversion_value")).alias(
                    "attributed_revenue"
                )
            ),
            ["channel", "campaign_id", "time_dimension"],
            "left",
        ).fillna(0)

        # Calculate ROI metrics
        return (
            roi_base.withColumn(
                "return_on_investment",
                (F.col("attributed_revenue") - F.col("spend_amount"))
                / F.col("spend_amount"),
            )
            .withColumn(
                "return_on_ad_spend",
                F.col("attributed_revenue") / F.col("spend_amount"),
            )
            .withColumn(
                "profit_margin",
                (F.col("attributed_revenue") - F.col("spend_amount"))
                / F.col("attributed_revenue"),
            )
            .withColumn(
                "cost_efficiency", F.col("spend_amount") / F.col("attributed_revenue")
            )
        )

    def _calculate_channel_roi(self, roi_metrics: DataFrame) -> DataFrame:
        """Calculate ROI metrics aggregated by channel."""
        return (
            roi_metrics.groupBy("channel")
            .agg(
                F.sum("spend_amount").alias("total_spend"),
                F.sum("attributed_revenue").alias("total_attributed_revenue"),
                F.avg("return_on_investment").alias("avg_roi"),
                F.avg("return_on_ad_spend").alias("avg_roas"),
                F.count("*").alias("campaign_count"),
            )
            .withColumn(
                "channel_roi",
                (F.col("total_attributed_revenue") - F.col("total_spend"))
                / F.col("total_spend"),
            )
            .withColumn(
                "channel_roas", F.col("total_attributed_revenue") / F.col("total_spend")
            )
        )

    def _calculate_campaign_roi(self, roi_metrics: DataFrame) -> DataFrame:
        """Calculate ROI metrics by individual campaign."""
        return (
            roi_metrics.groupBy("campaign_id", "channel")
            .agg(
                F.sum("spend_amount").alias("campaign_spend"),
                F.sum("attributed_revenue").alias("campaign_revenue"),
                F.first("return_on_investment").alias("campaign_roi"),
                F.first("return_on_ad_spend").alias("campaign_roas"),
            )
            .withColumn(
                "roi_category",
                F.when(F.col("campaign_roi") > 2.0, "High ROI")
                .when(F.col("campaign_roi") > 0.5, "Moderate ROI")
                .when(F.col("campaign_roi") > 0, "Low ROI")
                .otherwise("Negative ROI"),
            )
        )

    def _calculate_roi_trends(
        self, roi_metrics: DataFrame, time_period: str
    ) -> DataFrame:
        """Calculate ROI trends over time."""
        window_spec = Window.partitionBy("channel").orderBy("time_dimension")

        return (
            roi_metrics.groupBy("channel", "time_dimension")
            .agg(
                F.sum("spend_amount").alias("period_spend"),
                F.sum("attributed_revenue").alias("period_revenue"),
                F.avg("return_on_investment").alias("period_roi"),
            )
            .withColumn("prev_roi", F.lag("period_roi").over(window_spec))
            .withColumn("roi_change", F.col("period_roi") - F.col("prev_roi"))
            .withColumn(
                "roi_trend",
                F.when(F.col("roi_change") > 0.1, "Improving")
                .when(F.col("roi_change") < -0.1, "Declining")
                .otherwise("Stable"),
            )
        )

    def _analyze_marketing_efficiency(self, roi_metrics: DataFrame) -> DataFrame:
        """Analyze overall marketing efficiency."""
        # Calculate efficiency scores
        return (
            roi_metrics.withColumn(
                "efficiency_score",
                (F.col("return_on_investment") + F.col("return_on_ad_spend")) / 2,
            )
            .withColumn(
                "spend_efficiency_decile",
                F.ntile(10).over(Window.orderBy(F.desc("return_on_investment"))),
            )
            .withColumn(
                "performance_tier",
                F.when(F.col("spend_efficiency_decile") <= 2, "Top Performer")
                .when(F.col("spend_efficiency_decile") <= 5, "Above Average")
                .when(F.col("spend_efficiency_decile") <= 8, "Average")
                .otherwise("Below Average"),
            )
        )

    def _optimize_budget_allocation(self, channel_roi: DataFrame) -> DataFrame:
        """Generate budget allocation optimization recommendations."""
        total_spend = channel_roi.agg(F.sum("total_spend")).collect()[0][0]

        return (
            channel_roi.withColumn(
                "current_spend_share", F.col("total_spend") / total_spend
            )
            .withColumn(
                "roi_weight",
                F.col("channel_roi") / F.sum("channel_roi").over(Window.partitionBy()),
            )
            .withColumn("optimal_spend_share", F.col("roi_weight"))
            .withColumn(
                "spend_reallocation",
                F.col("optimal_spend_share") - F.col("current_spend_share"),
            )
            .withColumn(
                "budget_recommendation",
                F.when(F.col("spend_reallocation") > 0.05, "Increase Budget")
                .when(F.col("spend_reallocation") < -0.05, "Decrease Budget")
                .otherwise("Maintain Budget"),
            )
        )

    def _analyze_incremental_impact(
        self, marketing_spend_df: DataFrame, revenue_df: DataFrame
    ) -> DataFrame:
        """Analyze incremental impact of marketing spend."""
        # Simple correlation analysis (in practice, would use more sophisticated causal inference)
        window_spec = Window.partitionBy("channel").orderBy("time_dimension")

        # Calculate period-over-period changes
        spend_changes = marketing_spend_df.withColumn(
            "prev_spend", F.lag("spend_amount").over(window_spec)
        ).withColumn("spend_change", F.col("spend_amount") - F.col("prev_spend"))

        revenue_changes = revenue_df.withColumn(
            "prev_revenue", F.lag("revenue_amount").over(window_spec)
        ).withColumn("revenue_change", F.col("revenue_amount") - F.col("prev_revenue"))

        # Join and calculate incremental impact
        incremental = spend_changes.join(
            revenue_changes, ["channel", "time_dimension"], "inner"
        ).filter(
            F.col("spend_change").isNotNull() & F.col("revenue_change").isNotNull()
        )

        return incremental.withColumn(
            "incremental_roas", F.col("revenue_change") / F.col("spend_change")
        ).withColumn(
            "incrementality_score",
            F.when(F.col("incremental_roas") > 3.0, "High Incrementality")
            .when(F.col("incremental_roas") > 1.0, "Moderate Incrementality")
            .when(F.col("incremental_roas") > 0, "Low Incrementality")
            .otherwise("No Incrementality"),
        )

    def _generate_roi_summary(self, roi_metrics: DataFrame) -> Dict[str, Any]:
        """Generate ROI analysis summary."""
        summary_stats = roi_metrics.agg(
            F.sum("spend_amount").alias("total_spend"),
            F.sum("attributed_revenue").alias("total_revenue"),
            F.avg("return_on_investment").alias("avg_roi"),
            F.avg("return_on_ad_spend").alias("avg_roas"),
            F.count("*").alias("total_campaigns"),
        ).collect()[0]

        overall_roi = (
            (summary_stats["total_revenue"] - summary_stats["total_spend"])
            / summary_stats["total_spend"]
            if summary_stats["total_spend"] > 0
            else 0
        )
        overall_roas = (
            summary_stats["total_revenue"] / summary_stats["total_spend"]
            if summary_stats["total_spend"] > 0
            else 0
        )

        return {
            "total_marketing_spend": float(summary_stats["total_spend"]),
            "total_attributed_revenue": float(summary_stats["total_revenue"]),
            "overall_roi": float(overall_roi),
            "overall_roas": float(overall_roas),
            "average_campaign_roi": float(summary_stats["avg_roi"]),
            "average_campaign_roas": float(summary_stats["avg_roas"]),
            "total_campaigns_analyzed": int(summary_stats["total_campaigns"]),
            "roi_efficiency": "Excellent"
            if overall_roi > 2.0
            else "Good"
            if overall_roi > 0.5
            else "Poor",
            "analysis_date": datetime.now().isoformat(),
        }

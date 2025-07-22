"""
Merchant risk scoring system for fraud detection.
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class MerchantRiskLevel(Enum):
    """Merchant risk levels."""
    VERY_LOW = "very_low"
    LOW = "low" 
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


@dataclass
class MerchantProfile:
    """Merchant profile information."""
    merchant_id: str
    merchant_name: Optional[str]
    category: Optional[str]
    registration_date: Optional[str]
    transaction_volume: int
    total_revenue: float
    average_transaction_amount: float
    unique_customers: int
    chargeback_rate: float
    refund_rate: float
    risk_score: float
    risk_level: MerchantRiskLevel


class MerchantRiskScorer:
    """
    Merchant risk scoring system that evaluates merchant risk based on:
    - Transaction patterns and volumes
    - Customer behavior with the merchant
    - Financial metrics and ratios
    - Historical fraud patterns
    - Business profile characteristics
    """
    
    def __init__(self, spark: SparkSession):
        """Initialize the merchant risk scorer."""
        self.spark = spark
        self.risk_factors = {
            'transaction_volume_weight': 0.15,
            'chargeback_rate_weight': 0.25,
            'refund_rate_weight': 0.20,
            'customer_diversity_weight': 0.15,
            'amount_volatility_weight': 0.10,
            'new_merchant_penalty': 0.15
        }
    
    def calculate_merchant_risk_scores(self, transactions_df: DataFrame, 
                                     lookback_days: int = 30) -> DataFrame:
        """
        Calculate comprehensive risk scores for all merchants.
        
        Args:
            transactions_df: Transaction DataFrame
            lookback_days: Number of days to look back for analysis
            
        Returns:
            DataFrame with merchant risk scores
        """
        # Filter to recent transactions
        cutoff_date = F.date_sub(F.current_date(), lookback_days)
        recent_df = transactions_df.filter(F.col("timestamp") >= cutoff_date)
        
        # Calculate basic merchant metrics
        merchant_metrics = self._calculate_merchant_metrics(recent_df)
        
        # Calculate risk indicators
        risk_indicators = self._calculate_risk_indicators(recent_df, merchant_metrics)
        
        # Calculate final risk scores
        risk_scores = self._calculate_final_risk_scores(risk_indicators)
        
        return risk_scores
    
    def _calculate_merchant_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate basic merchant metrics."""
        return df.groupBy("merchant_id").agg(
            F.count("*").alias("transaction_count"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_transaction_amount"),
            F.stddev("price").alias("amount_stddev"),
            F.min("price").alias("min_amount"),
            F.max("price").alias("max_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.min("timestamp").alias("first_transaction"),
            F.max("timestamp").alias("last_transaction"),
            F.collect_list("payment_method").alias("payment_methods"),
            F.countDistinct("payment_method").alias("distinct_payment_methods")
        )
    
    def _calculate_risk_indicators(self, transactions_df: DataFrame, 
                                 merchant_metrics: DataFrame) -> DataFrame:
        """Calculate various risk indicators for merchants."""
        
        # Join transactions with merchant metrics
        enriched_df = transactions_df.join(merchant_metrics, "merchant_id")
        
        # Calculate additional risk indicators
        risk_df = enriched_df.groupBy("merchant_id").agg(
            # Volume-based indicators
            F.first("transaction_count").alias("transaction_count"),
            F.first("total_revenue").alias("total_revenue"),
            F.first("avg_transaction_amount").alias("avg_transaction_amount"),
            F.first("amount_stddev").alias("amount_stddev"),
            F.first("unique_customers").alias("unique_customers"),
            F.first("distinct_payment_methods").alias("distinct_payment_methods"),
            
            # Time-based patterns
            F.first("first_transaction").alias("first_transaction"),
            F.first("last_transaction").alias("last_transaction"),
            
            # Behavioral indicators
            F.sum(F.when(F.col("price") > F.col("avg_transaction_amount") * 3, 1).otherwise(0)).alias("high_amount_transactions"),
            F.sum(F.when(F.hour("timestamp").between(0, 5), 1).otherwise(0)).alias("unusual_hour_transactions"),
            F.sum(F.when(F.dayofweek("timestamp").isin([1, 7]), 1).otherwise(0)).alias("weekend_transactions"),
            
            # Round amounts (potential fraud indicator)
            F.sum(F.when((F.col("price") % 100 == 0) & (F.col("price") > 100), 1).otherwise(0)).alias("round_amount_transactions")
        )
        
        # Calculate derived metrics
        risk_df = risk_df.withColumn(
            "merchant_age_days",
            F.datediff(F.current_date(), F.col("first_transaction"))
        ).withColumn(
            "avg_daily_transactions",
            F.col("transaction_count") / F.greatest(F.col("merchant_age_days"), F.lit(1))
        ).withColumn(
            "revenue_per_customer",
            F.col("total_revenue") / F.greatest(F.col("unique_customers"), F.lit(1))
        ).withColumn(
            "amount_volatility",
            F.when(F.col("avg_transaction_amount") > 0,
                  F.col("amount_stddev") / F.col("avg_transaction_amount")
            ).otherwise(0.0)
        ).withColumn(
            "high_amount_rate",
            F.col("high_amount_transactions") / F.col("transaction_count")
        ).withColumn(
            "unusual_hour_rate",
            F.col("unusual_hour_transactions") / F.col("transaction_count")
        ).withColumn(
            "weekend_rate",
            F.col("weekend_transactions") / F.col("transaction_count")
        ).withColumn(
            "round_amount_rate",
            F.col("round_amount_transactions") / F.col("transaction_count")
        )
        
        return risk_df
    
    def _calculate_final_risk_scores(self, risk_df: DataFrame) -> DataFrame:
        """Calculate final risk scores based on all indicators."""
        
        # Normalize metrics to 0-1 scale using percentile ranks
        score_df = risk_df
        
        # Volume risk (higher volume = lower risk, but very low volume = higher risk)
        volume_percentile_window = Window.orderBy("transaction_count")
        score_df = score_df.withColumn(
            "volume_risk_score",
            F.when(F.col("transaction_count") < 10, 0.8)  # Very low volume is risky
            .when(F.col("transaction_count") < 50, 0.6)   # Low volume is moderately risky
            .otherwise(1.0 - F.percent_rank().over(volume_percentile_window))  # Higher volume = lower risk
        )
        
        # Customer diversity risk (low diversity = higher risk)
        customers_per_transaction = F.col("unique_customers") / F.col("transaction_count")
        score_df = score_df.withColumn(
            "customer_diversity_score",
            F.when(customers_per_transaction < 0.1, 0.9)  # Very low diversity
            .when(customers_per_transaction < 0.3, 0.6)   # Low diversity
            .when(customers_per_transaction < 0.7, 0.3)   # Medium diversity
            .otherwise(0.1)  # High diversity
        )
        
        # Amount volatility risk
        volatility_window = Window.orderBy("amount_volatility")
        score_df = score_df.withColumn(
            "volatility_risk_score",
            F.percent_rank().over(volatility_window)
        )
        
        # Behavioral risk patterns
        score_df = score_df.withColumn(
            "behavioral_risk_score",
            (F.col("high_amount_rate") * 0.3 +
             F.col("unusual_hour_rate") * 0.3 +
             F.col("round_amount_rate") * 0.4)
        )
        
        # New merchant risk
        score_df = score_df.withColumn(
            "new_merchant_risk_score",
            F.when(F.col("merchant_age_days") < 30, 0.8)    # Very new
            .when(F.col("merchant_age_days") < 90, 0.6)     # New
            .when(F.col("merchant_age_days") < 180, 0.4)    # Moderately new
            .otherwise(0.1)  # Established
        )
        
        # Calculate weighted final risk score
        weights = self.risk_factors
        score_df = score_df.withColumn(
            "merchant_risk_score",
            (F.col("volume_risk_score") * weights['transaction_volume_weight'] +
             F.col("customer_diversity_score") * weights['customer_diversity_weight'] +
             F.col("volatility_risk_score") * weights['amount_volatility_weight'] +
             F.col("behavioral_risk_score") * (weights['chargeback_rate_weight'] + weights['refund_rate_weight']) +
             F.col("new_merchant_risk_score") * weights['new_merchant_penalty'])
        )
        
        # Assign risk levels
        score_df = score_df.withColumn(
            "merchant_risk_level",
            F.when(F.col("merchant_risk_score") >= 0.8, "VERY_HIGH")
            .when(F.col("merchant_risk_score") >= 0.6, "HIGH")
            .when(F.col("merchant_risk_score") >= 0.4, "MEDIUM")
            .when(F.col("merchant_risk_score") >= 0.2, "LOW")
            .otherwise("VERY_LOW")
        )
        
        # Add risk flags
        score_df = score_df.withColumn(
            "risk_flags",
            F.array(
                F.when(F.col("transaction_count") < 10, "LOW_VOLUME").otherwise(F.lit(None)),
                F.when(F.col("customer_diversity_score") > 0.7, "LOW_CUSTOMER_DIVERSITY").otherwise(F.lit(None)),
                F.when(F.col("volatility_risk_score") > 0.8, "HIGH_AMOUNT_VOLATILITY").otherwise(F.lit(None)),
                F.when(F.col("unusual_hour_rate") > 0.3, "HIGH_UNUSUAL_HOURS").otherwise(F.lit(None)),
                F.when(F.col("round_amount_rate") > 0.5, "HIGH_ROUND_AMOUNTS").otherwise(F.lit(None)),
                F.when(F.col("merchant_age_days") < 30, "NEW_MERCHANT").otherwise(F.lit(None))
            ).cast("array<string>")
        )
        
        # Remove nulls from risk flags
        score_df = score_df.withColumn(
            "risk_flags",
            F.expr("filter(risk_flags, x -> x is not null)")
        )
        
        return score_df
    
    def get_high_risk_merchants(self, merchant_scores_df: DataFrame, 
                              min_risk_score: float = 0.6) -> DataFrame:
        """
        Get merchants with high risk scores.
        
        Args:
            merchant_scores_df: DataFrame with merchant risk scores
            min_risk_score: Minimum risk score threshold
            
        Returns:
            DataFrame with high-risk merchants
        """
        return merchant_scores_df.filter(
            F.col("merchant_risk_score") >= min_risk_score
        ).orderBy(F.col("merchant_risk_score").desc())
    
    def analyze_merchant_transaction_patterns(self, transactions_df: DataFrame, 
                                            merchant_id: str) -> Dict:
        """
        Analyze detailed transaction patterns for a specific merchant.
        
        Args:
            transactions_df: Transaction DataFrame
            merchant_id: Merchant ID to analyze
            
        Returns:
            Dictionary with detailed merchant analysis
        """
        merchant_data = transactions_df.filter(F.col("merchant_id") == merchant_id)
        
        if merchant_data.count() == 0:
            return {"error": f"No transactions found for merchant {merchant_id}"}
        
        # Basic statistics
        stats = merchant_data.agg(
            F.count("*").alias("total_transactions"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_amount"),
            F.stddev("price").alias("amount_stddev"),
            F.min("timestamp").alias("first_transaction"),
            F.max("timestamp").alias("last_transaction"),
            F.countDistinct("customer_id").alias("unique_customers")
        ).collect()[0]
        
        # Time pattern analysis
        hourly_pattern = merchant_data.groupBy(F.hour("timestamp").alias("hour"))\
                                    .agg(F.count("*").alias("count"))\
                                    .orderBy("hour")\
                                    .collect()
        
        # Daily pattern analysis
        daily_pattern = merchant_data.groupBy(F.date_format("timestamp", "EEEE").alias("day"))\
                                   .agg(F.count("*").alias("count"))\
                                   .collect()
        
        # Amount distribution
        amount_distribution = merchant_data.select(
            F.when(F.col("price") < 50, "0-50")
            .when(F.col("price") < 100, "50-100")
            .when(F.col("price") < 500, "100-500")
            .when(F.col("price") < 1000, "500-1000")
            .otherwise("1000+").alias("amount_range")
        ).groupBy("amount_range").agg(F.count("*").alias("count")).collect()
        
        return {
            "merchant_id": merchant_id,
            "basic_stats": {
                "total_transactions": stats["total_transactions"],
                "total_revenue": float(stats["total_revenue"]),
                "avg_amount": float(stats["avg_amount"]),
                "amount_stddev": float(stats["amount_stddev"]) if stats["amount_stddev"] else 0.0,
                "unique_customers": stats["unique_customers"],
                "first_transaction": str(stats["first_transaction"]),
                "last_transaction": str(stats["last_transaction"])
            },
            "hourly_pattern": {row["hour"]: row["count"] for row in hourly_pattern},
            "daily_pattern": {row["day"]: row["count"] for row in daily_pattern},
            "amount_distribution": {row["amount_range"]: row["count"] for row in amount_distribution}
        }
    
    def update_risk_factors_weights(self, new_weights: Dict[str, float]) -> None:
        """
        Update risk factor weights.
        
        Args:
            new_weights: Dictionary with new weights
        """
        for key, value in new_weights.items():
            if key in self.risk_factors:
                self.risk_factors[key] = value
    
    def save_risk_profiles(self, merchant_scores_df: DataFrame, output_path: str) -> None:
        """
        Save merchant risk profiles to storage.
        
        Args:
            merchant_scores_df: DataFrame with merchant risk scores
            output_path: Output path for saving profiles
        """
        merchant_scores_df.select(
            "merchant_id",
            "merchant_risk_score",
            "merchant_risk_level",
            "transaction_count",
            "total_revenue",
            "unique_customers",
            "risk_flags"
        ).write.mode("overwrite").parquet(output_path)
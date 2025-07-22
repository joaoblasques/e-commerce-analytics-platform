"""
Transaction pattern analysis for fraud detection.
"""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


class TransactionPatternAnalyzer:
    """
    Analyzes transaction patterns to identify potential fraud.
    
    Features:
    - Velocity analysis (transaction frequency)
    - Amount pattern analysis
    - Time-based pattern detection
    - Geographic pattern analysis
    - Payment method patterns
    - Merchant interaction patterns
    """
    
    def __init__(self, spark: SparkSession):
        """Initialize the pattern analyzer."""
        self.spark = spark
    
    def analyze_velocity_patterns(self, df: DataFrame, 
                                time_windows: List[str] = ["1 hour", "1 day", "7 days"]) -> DataFrame:
        """
        Analyze transaction velocity patterns.
        
        Args:
            df: Transaction DataFrame
            time_windows: List of time windows to analyze
            
        Returns:
            DataFrame with velocity analysis columns
        """
        result_df = df
        
        for window in time_windows:
            window_suffix = window.replace(" ", "_")
            
            # Create window specification
            window_spec = Window.partitionBy("customer_id")\
                               .orderBy("timestamp")\
                               .rangeBetween(-self._parse_window_seconds(window), 0)
            
            # Count transactions in window
            result_df = result_df.withColumn(
                f"transaction_count_{window_suffix}",
                F.count("*").over(window_spec)
            )
            
            # Sum amount in window
            result_df = result_df.withColumn(
                f"total_amount_{window_suffix}",
                F.sum("price").over(window_spec)
            )
            
            # Count distinct merchants in window
            result_df = result_df.withColumn(
                f"distinct_merchants_{window_suffix}",
                F.size(F.collect_set("merchant_id").over(window_spec))
            )
            
            # Count distinct payment methods in window
            result_df = result_df.withColumn(
                f"distinct_payment_methods_{window_suffix}",
                F.size(F.collect_set("payment_method").over(window_spec))
            )
        
        return result_df
    
    def analyze_amount_patterns(self, df: DataFrame) -> DataFrame:
        """
        Analyze amount-based patterns.
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with amount pattern analysis
        """
        # Calculate customer's historical spending patterns
        customer_window = Window.partitionBy("customer_id")
        
        result_df = df.withColumn(
            "customer_avg_amount",
            F.avg("price").over(customer_window)
        ).withColumn(
            "customer_max_amount",
            F.max("price").over(customer_window)
        ).withColumn(
            "customer_stddev_amount",
            F.stddev("price").over(customer_window)
        )
        
        # Calculate amount anomaly indicators
        result_df = result_df.withColumn(
            "amount_deviation_from_avg",
            F.abs(F.col("price") - F.col("customer_avg_amount")) / F.col("customer_avg_amount")
        ).withColumn(
            "amount_z_score",
            F.when(F.col("customer_stddev_amount") > 0,
                  (F.col("price") - F.col("customer_avg_amount")) / F.col("customer_stddev_amount")
            ).otherwise(0.0)
        )
        
        # Round amount patterns
        result_df = result_df.withColumn(
            "is_round_amount",
            (F.col("price") % 100 == 0) & (F.col("price") > 0)
        ).withColumn(
            "amount_ends_with_99",
            F.col("price").cast("string").endswith(".99") | 
            F.col("price").cast("string").endswith("99")
        )
        
        # Calculate percentile rank of transaction amount
        percentile_window = Window.orderBy("price")
        result_df = result_df.withColumn(
            "amount_percentile",
            F.percent_rank().over(percentile_window)
        )
        
        return result_df
    
    def analyze_time_patterns(self, df: DataFrame) -> DataFrame:
        """
        Analyze time-based transaction patterns.
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with time pattern analysis
        """
        result_df = df.withColumn("hour", F.hour("timestamp"))\
                     .withColumn("day_of_week", F.dayofweek("timestamp"))\
                     .withColumn("day_of_month", F.dayofmonth("timestamp"))
        
        # Business hours classification
        result_df = result_df.withColumn(
            "is_business_hours",
            (F.col("hour") >= 9) & (F.col("hour") <= 17)
        ).withColumn(
            "is_unusual_hour",
            (F.col("hour") >= 0) & (F.col("hour") <= 5)
        ).withColumn(
            "is_weekend",
            F.col("day_of_week").isin([1, 7])  # Sunday=1, Saturday=7
        )
        
        # Calculate customer's typical transaction times
        customer_time_window = Window.partitionBy("customer_id")
        
        result_df = result_df.withColumn(
            "customer_common_hours",
            F.collect_list("hour").over(customer_time_window)
        ).withColumn(
            "customer_avg_hour",
            F.avg("hour").over(customer_time_window)
        )
        
        # Time deviation from customer's normal pattern
        result_df = result_df.withColumn(
            "hour_deviation_from_normal",
            F.abs(F.col("hour") - F.col("customer_avg_hour"))
        )
        
        # Rapid succession detection (time between consecutive transactions)
        customer_time_ordered_window = Window.partitionBy("customer_id").orderBy("timestamp")
        
        result_df = result_df.withColumn(
            "prev_transaction_time",
            F.lag("timestamp", 1).over(customer_time_ordered_window)
        ).withColumn(
            "seconds_since_last_transaction",
            F.when(F.col("prev_transaction_time").isNotNull(),
                  F.unix_timestamp("timestamp") - F.unix_timestamp("prev_transaction_time")
            ).otherwise(F.lit(None))
        )
        
        # Flag rapid succession (< 60 seconds)
        result_df = result_df.withColumn(
            "is_rapid_succession",
            F.col("seconds_since_last_transaction") < 60
        )
        
        return result_df
    
    def analyze_geographic_patterns(self, df: DataFrame) -> DataFrame:
        """
        Analyze geographic transaction patterns.
        
        Args:
            df: Transaction DataFrame (assumes location columns exist)
            
        Returns:
            DataFrame with geographic pattern analysis
        """
        # Calculate customer's common locations
        customer_location_window = Window.partitionBy("customer_id")
        
        result_df = df.withColumn(
            "customer_locations",
            F.collect_set("location").over(customer_location_window)
        ).withColumn(
            "customer_location_count",
            F.size(F.collect_set("location").over(customer_location_window))
        )
        
        # Check for new locations
        result_df = result_df.withColumn(
            "is_new_location",
            ~F.array_contains(F.col("customer_locations"), F.col("location"))
        )
        
        # Velocity across different locations
        location_time_window = Window.partitionBy("customer_id", "location")\
                                    .orderBy("timestamp")\
                                    .rangeBetween(-3600, 0)  # 1 hour window
        
        result_df = result_df.withColumn(
            "location_transaction_count_1h",
            F.count("*").over(location_time_window)
        )
        
        # Multiple location usage pattern
        multi_location_window = Window.partitionBy("customer_id")\
                                     .orderBy("timestamp")\
                                     .rangeBetween(-3600, 0)  # 1 hour window
        
        result_df = result_df.withColumn(
            "distinct_locations_1h",
            F.size(F.collect_set("location").over(multi_location_window))
        )
        
        return result_df
    
    def analyze_merchant_patterns(self, df: DataFrame) -> DataFrame:
        """
        Analyze merchant interaction patterns.
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with merchant pattern analysis
        """
        # Customer-merchant relationship analysis
        customer_merchant_window = Window.partitionBy("customer_id")
        
        result_df = df.withColumn(
            "customer_merchant_count",
            F.size(F.collect_set("merchant_id").over(customer_merchant_window))
        ).withColumn(
            "customer_merchants",
            F.collect_set("merchant_id").over(customer_merchant_window)
        )
        
        # New merchant flag
        result_df = result_df.withColumn(
            "is_new_merchant",
            ~F.array_contains(F.col("customer_merchants"), F.col("merchant_id"))
        )
        
        # Merchant transaction frequency for customer
        merchant_customer_window = Window.partitionBy("customer_id", "merchant_id")
        
        result_df = result_df.withColumn(
            "merchant_transaction_count",
            F.count("*").over(merchant_customer_window)
        ).withColumn(
            "merchant_total_amount",
            F.sum("price").over(merchant_customer_window)
        ).withColumn(
            "merchant_avg_amount",
            F.avg("price").over(merchant_customer_window)
        )
        
        return result_df
    
    def analyze_payment_patterns(self, df: DataFrame) -> DataFrame:
        """
        Analyze payment method patterns.
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with payment pattern analysis
        """
        # Customer payment method usage
        customer_payment_window = Window.partitionBy("customer_id")
        
        result_df = df.withColumn(
            "customer_payment_methods",
            F.collect_set("payment_method").over(customer_payment_window)
        ).withColumn(
            "customer_payment_method_count",
            F.size(F.collect_set("payment_method").over(customer_payment_window))
        )
        
        # New payment method detection
        result_df = result_df.withColumn(
            "is_new_payment_method",
            ~F.array_contains(F.col("customer_payment_methods"), F.col("payment_method"))
        )
        
        # Payment method switching patterns
        customer_ordered_window = Window.partitionBy("customer_id").orderBy("timestamp")
        
        result_df = result_df.withColumn(
            "prev_payment_method",
            F.lag("payment_method", 1).over(customer_ordered_window)
        ).withColumn(
            "payment_method_changed",
            F.col("payment_method") != F.col("prev_payment_method")
        )
        
        return result_df
    
    def create_comprehensive_patterns(self, df: DataFrame) -> DataFrame:
        """
        Create comprehensive pattern analysis combining all pattern types.
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with all pattern analysis features
        """
        # Apply all pattern analyses
        result_df = df
        result_df = self.analyze_velocity_patterns(result_df)
        result_df = self.analyze_amount_patterns(result_df)
        result_df = self.analyze_time_patterns(result_df)
        
        # Only analyze geographic and merchant patterns if columns exist
        if "location" in df.columns:
            result_df = self.analyze_geographic_patterns(result_df)
        
        if "merchant_id" in df.columns:
            result_df = self.analyze_merchant_patterns(result_df)
        
        result_df = self.analyze_payment_patterns(result_df)
        
        return result_df
    
    def _parse_window_seconds(self, window_str: str) -> int:
        """Parse window string to seconds."""
        parts = window_str.split()
        if len(parts) != 2:
            raise ValueError(f"Invalid window format: {window_str}")
        
        value, unit = int(parts[0]), parts[1].lower()
        
        if unit in ["second", "seconds"]:
            return value
        elif unit in ["minute", "minutes"]:
            return value * 60
        elif unit in ["hour", "hours"]:
            return value * 3600
        elif unit in ["day", "days"]:
            return value * 86400
        elif unit in ["week", "weeks"]:
            return value * 604800
        else:
            raise ValueError(f"Unsupported time unit: {unit}")
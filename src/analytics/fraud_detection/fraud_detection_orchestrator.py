"""
Fraud detection orchestrator for real-time integration and coordination.
"""

from typing import Dict, List, Optional, Callable
import json
import logging
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType

from .rules_engine import ConfigurableRulesEngine, FraudRule
from .pattern_analyzer import TransactionPatternAnalyzer
from .merchant_risk_scorer import MerchantRiskScorer
from .alert_prioritizer import FraudAlertPrioritizer, AlertPriority


class FraudDetectionOrchestrator:
    """
    Main orchestrator for real-time fraud detection system.
    
    Coordinates all fraud detection components:
    - Rules engine for business rule evaluation
    - Pattern analyzer for behavioral analysis
    - Merchant risk scorer for merchant evaluation
    - Alert prioritizer for intelligent alert management
    - Real-time streaming processing
    - Batch model updates and maintenance
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """
        Initialize the fraud detection orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Optional configuration dictionary
        """
        self.spark = spark
        self.config = config or self._get_default_config()
        
        # Initialize components
        self.rules_engine = ConfigurableRulesEngine(spark, self.config.get('rules_config_path'))
        self.pattern_analyzer = TransactionPatternAnalyzer(spark)
        self.merchant_risk_scorer = MerchantRiskScorer(spark)
        self.alert_prioritizer = FraudAlertPrioritizer(spark)
        
        # Streaming queries registry
        self.active_streams: Dict[str, StreamingQuery] = {}
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def _get_default_config(self) -> Dict:
        """Get default configuration for fraud detection system."""
        return {
            'stream_source_path': 'data/delta/transactions',
            'checkpoint_location': 'data/checkpoints/fraud_detection',
            'output_path': 'data/delta/fraud_alerts',
            'batch_interval': '30 seconds',
            'watermark_delay': '10 minutes',
            'rules_config_path': None,
            'enable_pattern_analysis': True,
            'enable_merchant_scoring': True,
            'enable_alert_prioritization': True,
            'alert_thresholds': {
                'critical_priority_action': 'immediate_block',
                'high_priority_action': 'manual_review',
                'medium_priority_action': 'queue_review',
                'low_priority_action': 'batch_review'
            },
            'model_update_interval': '1 hour'
        }
    
    def start_real_time_detection(self, 
                                stream_source: Optional[DataFrame] = None,
                                output_sink: str = "console") -> StreamingQuery:
        """
        Start real-time fraud detection pipeline.
        
        Args:
            stream_source: Optional custom stream source DataFrame
            output_sink: Output sink type ('console', 'delta', 'kafka')
            
        Returns:
            StreamingQuery object
        """
        # Get stream source
        if stream_source is None:
            stream_df = self._get_transaction_stream()
        else:
            stream_df = stream_source
        
        # Apply fraud detection pipeline
        fraud_results_df = self._apply_fraud_detection_pipeline(stream_df)
        
        # Configure output sink
        output_query = self._configure_output_sink(fraud_results_df, output_sink)
        
        # Start the stream
        query_name = f"fraud_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        streaming_query = output_query.queryName(query_name).start()
        
        # Register the stream
        self.active_streams[query_name] = streaming_query
        
        self.logger.info(f"Started real-time fraud detection stream: {query_name}")
        return streaming_query
    
    def _get_transaction_stream(self) -> DataFrame:
        """Get transaction stream from configured source."""
        return (self.spark.readStream
                .format("delta")
                .option("maxFilesPerTrigger", "1")
                .load(self.config['stream_source_path'])
                .withWatermark("timestamp", self.config['watermark_delay']))
    
    def _apply_fraud_detection_pipeline(self, stream_df: DataFrame) -> DataFrame:
        """
        Apply complete fraud detection pipeline to streaming data.
        
        Args:
            stream_df: Input streaming DataFrame
            
        Returns:
            DataFrame with fraud detection results
        """
        # Start with the transaction stream
        result_df = stream_df
        
        # Apply pattern analysis if enabled
        if self.config.get('enable_pattern_analysis', True):
            result_df = self.pattern_analyzer.create_comprehensive_patterns(result_df)
        
        # Apply rules engine
        result_df = self.rules_engine.apply_rules(result_df)
        
        # Add merchant risk scoring if enabled
        if self.config.get('enable_merchant_scoring', True):
            # For streaming, we'll use pre-computed merchant risk scores
            # that are updated periodically in batch mode
            merchant_risks = self._load_merchant_risk_scores()
            if merchant_risks:
                result_df = result_df.join(
                    merchant_risks.select("merchant_id", "merchant_risk_score", "merchant_risk_level"),
                    "merchant_id",
                    "left"
                ).fillna({"merchant_risk_score": 0.5, "merchant_risk_level": "MEDIUM"})
        
        # Filter to transactions with fraud alerts
        alerts_df = result_df.filter(F.col("fraud_score") > 0)
        
        # Apply alert prioritization if enabled
        if self.config.get('enable_alert_prioritization', True):
            customer_risks = self._load_customer_risk_scores()
            merchant_risks = self._load_merchant_risk_scores()
            alerts_df = self.alert_prioritizer.prioritize_alerts(
                alerts_df, customer_risks, merchant_risks
            )
        
        # Add processing metadata
        alerts_df = alerts_df.withColumn("processing_timestamp", F.current_timestamp())\
                           .withColumn("detection_version", F.lit("1.0.0"))\
                           .withColumn("alert_id", F.concat(
                               F.lit("alert_"),
                               F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                               F.lit("_"),
                               F.col("transaction_id")
                           ))
        
        return alerts_df
    
    def _load_merchant_risk_scores(self) -> Optional[DataFrame]:
        """Load pre-computed merchant risk scores."""
        try:
            return self.spark.read.format("delta").load("data/delta/merchant_risk_scores")
        except Exception as e:
            self.logger.warning(f"Could not load merchant risk scores: {e}")
            return None
    
    def _load_customer_risk_scores(self) -> Optional[DataFrame]:
        """Load pre-computed customer risk scores."""
        try:
            return self.spark.read.format("delta").load("data/delta/customer_risk_scores")
        except Exception as e:
            self.logger.warning(f"Could not load customer risk scores: {e}")
            return None
    
    def _configure_output_sink(self, df: DataFrame, sink_type: str):
        """Configure output sink for streaming results."""
        base_query = df.writeStream.outputMode("append")\
                       .trigger(processingTime=self.config['batch_interval'])\
                       .option("checkpointLocation", self.config['checkpoint_location'])
        
        if sink_type == "console":
            return base_query.format("console").option("truncate", False)
        elif sink_type == "delta":
            return base_query.format("delta").option("path", self.config['output_path'])
        elif sink_type == "kafka":
            return base_query.format("kafka")\
                            .option("kafka.bootstrap.servers", self.config.get('kafka_servers', 'localhost:9092'))\
                            .option("topic", self.config.get('kafka_topic', 'fraud-alerts'))
        else:
            raise ValueError(f"Unsupported sink type: {sink_type}")
    
    def process_batch_updates(self) -> None:
        """
        Process batch updates for models and risk scores.
        
        This should be run periodically to update:
        - Merchant risk scores
        - Customer risk profiles
        - Rule performance metrics
        - Model retraining
        """
        self.logger.info("Starting batch updates...")
        
        try:
            # Update merchant risk scores
            self._update_merchant_risk_scores()
            
            # Update customer risk profiles
            self._update_customer_risk_profiles()
            
            # Update rule performance metrics
            self._update_rule_performance_metrics()
            
            self.logger.info("Batch updates completed successfully")
            
        except Exception as e:
            self.logger.error(f"Batch updates failed: {e}")
            raise
    
    def _update_merchant_risk_scores(self) -> None:
        """Update merchant risk scores based on recent transaction data."""
        # Get recent transactions (last 30 days)
        cutoff_date = datetime.now() - timedelta(days=30)
        
        recent_transactions = self.spark.read.format("delta")\
                                   .load(self.config['stream_source_path'])\
                                   .filter(F.col("timestamp") >= F.lit(cutoff_date))
        
        if recent_transactions.count() > 0:
            # Calculate updated merchant risk scores
            merchant_scores = self.merchant_risk_scorer.calculate_merchant_risk_scores(
                recent_transactions, lookback_days=30
            )
            
            # Save updated scores
            merchant_scores.write.mode("overwrite")\
                          .format("delta")\
                          .save("data/delta/merchant_risk_scores")
            
            self.logger.info(f"Updated merchant risk scores for {merchant_scores.count()} merchants")
    
    def _update_customer_risk_profiles(self) -> None:
        """Update customer risk profiles based on recent activity."""
        # This is a placeholder for customer risk profiling
        # In a real implementation, this would analyze customer behavior patterns
        self.logger.info("Customer risk profile updates - placeholder")
    
    def _update_rule_performance_metrics(self) -> None:
        """Update rule performance metrics based on historical alert resolutions."""
        # This would analyze historical alert outcomes to update rule accuracy
        self.logger.info("Rule performance metrics updates - placeholder")
    
    def get_detection_metrics(self, time_window: str = "1 hour") -> Dict:
        """
        Get fraud detection metrics for the specified time window.
        
        Args:
            time_window: Time window for metrics (e.g., "1 hour", "1 day")
            
        Returns:
            Dictionary with detection metrics
        """
        try:
            # Read recent fraud alerts
            alerts_df = self.spark.read.format("delta")\
                                .load(self.config['output_path'])\
                                .filter(
                                    F.col("processing_timestamp") >= 
                                    F.expr(f"current_timestamp() - interval {time_window}")
                                )
            
            if alerts_df.count() == 0:
                return {"message": "No fraud alerts in the specified time window"}
            
            # Calculate metrics
            metrics = alerts_df.agg(
                F.count("*").alias("total_alerts"),
                F.sum("price").alias("total_financial_exposure"),
                F.avg("fraud_score").alias("avg_fraud_score"),
                F.avg("priority_score").alias("avg_priority_score")
            ).collect()[0]
            
            # Priority breakdown
            priority_breakdown = alerts_df.groupBy("alert_priority")\
                                        .agg(F.count("*").alias("count"))\
                                        .collect()
            
            # Top triggered rules
            top_rules = alerts_df.select(F.explode("triggered_rules").alias("rule"))\
                               .groupBy("rule")\
                               .agg(F.count("*").alias("count"))\
                               .orderBy(F.col("count").desc())\
                               .limit(5).collect()
            
            return {
                "time_window": time_window,
                "total_alerts": metrics["total_alerts"],
                "total_financial_exposure": float(metrics["total_financial_exposure"]) if metrics["total_financial_exposure"] else 0.0,
                "avg_fraud_score": float(metrics["avg_fraud_score"]) if metrics["avg_fraud_score"] else 0.0,
                "avg_priority_score": float(metrics["avg_priority_score"]) if metrics["avg_priority_score"] else 0.0,
                "priority_breakdown": {row["alert_priority"]: row["count"] for row in priority_breakdown},
                "top_triggered_rules": [{"rule": row["rule"], "count": row["count"]} for row in top_rules]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting detection metrics: {e}")
            return {"error": str(e)}
    
    def stop_all_streams(self) -> None:
        """Stop all active streaming queries."""
        for query_name, query in self.active_streams.items():
            try:
                query.stop()
                self.logger.info(f"Stopped stream: {query_name}")
            except Exception as e:
                self.logger.error(f"Error stopping stream {query_name}: {e}")
        
        self.active_streams.clear()
    
    def add_custom_rule(self, rule: FraudRule) -> None:
        """Add a custom fraud detection rule."""
        self.rules_engine.add_rule(rule)
        self.logger.info(f"Added custom rule: {rule.name}")
    
    def update_configuration(self, new_config: Dict) -> None:
        """Update orchestrator configuration."""
        self.config.update(new_config)
        self.logger.info("Configuration updated")
    
    def get_system_status(self) -> Dict:
        """Get system status information."""
        return {
            "active_streams": list(self.active_streams.keys()),
            "total_rules": len(self.rules_engine.rules),
            "enabled_rules": len(self.rules_engine.get_enabled_rules()),
            "configuration": self.config,
            "last_batch_update": datetime.now().isoformat()  # Placeholder
        }
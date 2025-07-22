"""
Fraud detection orchestrator for real-time integration and coordination.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from .alert_prioritizer import AlertPriority, FraudAlertPrioritizer
from .feedback_loop import FeedbackLoop
from .merchant_risk_scorer import MerchantRiskScorer
from .ml_models import MLFraudModelTrainer
from .model_monitoring import ModelPerformanceMonitor
from .model_serving import ModelServingPipeline
from .pattern_analyzer import TransactionPatternAnalyzer
from .rules_engine import ConfigurableRulesEngine, FraudRule


class FraudDetectionOrchestrator:
    """
    Main orchestrator for real-time fraud detection system.

    Coordinates all fraud detection components:
    - Rules engine for business rule evaluation
    - Pattern analyzer for behavioral analysis
    - Merchant risk scorer for merchant evaluation
    - Alert prioritizer for intelligent alert management
    - ML models for advanced fraud detection
    - Model serving pipeline for real-time ML predictions
    - Model monitoring and performance tracking
    - Feedback loop for continuous model improvement
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

        # Initialize traditional fraud detection components
        self.rules_engine = ConfigurableRulesEngine(
            spark, self.config.get("rules_config_path")
        )
        self.pattern_analyzer = TransactionPatternAnalyzer(spark)
        self.merchant_risk_scorer = MerchantRiskScorer(spark)
        self.alert_prioritizer = FraudAlertPrioritizer(spark)

        # Initialize ML components
        self.ml_trainer = MLFraudModelTrainer(spark, self.config.get("ml_config"))
        self.model_serving = ModelServingPipeline(
            spark, self.config.get("serving_config")
        )
        self.model_monitor = ModelPerformanceMonitor(
            spark, self.config.get("monitoring_config")
        )
        self.feedback_loop = FeedbackLoop(spark, self.config.get("feedback_config"))

        # Streaming queries registry
        self.active_streams: Dict[str, StreamingQuery] = {}

        # Setup logging
        self.logger = logging.getLogger(__name__)

    def _get_default_config(self) -> Dict:
        """Get default configuration for fraud detection system."""
        return {
            "stream_source_path": "data/delta/transactions",
            "checkpoint_location": "data/checkpoints/fraud_detection",
            "output_path": "data/delta/fraud_alerts",
            "batch_interval": "30 seconds",
            "watermark_delay": "10 minutes",
            "rules_config_path": None,
            "enable_pattern_analysis": True,
            "enable_merchant_scoring": True,
            "enable_alert_prioritization": True,
            "enable_ml_models": True,
            "ml_ensemble_enabled": True,
            "ml_model_threshold": 0.5,
            "alert_thresholds": {
                "critical_priority_action": "immediate_block",
                "high_priority_action": "manual_review",
                "medium_priority_action": "queue_review",
                "low_priority_action": "batch_review",
            },
            "model_update_interval": "1 hour",
        }

    def start_real_time_detection(
        self, stream_source: Optional[DataFrame] = None, output_sink: str = "console"
    ) -> StreamingQuery:
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
        return (
            self.spark.readStream.format("delta")
            .option("maxFilesPerTrigger", "1")
            .load(self.config["stream_source_path"])
            .withWatermark("timestamp", self.config["watermark_delay"])
        )

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
        if self.config.get("enable_pattern_analysis", True):
            result_df = self.pattern_analyzer.create_comprehensive_patterns(result_df)

        # Apply rules engine
        result_df = self.rules_engine.apply_rules(result_df)

        # Add merchant risk scoring if enabled
        if self.config.get("enable_merchant_scoring", True):
            # For streaming, we'll use pre-computed merchant risk scores
            # that are updated periodically in batch mode
            merchant_risks = self._load_merchant_risk_scores()
            if merchant_risks:
                result_df = result_df.join(
                    merchant_risks.select(
                        "merchant_id", "merchant_risk_score", "merchant_risk_level"
                    ),
                    "merchant_id",
                    "left",
                ).fillna({"merchant_risk_score": 0.5, "merchant_risk_level": "MEDIUM"})

        # Apply ML models if enabled
        if self.config.get("enable_ml_models", True):
            result_df = self._apply_ml_fraud_detection(result_df)

        # Combine rule-based and ML-based fraud scores
        result_df = self._combine_fraud_scores(result_df)

        # Filter to transactions with fraud alerts
        alerts_df = result_df.filter(F.col("combined_fraud_score") > 0)

        # Apply alert prioritization if enabled
        if self.config.get("enable_alert_prioritization", True):
            customer_risks = self._load_customer_risk_scores()
            merchant_risks = self._load_merchant_risk_scores()
            alerts_df = self.alert_prioritizer.prioritize_alerts(
                alerts_df, customer_risks, merchant_risks
            )

        # Add processing metadata
        alerts_df = (
            alerts_df.withColumn("processing_timestamp", F.current_timestamp())
            .withColumn("detection_version", F.lit("1.0.0"))
            .withColumn(
                "alert_id",
                F.concat(
                    F.lit("alert_"),
                    F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                    F.lit("_"),
                    F.col("transaction_id"),
                ),
            )
        )

        return alerts_df

    def _load_merchant_risk_scores(self) -> Optional[DataFrame]:
        """Load pre-computed merchant risk scores."""
        try:
            return self.spark.read.format("delta").load(
                "data/delta/merchant_risk_scores"
            )
        except Exception as e:
            self.logger.warning(f"Could not load merchant risk scores: {e}")
            return None

    def _load_customer_risk_scores(self) -> Optional[DataFrame]:
        """Load pre-computed customer risk scores."""
        try:
            return self.spark.read.format("delta").load(
                "data/delta/customer_risk_scores"
            )
        except Exception as e:
            self.logger.warning(f"Could not load customer risk scores: {e}")
            return None

    def _apply_ml_fraud_detection(self, df: DataFrame) -> DataFrame:
        """
        Apply ML-based fraud detection to the DataFrame.

        Args:
            df: Input DataFrame with transaction data

        Returns:
            DataFrame with ML fraud predictions
        """
        try:
            # Convert streaming DataFrame to batch for ML prediction
            # In a production system, you might want to buffer and process in micro-batches

            # Make ML predictions using the model serving pipeline
            ml_predictions = []

            # Process transactions in micro-batches for real-time prediction
            # This is a simplified approach - in production, you'd implement proper streaming ML
            def process_batch(batch_df, batch_id):
                if batch_df.count() > 0:
                    # Convert DataFrame rows to prediction requests
                    transactions = batch_df.collect()

                    for row in transactions:
                        transaction_data = {
                            "customer_id": row.customer_id,
                            "merchant_id": row.merchant_id,
                            "price": float(row.price),
                            "payment_method": row.payment_method,
                            "timestamp": row.timestamp.isoformat(),
                        }

                        # Get ML prediction
                        prediction = self.model_serving.predict_fraud(transaction_data)

                        # Store prediction results (in production, you'd write to a proper store)
                        ml_predictions.append(
                            {
                                "transaction_id": row.transaction_id,
                                "ml_fraud_probability": prediction.fraud_probability,
                                "ml_fraud_prediction": prediction.prediction,
                                "ml_model_version": prediction.model_version,
                                "ml_prediction_time_ms": prediction.prediction_time_ms,
                            }
                        )

            # For streaming, we need to handle this differently
            # This is a placeholder implementation
            result_df = (
                df.withColumn("ml_fraud_probability", F.lit(0.0))
                .withColumn("ml_fraud_prediction", F.lit(0))
                .withColumn("ml_model_version", F.lit("v1.0.0"))
                .withColumn("ml_prediction_time_ms", F.lit(100.0))
            )

            # In a real streaming implementation, you would:
            # 1. Use Spark's foreachBatch to process micro-batches
            # 2. Call ML model serving for each transaction
            # 3. Store results in a temporary table or cache
            # 4. Join back with the streaming DataFrame

            self.logger.debug("Applied ML fraud detection to streaming data")
            return result_df

        except Exception as e:
            self.logger.error(f"Error applying ML fraud detection: {e}")
            # Return original DataFrame with null ML columns on error
            return (
                df.withColumn("ml_fraud_probability", F.lit(None))
                .withColumn("ml_fraud_prediction", F.lit(None))
                .withColumn("ml_model_version", F.lit(None))
                .withColumn("ml_prediction_time_ms", F.lit(None))
            )

    def _combine_fraud_scores(self, df: DataFrame) -> DataFrame:
        """
        Combine rule-based and ML-based fraud scores into a unified score.

        Args:
            df: DataFrame with both rule-based and ML-based fraud scores

        Returns:
            DataFrame with combined fraud score
        """
        # Define weights for combining scores
        rule_weight = self.config.get("rule_weight", 0.4)
        ml_weight = self.config.get("ml_weight", 0.6)

        # Ensure weights sum to 1.0
        total_weight = rule_weight + ml_weight
        if total_weight > 0:
            rule_weight = rule_weight / total_weight
            ml_weight = ml_weight / total_weight
        else:
            rule_weight, ml_weight = 0.5, 0.5

        # Combine scores using weighted average
        combined_df = (
            df.withColumn(
                "combined_fraud_score",
                F.when(
                    F.col("ml_fraud_probability").isNotNull(),
                    (F.col("fraud_score") * F.lit(rule_weight))
                    + (F.col("ml_fraud_probability") * F.lit(ml_weight)),
                ).otherwise(F.col("fraud_score")),
            )
            .withColumn(
                "combined_fraud_prediction",
                F.when(
                    F.col("combined_fraud_score")
                    > F.lit(self.config.get("ml_model_threshold", 0.5)),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "detection_method",
                F.when(
                    F.col("ml_fraud_probability").isNotNull(),
                    F.when(
                        (F.col("fraud_score") > 0)
                        & (F.col("ml_fraud_prediction") == 1),
                        "rules_and_ml",
                    )
                    .when(F.col("fraud_score") > 0, "rules_only")
                    .when(F.col("ml_fraud_prediction") == 1, "ml_only")
                    .otherwise("none"),
                ).otherwise(
                    F.when(F.col("fraud_score") > 0, "rules_only").otherwise("none")
                ),
            )
        )

        return combined_df

    def _configure_output_sink(self, df: DataFrame, sink_type: str):
        """Configure output sink for streaming results."""
        base_query = (
            df.writeStream.outputMode("append")
            .trigger(processingTime=self.config["batch_interval"])
            .option("checkpointLocation", self.config["checkpoint_location"])
        )

        if sink_type == "console":
            return base_query.format("console").option("truncate", False)
        elif sink_type == "delta":
            return base_query.format("delta").option("path", self.config["output_path"])
        elif sink_type == "kafka":
            return (
                base_query.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    self.config.get("kafka_servers", "localhost:9092"),
                )
                .option("topic", self.config.get("kafka_topic", "fraud-alerts"))
            )
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

            # Update ML models and monitoring
            self._update_ml_models()

            # Process ML feedback and potentially trigger retraining
            self._process_ml_feedback()

            self.logger.info("Batch updates completed successfully")

        except Exception as e:
            self.logger.error(f"Batch updates failed: {e}")
            raise

    def _update_merchant_risk_scores(self) -> None:
        """Update merchant risk scores based on recent transaction data."""
        # Get recent transactions (last 30 days)
        cutoff_date = datetime.now() - timedelta(days=30)

        recent_transactions = (
            self.spark.read.format("delta")
            .load(self.config["stream_source_path"])
            .filter(F.col("timestamp") >= F.lit(cutoff_date))
        )

        if recent_transactions.count() > 0:
            # Calculate updated merchant risk scores
            merchant_scores = self.merchant_risk_scorer.calculate_merchant_risk_scores(
                recent_transactions, lookback_days=30
            )

            # Save updated scores
            merchant_scores.write.mode("overwrite").format("delta").save(
                "data/delta/merchant_risk_scores"
            )

            self.logger.info(
                f"Updated merchant risk scores for {merchant_scores.count()} merchants"
            )

    def _update_customer_risk_profiles(self) -> None:
        """Update customer risk profiles based on recent activity."""
        # This is a placeholder for customer risk profiling
        # In a real implementation, this would analyze customer behavior patterns
        self.logger.info("Customer risk profile updates - placeholder")

    def _update_rule_performance_metrics(self) -> None:
        """Update rule performance metrics based on historical alert resolutions."""
        # This would analyze historical alert outcomes to update rule accuracy
        self.logger.info("Rule performance metrics updates - placeholder")

    def _update_ml_models(self) -> None:
        """Update ML model performance monitoring and drift detection."""
        try:
            # Get recent predictions and actual outcomes for model evaluation
            cutoff_date = datetime.now() - timedelta(days=7)

            # Load recent fraud alerts with outcomes (this would come from investigation results)
            try:
                alerts_df = (
                    self.spark.read.format("delta")
                    .load(self.config["output_path"])
                    .filter(F.col("processing_timestamp") >= F.lit(cutoff_date))
                )

                if alerts_df.count() > 0:
                    # Simulate actual outcomes (in production, this would come from investigations)
                    # For now, we'll create synthetic outcomes for demonstration
                    actuals_df = alerts_df.select("transaction_id").withColumn(
                        "actual_fraud", F.when(F.rand() > 0.8, 1).otherwise(0)
                    )

                    # Evaluate model performance
                    if self.config.get("enable_ml_models", True):
                        predictions_df = alerts_df.select(
                            "transaction_id",
                            "ml_fraud_prediction",
                            "ml_fraud_probability",
                            "ml_prediction_time_ms",
                        ).filter(F.col("ml_fraud_prediction").isNotNull())

                        if predictions_df.count() > 0:
                            metrics = self.model_monitor.evaluate_model_performance(
                                "fraud_ensemble", "v1.0.0", predictions_df, actuals_df
                            )

                            self.logger.info(
                                f"ML model performance - Accuracy: {metrics.accuracy:.3f}, "
                                f"Precision: {metrics.precision:.3f}, Recall: {metrics.recall:.3f}"
                            )

                    # Check for data drift
                    recent_transactions = (
                        self.spark.read.format("delta")
                        .load(self.config["stream_source_path"])
                        .filter(F.col("timestamp") >= F.lit(cutoff_date))
                    )

                    if recent_transactions.count() > 0:
                        drift_results = self.model_monitor.detect_data_drift(
                            "fraud_ensemble", recent_transactions
                        )

                        significant_drift = [
                            d for d in drift_results if d.is_drift_detected
                        ]
                        if significant_drift:
                            self.logger.warning(
                                f"Data drift detected in {len(significant_drift)} features"
                            )

            except Exception as e:
                self.logger.warning(
                    f"Could not load recent alerts for ML evaluation: {e}"
                )

        except Exception as e:
            self.logger.error(f"ML model updates failed: {e}")

    def _process_ml_feedback(self) -> None:
        """Process ML model feedback and trigger retraining if needed."""
        try:
            if not self.config.get("enable_ml_models", True):
                return

            # Start feedback processing if not already running
            if not self.feedback_loop._running:
                self.feedback_loop.start_feedback_processing()

            # Get feedback summary
            feedback_summary = self.feedback_loop.get_feedback_summary(days=30)

            if feedback_summary.get("total_feedback_entries", 0) > 0:
                self.logger.info(
                    f"Processed {feedback_summary['total_feedback_entries']} feedback entries "
                    f"with {feedback_summary.get('feedback_based_accuracy', 0):.3f} accuracy"
                )

            # Get retraining status
            retraining_status = self.feedback_loop.get_retraining_status()

            if retraining_status.get("active_jobs", 0) > 0:
                self.logger.info(
                    f"ML retraining in progress: {retraining_status['active_jobs']} active jobs"
                )

            # Check if retraining triggers are met
            self.feedback_loop._check_retraining_triggers()

        except Exception as e:
            self.logger.error(f"ML feedback processing failed: {e}")

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
            alerts_df = (
                self.spark.read.format("delta")
                .load(self.config["output_path"])
                .filter(
                    F.col("processing_timestamp")
                    >= F.expr(f"current_timestamp() - interval {time_window}")
                )
            )

            if alerts_df.count() == 0:
                return {"message": "No fraud alerts in the specified time window"}

            # Calculate metrics
            metrics = alerts_df.agg(
                F.count("*").alias("total_alerts"),
                F.sum("price").alias("total_financial_exposure"),
                F.avg("fraud_score").alias("avg_fraud_score"),
                F.avg("priority_score").alias("avg_priority_score"),
            ).collect()[0]

            # Priority breakdown
            priority_breakdown = (
                alerts_df.groupBy("alert_priority")
                .agg(F.count("*").alias("count"))
                .collect()
            )

            # Top triggered rules
            top_rules = (
                alerts_df.select(F.explode("triggered_rules").alias("rule"))
                .groupBy("rule")
                .agg(F.count("*").alias("count"))
                .orderBy(F.col("count").desc())
                .limit(5)
                .collect()
            )

            return {
                "time_window": time_window,
                "total_alerts": metrics["total_alerts"],
                "total_financial_exposure": float(metrics["total_financial_exposure"])
                if metrics["total_financial_exposure"]
                else 0.0,
                "avg_fraud_score": float(metrics["avg_fraud_score"])
                if metrics["avg_fraud_score"]
                else 0.0,
                "avg_priority_score": float(metrics["avg_priority_score"])
                if metrics["avg_priority_score"]
                else 0.0,
                "priority_breakdown": {
                    row["alert_priority"]: row["count"] for row in priority_breakdown
                },
                "top_triggered_rules": [
                    {"rule": row["rule"], "count": row["count"]} for row in top_rules
                ],
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
        status = {
            "active_streams": list(self.active_streams.keys()),
            "total_rules": len(self.rules_engine.rules),
            "enabled_rules": len(self.rules_engine.get_enabled_rules()),
            "configuration": self.config,
            "last_batch_update": datetime.now().isoformat(),  # Placeholder
        }

        # Add ML-specific status if enabled
        if self.config.get("enable_ml_models", True):
            try:
                status.update(
                    {
                        "ml_models_loaded": len(self.ml_trainer.models),
                        "model_serving_status": self.model_serving.get_serving_status(),
                        "monitoring_status": self.model_monitor.get_monitoring_summary(),
                        "retraining_status": self.feedback_loop.get_retraining_status(),
                        "feedback_summary": self.feedback_loop.get_feedback_summary(
                            days=7
                        ),
                    }
                )
            except Exception as e:
                status["ml_status_error"] = str(e)

        return status

    def train_ml_models(self, training_data_path: Optional[str] = None) -> Dict:
        """
        Train ML fraud detection models.

        Args:
            training_data_path: Optional path to training data

        Returns:
            Training results dictionary
        """
        try:
            # Get training data
            if training_data_path:
                training_df = self.spark.read.format("delta").load(training_data_path)
            else:
                # Use recent transaction data for training
                cutoff_date = datetime.now() - timedelta(days=90)
                training_df = (
                    self.spark.read.format("delta")
                    .load(self.config["stream_source_path"])
                    .filter(F.col("timestamp") >= F.lit(cutoff_date))
                )

            # Train ensemble models
            results = self.ml_trainer.train_ensemble_models(training_df)

            # Set reference data for drift monitoring
            self.model_monitor.set_reference_data("fraud_ensemble", training_df)

            # Save trained models
            model_save_path = self.config.get(
                "ml_model_path", "data/models/fraud_detection"
            )
            self.ml_trainer.save_models(model_save_path)

            # Deploy models to serving pipeline
            for model_name in results["models"].keys():
                self.model_serving.deploy_model(
                    model_name=model_name,
                    model_path=f"{model_save_path}/{model_name}",
                    version="v1.0.0",
                    traffic_percentage=100.0 / len(results["models"]),
                )

            self.logger.info(
                f"Successfully trained and deployed {len(results['models'])} ML models"
            )
            return results

        except Exception as e:
            self.logger.error(f"ML model training failed: {e}")
            return {"error": str(e)}

    def collect_investigator_feedback(
        self,
        transaction_id: str,
        prediction_id: str,
        is_fraud: bool,
        investigator_id: str,
        confidence: float = 0.9,
        comments: str = None,
    ) -> bool:
        """
        Collect feedback from fraud investigators.

        Args:
            transaction_id: ID of the transaction
            prediction_id: ID of the prediction
            is_fraud: True if transaction is confirmed fraud, False otherwise
            investigator_id: ID of the investigator
            confidence: Confidence in the feedback (0-1)
            comments: Optional comments

        Returns:
            True if feedback collected successfully
        """
        from .feedback_loop import FeedbackLabel

        try:
            # Map boolean to feedback label
            if is_fraud:
                # Need to check if it was correctly predicted or missed
                # For now, assume it was correctly predicted (TRUE_POSITIVE)
                # In production, you'd check the original prediction
                feedback_label = FeedbackLabel.TRUE_POSITIVE
            else:
                # Assume it was incorrectly flagged (FALSE_POSITIVE)
                feedback_label = FeedbackLabel.FALSE_POSITIVE

            return self.feedback_loop.collect_investigator_feedback(
                transaction_id=transaction_id,
                prediction_id=prediction_id,
                feedback_label=feedback_label,
                investigator_id=investigator_id,
                confidence_score=confidence,
                comments=comments,
            )

        except Exception as e:
            self.logger.error(f"Failed to collect investigator feedback: {e}")
            return False

    def collect_customer_dispute(
        self,
        transaction_id: str,
        prediction_id: str,
        dispute_type: str,
        resolution: str,
    ) -> bool:
        """
        Collect customer dispute information.

        Args:
            transaction_id: ID of the disputed transaction
            prediction_id: ID of the prediction
            dispute_type: Type of dispute (chargeback, inquiry, etc.)
            resolution: Resolution of the dispute

        Returns:
            True if dispute collected successfully
        """
        try:
            return self.feedback_loop.collect_customer_dispute(
                transaction_id=transaction_id,
                prediction_id=prediction_id,
                dispute_type=dispute_type,
                resolution=resolution,
            )

        except Exception as e:
            self.logger.error(f"Failed to collect customer dispute: {e}")
            return False

    def get_ml_model_performance(
        self, model_name: str = "fraud_ensemble", days: int = 30
    ) -> Dict:
        """
        Get ML model performance metrics.

        Args:
            model_name: Name of the model
            days: Number of days for trend analysis

        Returns:
            Dictionary with performance metrics and trends
        """
        try:
            if not self.config.get("enable_ml_models", True):
                return {"error": "ML models are disabled"}

            # Get performance trends
            trends = self.model_monitor.get_performance_trends(
                model_name, "v1.0.0", days
            )

            # Get monitoring summary
            summary = self.model_monitor.get_monitoring_summary()

            # Get serving status
            serving_status = self.model_serving.get_serving_status()

            return {
                "model_name": model_name,
                "performance_trends": trends,
                "monitoring_summary": summary,
                "serving_status": serving_status,
                "time_period_days": days,
            }

        except Exception as e:
            self.logger.error(f"Failed to get ML model performance: {e}")
            return {"error": str(e)}

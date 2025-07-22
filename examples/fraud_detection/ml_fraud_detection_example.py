"""
Usage example for ML-based fraud detection system.

This example demonstrates how to use the integrated ML fraud detection system
including model training, serving, monitoring, and feedback collection.
"""

import os
import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from analytics.fraud_detection.feedback_loop import FeedbackLabel, FeedbackLoop
from analytics.fraud_detection.fraud_detection_orchestrator import (
    FraudDetectionOrchestrator,
)
from analytics.fraud_detection.ml_models import MLFraudModelTrainer
from analytics.fraud_detection.model_monitoring import ModelPerformanceMonitor
from analytics.fraud_detection.model_serving import ModelServingPipeline


def create_sample_transaction_data(spark: SparkSession, num_transactions: int = 1000):
    """Create sample transaction data for demonstration."""
    print(f"Creating {num_transactions} sample transactions...")

    # Define schema
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    # Generate sample data
    import random

    from faker import Faker

    fake = Faker()

    data = []
    base_time = datetime.now()

    for i in range(num_transactions):
        # Create some fraud patterns
        is_fraud_pattern = i % 50 == 0  # 2% fraud rate

        if is_fraud_pattern:
            # Fraud patterns: high amounts, unusual hours, repeated merchants
            price = random.uniform(5000, 15000)
            hour_offset = random.choice([-22, -23, -1, -2, -3])  # Unusual hours
            customer_id = (
                f"customer_{random.randint(1, 20)}"  # Fewer customers for velocity
            )
            merchant_id = f"merchant_{random.randint(1, 5)}"  # Fewer merchants
        else:
            # Normal patterns
            price = random.uniform(10, 500)
            hour_offset = random.randint(-16, -6)  # Business hours
            customer_id = f"customer_{random.randint(1, 200)}"
            merchant_id = f"merchant_{random.randint(1, 100)}"

        data.append(
            (
                f"txn_{i}",
                customer_id,
                merchant_id,
                price,
                random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
                base_time + timedelta(hours=hour_offset, minutes=random.randint(0, 59)),
            )
        )

    df = spark.createDataFrame(data, schema)
    print(f"Created {df.count()} transactions")
    return df


def demonstrate_ml_training(spark: SparkSession):
    """Demonstrate ML model training."""
    print("\n" + "=" * 60)
    print("DEMONSTRATING ML MODEL TRAINING")
    print("=" * 60)

    # Create sample data
    training_data = create_sample_transaction_data(spark, 2000)

    # Initialize ML trainer
    trainer = MLFraudModelTrainer(spark)

    # Train models
    print("\nTraining ensemble fraud detection models...")
    training_results = trainer.train_ensemble_models(training_data)

    print("\nTraining Results:")
    print(f"- Models trained: {list(training_results['models'].keys())}")
    print(f"- Training data size: {training_results['training_data_size']}")
    print(f"- Test data size: {training_results['test_data_size']}")
    print(f"- Feature columns: {len(training_results['feature_columns'])}")

    print("\nModel Performance:")
    for model_name, metrics in training_results["metrics"].items():
        print(f"  {model_name}:")
        print(f"    - AUC-ROC: {metrics['auc_roc']:.3f}")
        print(f"    - Accuracy: {metrics['accuracy']:.3f}")

    # Get feature importance
    print("\nTop 10 Most Important Features (Random Forest):")
    feature_importance = trainer.get_feature_importance("random_forest")
    for i, (feature, importance) in enumerate(list(feature_importance.items())[:10]):
        print(f"  {i+1}. {feature}: {importance:.3f}")

    return trainer, training_data


def demonstrate_model_serving(spark: SparkSession, trainer: MLFraudModelTrainer):
    """Demonstrate real-time model serving."""
    print("\n" + "=" * 60)
    print("DEMONSTRATING MODEL SERVING")
    print("=" * 60)

    # Initialize serving pipeline
    serving = ModelServingPipeline(spark)

    # Deploy models (simulate deployment)
    print("\nDeploying models to serving pipeline...")
    for model_name in trainer.models.keys():
        success = serving.deploy_model(
            model_name=model_name,
            model_path=f"data/models/{model_name}",
            version="v1.0.0",
            traffic_percentage=100.0 / len(trainer.models),
        )
        print(f"  {model_name}: {'✅ Deployed' if success else '❌ Failed'}")

    # Start serving
    serving.start_serving()
    print("✅ Model serving started")

    # Test predictions
    print("\nTesting real-time predictions...")
    test_transactions = [
        {
            "customer_id": "customer_123",
            "merchant_id": "merchant_456",
            "price": 75.50,
            "payment_method": "credit_card",
            "timestamp": datetime.now().isoformat(),
        },
        {
            "customer_id": "customer_999",
            "merchant_id": "merchant_001",
            "price": 8500.00,  # High amount - suspicious
            "payment_method": "credit_card",
            "timestamp": (
                datetime.now() - timedelta(hours=3)
            ).isoformat(),  # Unusual hour
        },
        {
            "customer_id": "customer_456",
            "merchant_id": "merchant_789",
            "price": 25.99,
            "payment_method": "debit_card",
            "timestamp": datetime.now().isoformat(),
        },
    ]

    for i, transaction in enumerate(test_transactions, 1):
        print(f"\nTransaction {i}: ${transaction['price']:.2f}")

        # Make prediction
        prediction = serving.predict_fraud(transaction)

        print(f"  Fraud Probability: {prediction.fraud_probability:.3f}")
        print(
            f"  Prediction: {'🚨 FRAUD' if prediction.prediction == 1 else '✅ LEGITIMATE'}"
        )
        print(f"  Model Version: {prediction.model_version}")
        print(f"  Prediction Time: {prediction.prediction_time_ms:.1f}ms")

        if prediction.error:
            print(f"  Error: {prediction.error}")

    # Get serving status
    print("\nServing Status:")
    status = serving.get_serving_status()
    print(f"  Status: {status['status']}")
    print(f"  Total Predictions: {status['metrics']['total_predictions']}")
    print(
        f"  Success Rate: {status['metrics']['successful_predictions'] / max(status['metrics']['total_predictions'], 1) * 100:.1f}%"
    )
    print(f"  Average Latency: {status['metrics']['average_prediction_time_ms']:.1f}ms")

    # Stop serving
    serving.stop_serving()
    print("✅ Model serving stopped")

    return serving


def demonstrate_model_monitoring(spark: SparkSession, training_data):
    """Demonstrate model performance monitoring."""
    print("\n" + "=" * 60)
    print("DEMONSTRATING MODEL MONITORING")
    print("=" * 60)

    # Initialize monitor
    monitor = ModelPerformanceMonitor(spark)

    # Set reference data for drift detection
    print("\nSetting reference data for drift detection...")
    monitor.set_reference_data("fraud_ensemble", training_data)
    print("✅ Reference data set")

    # Create sample predictions data
    print("\nCreating sample predictions for evaluation...")
    predictions_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("ml_fraud_prediction", IntegerType(), True),
            StructField("fraud_probability", DoubleType(), True),
            StructField("prediction_time_ms", DoubleType(), True),
        ]
    )

    # Simulate predictions
    predictions_data = []
    for i in range(200):
        is_fraud = i < 40  # 20% fraud rate in predictions
        predictions_data.append(
            (
                f"txn_{i}",
                1 if is_fraud else 0,
                0.8 if is_fraud else 0.2,
                50.0 + (i % 100),
            )
        )

    predictions_df = spark.createDataFrame(predictions_data, predictions_schema)

    # Create sample actual outcomes
    actuals_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("actual_fraud", IntegerType(), True),
        ]
    )

    # Simulate ground truth with some errors
    actuals_data = []
    for i in range(200):
        actual_fraud = 1 if i < 35 else 0  # 17.5% actual fraud rate (some FP/FN)
        actuals_data.append((f"txn_{i}", actual_fraud))

    actuals_df = spark.createDataFrame(actuals_data, actuals_schema)

    # Evaluate model performance
    print("\nEvaluating model performance...")
    metrics = monitor.evaluate_model_performance(
        "fraud_ensemble", "v1.0.0", predictions_df, actuals_df
    )

    print("Performance Metrics:")
    print(f"  Accuracy: {metrics.accuracy:.3f}")
    print(f"  Precision: {metrics.precision:.3f}")
    print(f"  Recall: {metrics.recall:.3f}")
    print(f"  F1-Score: {metrics.f1_score:.3f}")
    print(f"  AUC-ROC: {metrics.auc_roc:.3f}")
    print(f"  False Positive Rate: {metrics.false_positive_rate:.3f}")
    print(f"  Sample Size: {metrics.sample_size}")
    print(f"  Avg Prediction Latency: {metrics.prediction_latency_ms:.1f}ms")

    # Test data drift detection
    print("\nTesting data drift detection...")

    # Create drifted data (shifted distributions)
    drifted_data = training_data.withColumn(
        "price", F.col("price") + 100.0  # Shift price distribution
    ).withColumn(
        "timestamp", F.col("timestamp") + F.expr("interval 1 hour")  # Shift time
    )

    drift_results = monitor.detect_data_drift("fraud_ensemble", drifted_data)

    print("Data Drift Detection Results:")
    print(f"  Features analyzed: {len(drift_results)}")

    significant_drift = [d for d in drift_results if d.is_drift_detected]
    if significant_drift:
        print(f"  🚨 Drift detected in {len(significant_drift)} features:")
        for drift in significant_drift[:5]:  # Show top 5
            print(
                f"    - {drift.feature_name}: score={drift.drift_score:.3f} ({drift.drift_magnitude})"
            )
    else:
        print("  ✅ No significant drift detected")

    # Get monitoring summary
    summary = monitor.get_monitoring_summary()
    print("\nMonitoring Summary:")
    print(f"  Models monitored: {len(summary['models_monitored'])}")
    print(f"  Total alerts: {summary['total_alerts']}")
    print(f"  Recent alerts (24h): {summary['recent_alerts']}")

    return monitor


def demonstrate_feedback_loop(spark: SparkSession):
    """Demonstrate feedback loop and continuous learning."""
    print("\n" + "=" * 60)
    print("DEMONSTRATING FEEDBACK LOOP")
    print("=" * 60)

    # Initialize feedback loop
    feedback_loop = FeedbackLoop(spark)

    # Start feedback processing
    print("\nStarting feedback processing...")
    feedback_loop.start_feedback_processing()
    print("✅ Feedback processing started")

    # Simulate investigator feedback
    print("\nCollecting investigator feedback...")
    feedback_scenarios = [
        (
            "txn_001",
            "pred_001",
            FeedbackLabel.TRUE_POSITIVE,
            "inv_alice",
            0.95,
            "Clear fraud pattern",
        ),
        (
            "txn_002",
            "pred_002",
            FeedbackLabel.FALSE_POSITIVE,
            "inv_bob",
            0.85,
            "Legitimate high-value purchase",
        ),
        (
            "txn_003",
            "pred_003",
            FeedbackLabel.TRUE_POSITIVE,
            "inv_charlie",
            0.90,
            "Velocity attack detected",
        ),
        (
            "txn_004",
            "pred_004",
            FeedbackLabel.FALSE_POSITIVE,
            "inv_alice",
            0.80,
            "Customer disputed correctly",
        ),
        (
            "txn_005",
            "pred_005",
            FeedbackLabel.TRUE_NEGATIVE,
            "inv_bob",
            0.95,
            "Normal transaction pattern",
        ),
    ]

    for txn_id, pred_id, label, investigator, confidence, comment in feedback_scenarios:
        success = feedback_loop.collect_investigator_feedback(
            transaction_id=txn_id,
            prediction_id=pred_id,
            feedback_label=label,
            investigator_id=investigator,
            confidence_score=confidence,
            comments=comment,
        )
        print(f"  {txn_id}: {'✅' if success else '❌'} {label.value}")

    # Simulate customer disputes
    print("\nCollecting customer disputes...")
    dispute_scenarios = [
        ("txn_101", "pred_101", "chargeback", "fraud_confirmed"),
        ("txn_102", "pred_102", "inquiry", "legitimate"),
        ("txn_103", "pred_103", "dispute", "resolved_customer"),
    ]

    for txn_id, pred_id, dispute_type, resolution in dispute_scenarios:
        success = feedback_loop.collect_customer_dispute(
            transaction_id=txn_id,
            prediction_id=pred_id,
            dispute_type=dispute_type,
            resolution=resolution,
        )
        print(f"  {txn_id}: {'✅' if success else '❌'} {resolution}")

    # Give feedback time to process
    import time

    time.sleep(2)

    # Get feedback summary
    print("\nFeedback Summary:")
    summary = feedback_loop.get_feedback_summary(days=1)

    if "message" in summary:
        print(f"  {summary['message']}")
    else:
        print(f"  Total entries: {summary['total_feedback_entries']}")
        print(f"  Feedback accuracy: {summary['feedback_based_accuracy']:.3f}")
        print(f"  Average confidence: {summary['average_confidence']:.3f}")
        print(f"  Unique investigators: {summary['unique_investigators']}")
        print(f"  Feedback by type: {summary['feedback_by_type']}")
        print(f"  Feedback by label: {summary['feedback_by_label']}")

    # Check retraining status
    print("\nRetraining Status:")
    retraining_status = feedback_loop.get_retraining_status()
    print(f"  Total jobs: {retraining_status['total_jobs']}")
    print(f"  Active jobs: {retraining_status['active_jobs']}")
    print(f"  Completed jobs: {retraining_status['completed_jobs']}")
    print(f"  Failed jobs: {retraining_status['failed_jobs']}")

    # Stop feedback processing
    feedback_loop.stop_feedback_processing()
    print("✅ Feedback processing stopped")

    return feedback_loop


def demonstrate_orchestrator_integration(spark: SparkSession):
    """Demonstrate complete orchestrator integration."""
    print("\n" + "=" * 60)
    print("DEMONSTRATING ORCHESTRATOR INTEGRATION")
    print("=" * 60)

    # Initialize orchestrator with ML enabled
    config = {
        "enable_ml_models": True,
        "ml_ensemble_enabled": True,
        "ml_model_threshold": 0.6,
        "rule_weight": 0.4,
        "ml_weight": 0.6,
        "stream_source_path": "data/delta/transactions",
        "output_path": "data/delta/fraud_alerts",
        "ml_model_path": "data/models/fraud_detection",
    }

    orchestrator = FraudDetectionOrchestrator(spark, config)

    # Train ML models through orchestrator
    print("\nTraining ML models through orchestrator...")
    sample_data = create_sample_transaction_data(spark, 1000)

    # Save sample data to simulate stream source
    sample_data.write.mode("overwrite").format("delta").save(
        config["stream_source_path"]
    )

    # Train models
    training_results = orchestrator.train_ml_models()

    if "error" in training_results:
        print(f"  ❌ Training failed: {training_results['error']}")
    else:
        print(f"  ✅ Successfully trained {len(training_results['models'])} models")
        print(f"  Training data size: {training_results['training_data_size']}")

    # Get system status
    print("\nSystem Status:")
    status = orchestrator.get_system_status()
    print(f"  Active streams: {len(status['active_streams'])}")
    print(f"  Total rules: {status['total_rules']}")
    print(f"  ML models loaded: {status.get('ml_models_loaded', 0)}")

    if "model_serving_status" in status:
        serving_status = status["model_serving_status"]
        print(f"  Model serving status: {serving_status['status']}")
        print(f"  Total predictions: {serving_status['metrics']['total_predictions']}")

    # Test investigator feedback through orchestrator
    print("\nTesting investigator feedback collection...")
    feedback_success = orchestrator.collect_investigator_feedback(
        transaction_id="txn_orchestrator_001",
        prediction_id="pred_orchestrator_001",
        is_fraud=True,
        investigator_id="inv_test",
        confidence=0.9,
        comments="Test feedback through orchestrator",
    )
    print(f"  Feedback collection: {'✅ Success' if feedback_success else '❌ Failed'}")

    # Test customer dispute through orchestrator
    print("\nTesting customer dispute collection...")
    dispute_success = orchestrator.collect_customer_dispute(
        transaction_id="txn_orchestrator_002",
        prediction_id="pred_orchestrator_002",
        dispute_type="chargeback",
        resolution="legitimate",
    )
    print(f"  Dispute collection: {'✅ Success' if dispute_success else '❌ Failed'}")

    # Get ML model performance
    print("\nML Model Performance:")
    performance = orchestrator.get_ml_model_performance()

    if "error" in performance:
        print(f"  ❌ {performance['error']}")
    else:
        print(f"  Model: {performance['model_name']}")
        print(f"  Time period: {performance['time_period_days']} days")

        if performance["performance_trends"]:
            trends = performance["performance_trends"]
            if trends.get("accuracy"):
                print(f"  Latest accuracy: {trends['accuracy'][-1]:.3f}")

        monitoring = performance["monitoring_summary"]
        print(f"  Total alerts: {monitoring['total_alerts']}")

    return orchestrator


def main():
    """Main demonstration function."""
    print("🚀 ML-Based Fraud Detection System Demo")
    print("=" * 60)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("MLFraudDetectionDemo")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Demo 1: ML Model Training
        trainer, training_data = demonstrate_ml_training(spark)

        # Demo 2: Model Serving
        serving = demonstrate_model_serving(spark, trainer)

        # Demo 3: Model Monitoring
        monitor = demonstrate_model_monitoring(spark, training_data)

        # Demo 4: Feedback Loop
        feedback_loop = demonstrate_feedback_loop(spark)

        # Demo 5: Orchestrator Integration
        orchestrator = demonstrate_orchestrator_integration(spark)

        print("\n" + "=" * 60)
        print("🎉 DEMO COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nKey Features Demonstrated:")
        print("✅ ML ensemble model training (Random Forest, GBT, Logistic Regression)")
        print("✅ Real-time model serving with A/B testing")
        print("✅ Model performance monitoring and drift detection")
        print("✅ Feedback loop for continuous learning")
        print("✅ Complete integration with fraud detection orchestrator")
        print("\nNext Steps:")
        print("• Deploy to production environment")
        print("• Set up automated retraining pipelines")
        print("• Integrate with alert management systems")
        print("• Add more sophisticated feature engineering")
        print("• Implement advanced drift detection algorithms")

    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""
Examples demonstrating how to use the fraud detection system.

This file provides practical examples for:
- Setting up fraud detection rules
- Running real-time fraud detection
- Analyzing merchant risk scores
- Managing fraud alerts and prioritization
- Integrating with existing analytics pipelines
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta

from src.analytics.fraud_detection import (
    FraudDetectionOrchestrator,
    ConfigurableRulesEngine,
    FraudRule,
    RuleSeverity,
    RuleType,
    TransactionPatternAnalyzer,
    MerchantRiskScorer,
    FraudAlertPrioritizer,
    AlertPriority
)


def create_spark_session():
    """Create and configure SparkSession for fraud detection."""
    return SparkSession.builder \
        .appName("FraudDetectionExamples") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints") \
        .getOrCreate()


def example_1_basic_fraud_detection():
    """
    Example 1: Basic fraud detection with default rules.
    """
    print("=== Example 1: Basic Fraud Detection ===")
    
    spark = create_spark_session()
    
    # Load transaction data
    transactions_df = spark.read.format("delta").load("data/delta/transactions")
    
    # Initialize orchestrator with default configuration
    orchestrator = FraudDetectionOrchestrator(spark)
    
    # Apply fraud detection to batch data
    pattern_analyzer = TransactionPatternAnalyzer(spark)
    enriched_df = pattern_analyzer.create_comprehensive_patterns(transactions_df)
    
    # Apply rules
    fraud_results = orchestrator.rules_engine.apply_rules(enriched_df)
    
    # Show results
    fraud_alerts = fraud_results.filter(F.col("fraud_score") > 0)
    print(f"Found {fraud_alerts.count()} potential fraud transactions")
    
    fraud_alerts.select("transaction_id", "customer_id", "price", 
                       "fraud_score", "fraud_risk_level", "triggered_rules").show(10)
    
    spark.stop()


def example_2_custom_fraud_rules():
    """
    Example 2: Creating and using custom fraud detection rules.
    """
    print("=== Example 2: Custom Fraud Rules ===")
    
    spark = create_spark_session()
    
    # Initialize rules engine
    rules_engine = ConfigurableRulesEngine(spark)
    
    # Create custom rules
    custom_rules = [
        FraudRule(
            id="high_velocity_cards",
            name="High Velocity Credit Card Usage",
            description="Flag customers using credit cards more than 10 times per hour",
            rule_type=RuleType.VELOCITY,
            severity=RuleSeverity.HIGH,
            condition="distinct_payment_methods_1h > 2 AND transaction_count_1h > 10"
        ),
        FraudRule(
            id="unusual_amount_pattern",
            name="Unusual Amount Pattern",
            description="Flag transactions with amounts significantly different from customer's norm",
            rule_type=RuleType.PATTERN,
            severity=RuleSeverity.MEDIUM,
            condition="amount_z_score > 3 OR amount_z_score < -3"
        ),
        FraudRule(
            id="suspicious_timing",
            name="Suspicious Transaction Timing",
            description="Flag transactions during unusual hours with high amounts",
            rule_type=RuleType.TIME_BASED,
            severity=RuleSeverity.MEDIUM,
            condition="is_unusual_hour = true AND price > 1000"
        ),
        FraudRule(
            id="rapid_location_change",
            name="Rapid Location Change",
            description="Flag transactions from different locations within short time",
            rule_type=RuleType.LOCATION,
            severity=RuleSeverity.CRITICAL,
            condition="distinct_locations_1h > 2 AND transaction_count_1h > 1"
        )
    ]
    
    # Add custom rules
    for rule in custom_rules:
        rules_engine.add_rule(rule)
        print(f"Added rule: {rule.name}")
    
    # Load and process transactions
    transactions_df = spark.read.format("delta").load("data/delta/transactions")
    pattern_analyzer = TransactionPatternAnalyzer(spark)
    enriched_df = pattern_analyzer.create_comprehensive_patterns(transactions_df)
    
    # Apply custom rules
    fraud_results = rules_engine.apply_rules(enriched_df)
    
    # Analyze rule performance
    rule_stats = rules_engine.get_rule_statistics(fraud_results)
    
    print("\nRule Performance Statistics:")
    for rule_id, stats in rule_stats.items():
        print(f"Rule: {stats['name']}")
        print(f"  Triggers: {stats['triggers']}")
        print(f"  Trigger Rate: {stats['trigger_rate']:.2%}")
        print(f"  Avg Score When Triggered: {stats.get('avg_score_when_triggered', 0):.3f}")
        print()
    
    spark.stop()


def example_3_merchant_risk_analysis():
    """
    Example 3: Comprehensive merchant risk analysis.
    """
    print("=== Example 3: Merchant Risk Analysis ===")
    
    spark = create_spark_session()
    
    # Load transaction data
    transactions_df = spark.read.format("delta").load("data/delta/transactions")
    
    # Initialize merchant risk scorer
    risk_scorer = MerchantRiskScorer(spark)
    
    # Calculate merchant risk scores
    merchant_scores = risk_scorer.calculate_merchant_risk_scores(transactions_df, lookback_days=30)
    
    print("Top 10 Highest Risk Merchants:")
    high_risk_merchants = risk_scorer.get_high_risk_merchants(merchant_scores, min_risk_score=0.5)
    high_risk_merchants.select("merchant_id", "merchant_risk_score", "merchant_risk_level",
                              "transaction_count", "total_revenue", "risk_flags").show(10)
    
    # Detailed analysis for a specific merchant
    sample_merchant = high_risk_merchants.first()["merchant_id"]
    detailed_analysis = risk_scorer.analyze_merchant_transaction_patterns(transactions_df, sample_merchant)
    
    print(f"\nDetailed Analysis for Merchant: {sample_merchant}")
    print("Basic Stats:", detailed_analysis["basic_stats"])
    print("Amount Distribution:", detailed_analysis["amount_distribution"])
    
    # Save merchant risk profiles
    risk_scorer.save_risk_profiles(merchant_scores, "data/output/merchant_risk_profiles")
    print("\nMerchant risk profiles saved to data/output/merchant_risk_profiles")
    
    spark.stop()


def example_4_alert_prioritization():
    """
    Example 4: Fraud alert prioritization and management.
    """
    print("=== Example 4: Alert Prioritization ===")
    
    spark = create_spark_session()
    
    # Load transaction data and generate alerts
    transactions_df = spark.read.format("delta").load("data/delta/transactions")
    
    # Initialize components
    orchestrator = FraudDetectionOrchestrator(spark)
    pattern_analyzer = TransactionPatternAnalyzer(spark)
    alert_prioritizer = FraudAlertPrioritizer(spark)
    
    # Process transactions and generate alerts
    enriched_df = pattern_analyzer.create_comprehensive_patterns(transactions_df)
    fraud_alerts = orchestrator.rules_engine.apply_rules(enriched_df)
    fraud_alerts = fraud_alerts.filter(F.col("fraud_score") > 0)
    
    if fraud_alerts.count() > 0:
        # Prioritize alerts
        prioritized_alerts = alert_prioritizer.prioritize_alerts(fraud_alerts)
        
        # Get priority queue
        priority_queue = alert_prioritizer.get_priority_queue(prioritized_alerts, max_alerts=20)
        
        print("Top Priority Fraud Alerts:")
        priority_queue.select("transaction_id", "customer_id", "price", "alert_priority",
                             "priority_score", "recommended_actions", 
                             "estimated_investigation_time_minutes").show()
        
        # Get alert summary metrics
        summary_metrics = alert_prioritizer.get_alert_summary_metrics(prioritized_alerts)
        
        print("\nAlert Summary Metrics:")
        print(f"Total Alerts: {summary_metrics['overall_stats']['total_alerts']}")
        print(f"Total Financial Exposure: ${summary_metrics['overall_stats']['total_financial_exposure']:,.2f}")
        print(f"Estimated Investigation Time: {summary_metrics['overall_stats']['total_investigation_time_hours']:.1f} hours")
        
        print("\nPriority Breakdown:")
        for priority, stats in summary_metrics['priority_breakdown'].items():
            print(f"  {priority}: {stats['count']} alerts (${stats['total_amount']:,.2f})")
        
        # Save prioritized alerts
        alert_prioritizer.save_prioritized_alerts(prioritized_alerts, "data/output/prioritized_alerts")
        print("\nPrioritized alerts saved to data/output/prioritized_alerts")
    else:
        print("No fraud alerts found in the current dataset")
    
    spark.stop()


def example_5_real_time_streaming():
    """
    Example 5: Real-time fraud detection with streaming.
    """
    print("=== Example 5: Real-Time Streaming Fraud Detection ===")
    
    spark = create_spark_session()
    
    # Initialize orchestrator with streaming configuration
    config = {
        'stream_source_path': 'data/delta/transactions',
        'output_path': 'data/delta/fraud_alerts_stream',
        'batch_interval': '30 seconds',
        'watermark_delay': '5 minutes',
        'enable_pattern_analysis': True,
        'enable_merchant_scoring': True,
        'enable_alert_prioritization': True
    }
    
    orchestrator = FraudDetectionOrchestrator(spark, config)
    
    # Start real-time detection (console output for demo)
    streaming_query = orchestrator.start_real_time_detection(output_sink="console")
    
    print("Real-time fraud detection started...")
    print("Streaming query name:", streaming_query.name)
    print("Streaming query ID:", streaming_query.id)
    
    try:
        # Run for a limited time (in production, this would run continuously)
        streaming_query.awaitTermination(timeout=60)  # Run for 1 minute
    except KeyboardInterrupt:
        print("Stopping streaming query...")
    finally:
        orchestrator.stop_all_streams()
    
    spark.stop()


def example_6_system_monitoring():
    """
    Example 6: System monitoring and metrics.
    """
    print("=== Example 6: System Monitoring ===")
    
    spark = create_spark_session()
    
    # Initialize orchestrator
    orchestrator = FraudDetectionOrchestrator(spark)
    
    # Get system status
    status = orchestrator.get_system_status()
    print("System Status:")
    print(f"  Active Streams: {len(status['active_streams'])}")
    print(f"  Total Rules: {status['total_rules']}")
    print(f"  Enabled Rules: {status['enabled_rules']}")
    
    # Get detection metrics (if there's historical data)
    try:
        metrics = orchestrator.get_detection_metrics(time_window="1 hour")
        if "error" not in metrics:
            print("\nDetection Metrics (Last Hour):")
            print(f"  Total Alerts: {metrics['total_alerts']}")
            print(f"  Financial Exposure: ${metrics['total_financial_exposure']:,.2f}")
            print(f"  Average Fraud Score: {metrics['avg_fraud_score']:.3f}")
            
            if metrics['top_triggered_rules']:
                print("  Top Triggered Rules:")
                for rule in metrics['top_triggered_rules']:
                    print(f"    {rule['rule']}: {rule['count']} times")
        else:
            print("No detection metrics available:", metrics.get("message", metrics["error"]))
    except Exception as e:
        print(f"Could not retrieve detection metrics: {e}")
    
    spark.stop()


def example_7_configuration_management():
    """
    Example 7: Advanced configuration management.
    """
    print("=== Example 7: Configuration Management ===")
    
    spark = create_spark_session()
    
    # Initialize orchestrator
    orchestrator = FraudDetectionOrchestrator(spark)
    
    # Update alert prioritizer weights
    new_priority_weights = {
        'financial_impact': 0.30,  # Increase financial impact weight
        'customer_risk': 0.25,     # Increase customer risk weight
        'merchant_risk': 0.15,
        'pattern_complexity': 0.10,
        'historical_accuracy': 0.10,
        'time_sensitivity': 0.05,  # Decrease time sensitivity weight
        'business_criticality': 0.05
    }
    
    orchestrator.alert_prioritizer.update_priority_weights(new_priority_weights)
    print("Updated alert prioritization weights")
    
    # Update risk factor weights for merchant scoring
    new_risk_weights = {
        'transaction_volume_weight': 0.10,
        'chargeback_rate_weight': 0.30,  # Increase chargeback focus
        'refund_rate_weight': 0.25,      # Increase refund focus
        'customer_diversity_weight': 0.15,
        'amount_volatility_weight': 0.10,
        'new_merchant_penalty': 0.10
    }
    
    orchestrator.merchant_risk_scorer.update_risk_factors_weights(new_risk_weights)
    print("Updated merchant risk scoring weights")
    
    # Update overall orchestrator configuration
    config_updates = {
        'batch_interval': '15 seconds',  # More frequent processing
        'watermark_delay': '2 minutes',  # Shorter watermark delay
        'alert_thresholds': {
            'critical_priority_action': 'immediate_block_and_notify',
            'high_priority_action': 'priority_manual_review',
            'medium_priority_action': 'standard_review_queue',
            'low_priority_action': 'automated_batch_review'
        }
    }
    
    orchestrator.update_configuration(config_updates)
    print("Updated orchestrator configuration")
    
    # Display final configuration
    print("\nCurrent Configuration:")
    for key, value in orchestrator.config.items():
        print(f"  {key}: {value}")
    
    spark.stop()


if __name__ == "__main__":
    """
    Run all examples. Comment out examples you don't want to run.
    """
    
    print("Fraud Detection System Examples")
    print("=" * 50)
    
    try:
        # Basic examples
        example_1_basic_fraud_detection()
        print()
        
        example_2_custom_fraud_rules()
        print()
        
        example_3_merchant_risk_analysis()
        print()
        
        example_4_alert_prioritization()
        print()
        
        # Advanced examples (comment out if you don't have streaming setup)
        # example_5_real_time_streaming()
        # print()
        
        example_6_system_monitoring()
        print()
        
        example_7_configuration_management()
        print()
        
    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()
    
    print("Examples completed!")
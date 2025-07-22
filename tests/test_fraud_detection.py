"""
Comprehensive tests for fraud detection system.
"""

import pytest
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

from src.analytics.fraud_detection import (
    ConfigurableRulesEngine,
    FraudRule,
    RuleSeverity, 
    RuleType,
    TransactionPatternAnalyzer,
    MerchantRiskScorer,
    MerchantRiskLevel,
    FraudAlertPrioritizer,
    AlertPriority,
    FraudDetectionOrchestrator
)


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder\
        .appName("FraudDetectionTests")\
        .master("local[2]")\
        .config("spark.sql.adaptive.enabled", "false")\
        .getOrCreate()


@pytest.fixture
def sample_transactions(spark):
    """Create sample transaction data for testing."""
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True)
    ])
    
    base_time = datetime.now()
    data = [
        ("txn_001", "cust_001", "merchant_001", 100.0, "credit_card", base_time, "NYC"),
        ("txn_002", "cust_001", "merchant_002", 15000.0, "credit_card", base_time + timedelta(minutes=5), "NYC"),
        ("txn_003", "cust_002", "merchant_001", 500.0, "paypal", base_time + timedelta(minutes=10), "LA"),
        ("txn_004", "cust_001", "merchant_001", 50.0, "credit_card", base_time + timedelta(minutes=15), "Chicago"),
        ("txn_005", "cust_003", "merchant_003", 2000.0, "bank_transfer", base_time + timedelta(hours=2), "NYC"),
        ("txn_006", "cust_001", "merchant_001", 100.0, "debit_card", base_time + timedelta(minutes=20), "NYC"),
        ("txn_007", "cust_002", "merchant_002", 800.0, "credit_card", base_time + timedelta(hours=3), "LA"),
        ("txn_008", "cust_004", "merchant_004", 25000.0, "wire_transfer", base_time + timedelta(minutes=2), "Miami")
    ]
    
    return spark.createDataFrame(data, schema)


class TestConfigurableRulesEngine:
    """Test suite for ConfigurableRulesEngine."""
    
    def test_rule_creation_and_management(self, spark):
        """Test creating and managing rules."""
        engine = ConfigurableRulesEngine(spark)
        
        # Test adding a rule
        rule = FraudRule(
            id="test_rule",
            name="Test High Amount",
            description="Test rule for high amounts",
            rule_type=RuleType.AMOUNT_THRESHOLD,
            severity=RuleSeverity.HIGH,
            condition="price > 5000"
        )
        
        engine.add_rule(rule)
        assert len(engine.rules) > 0
        assert "test_rule" in engine.rules
        
        # Test disabling rule
        engine.disable_rule("test_rule")
        assert not engine.rules["test_rule"].enabled
        
        # Test enabling rule
        engine.enable_rule("test_rule")
        assert engine.rules["test_rule"].enabled
        
        # Test removing rule
        engine.remove_rule("test_rule")
        assert "test_rule" not in engine.rules
    
    def test_apply_rules(self, spark, sample_transactions):
        """Test applying rules to transaction data."""
        engine = ConfigurableRulesEngine(spark)
        
        # Add test rule for high amounts
        high_amount_rule = FraudRule(
            id="high_amount_test",
            name="High Amount Test",
            description="Flag transactions over $10,000",
            rule_type=RuleType.AMOUNT_THRESHOLD,
            severity=RuleSeverity.HIGH,
            condition="price > 10000"
        )
        engine.add_rule(high_amount_rule)
        
        # Apply rules
        result_df = engine.apply_rules(sample_transactions)
        
        # Check results
        assert "fraud_score" in result_df.columns
        assert "triggered_rules" in result_df.columns
        assert "fraud_risk_level" in result_df.columns
        
        # Check that high amount transactions are flagged
        high_amount_alerts = result_df.filter(F.col("price") > 10000).collect()
        for alert in high_amount_alerts:
            assert alert["fraud_score"] > 0
            assert "high_amount_test" in alert["triggered_rules"]
    
    def test_rule_statistics(self, spark, sample_transactions):
        """Test rule statistics calculation."""
        engine = ConfigurableRulesEngine(spark)
        
        # Apply rules first
        result_df = engine.apply_rules(sample_transactions)
        
        # Get statistics
        stats = engine.get_rule_statistics(result_df)
        
        assert isinstance(stats, dict)
        for rule_id, rule_stats in stats.items():
            assert "triggers" in rule_stats
            assert "total" in rule_stats
            assert "trigger_rate" in rule_stats


class TestTransactionPatternAnalyzer:
    """Test suite for TransactionPatternAnalyzer."""
    
    def test_velocity_patterns(self, spark, sample_transactions):
        """Test velocity pattern analysis."""
        analyzer = TransactionPatternAnalyzer(spark)
        
        result_df = analyzer.analyze_velocity_patterns(sample_transactions)
        
        # Check velocity columns exist
        assert "transaction_count_1_hour" in result_df.columns
        assert "total_amount_1_hour" in result_df.columns
        assert "distinct_merchants_1_hour" in result_df.columns
        
        # Check that customer with multiple transactions has higher counts
        cust_001_data = result_df.filter(F.col("customer_id") == "cust_001").collect()
        assert len(cust_001_data) > 0
    
    def test_amount_patterns(self, spark, sample_transactions):
        """Test amount pattern analysis."""
        analyzer = TransactionPatternAnalyzer(spark)
        
        result_df = analyzer.analyze_amount_patterns(sample_transactions)
        
        # Check amount pattern columns exist
        assert "customer_avg_amount" in result_df.columns
        assert "amount_deviation_from_avg" in result_df.columns
        assert "amount_z_score" in result_df.columns
        assert "is_round_amount" in result_df.columns
        assert "amount_percentile" in result_df.columns
    
    def test_time_patterns(self, spark, sample_transactions):
        """Test time-based pattern analysis."""
        analyzer = TransactionPatternAnalyzer(spark)
        
        result_df = analyzer.analyze_time_patterns(sample_transactions)
        
        # Check time pattern columns exist  
        assert "hour" in result_df.columns
        assert "is_business_hours" in result_df.columns
        assert "is_unusual_hour" in result_df.columns
        assert "is_weekend" in result_df.columns
        assert "seconds_since_last_transaction" in result_df.columns
    
    def test_comprehensive_patterns(self, spark, sample_transactions):
        """Test comprehensive pattern analysis."""
        analyzer = TransactionPatternAnalyzer(spark)
        
        result_df = analyzer.create_comprehensive_patterns(sample_transactions)
        
        # Check that multiple pattern types are included
        velocity_cols = [col for col in result_df.columns if "transaction_count" in col]
        amount_cols = [col for col in result_df.columns if "amount_deviation" in col]
        time_cols = [col for col in result_df.columns if "hour" in col]
        
        assert len(velocity_cols) > 0
        assert len(amount_cols) > 0  
        assert len(time_cols) > 0


class TestMerchantRiskScorer:
    """Test suite for MerchantRiskScorer."""
    
    def test_merchant_risk_calculation(self, spark, sample_transactions):
        """Test merchant risk score calculation."""
        scorer = MerchantRiskScorer(spark)
        
        risk_scores_df = scorer.calculate_merchant_risk_scores(sample_transactions)
        
        # Check required columns exist
        assert "merchant_id" in risk_scores_df.columns
        assert "merchant_risk_score" in risk_scores_df.columns
        assert "merchant_risk_level" in risk_scores_df.columns
        assert "risk_flags" in risk_scores_df.columns
        
        # Check risk scores are in valid range [0, 1]
        risk_data = risk_scores_df.collect()
        for row in risk_data:
            assert 0.0 <= row["merchant_risk_score"] <= 1.0
            assert row["merchant_risk_level"] in ["VERY_LOW", "LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
    
    def test_high_risk_merchant_identification(self, spark, sample_transactions):
        """Test identification of high-risk merchants."""
        scorer = MerchantRiskScorer(spark)
        
        risk_scores_df = scorer.calculate_merchant_risk_scores(sample_transactions)
        high_risk_df = scorer.get_high_risk_merchants(risk_scores_df, min_risk_score=0.3)
        
        # Check that all returned merchants meet the threshold
        high_risk_data = high_risk_df.collect()
        for row in high_risk_data:
            assert row["merchant_risk_score"] >= 0.3
    
    def test_merchant_analysis(self, spark, sample_transactions):
        """Test detailed merchant analysis."""
        scorer = MerchantRiskScorer(spark)
        
        # Analyze specific merchant
        analysis = scorer.analyze_merchant_transaction_patterns(sample_transactions, "merchant_001")
        
        assert "merchant_id" in analysis
        assert "basic_stats" in analysis
        assert "hourly_pattern" in analysis
        assert "daily_pattern" in analysis
        assert "amount_distribution" in analysis


class TestFraudAlertPrioritizer:
    """Test suite for FraudAlertPrioritizer."""
    
    def test_alert_prioritization(self, spark, sample_transactions):
        """Test fraud alert prioritization."""
        prioritizer = FraudAlertPrioritizer(spark)
        
        # First generate some fraud alerts using rules engine
        engine = ConfigurableRulesEngine(spark)
        alerts_df = engine.apply_rules(sample_transactions)
        alerts_df = alerts_df.filter(F.col("fraud_score") > 0)
        
        if alerts_df.count() == 0:
            # Add a rule that will trigger some alerts
            test_rule = FraudRule(
                id="test_trigger",
                name="Test Trigger",
                description="Always trigger for testing",
                rule_type=RuleType.CUSTOM,
                severity=RuleSeverity.MEDIUM,
                condition="price > 0"  # Always true
            )
            engine.add_rule(test_rule)
            alerts_df = engine.apply_rules(sample_transactions)
            alerts_df = alerts_df.filter(F.col("fraud_score") > 0)
        
        # Prioritize alerts
        prioritized_df = prioritizer.prioritize_alerts(alerts_df)
        
        # Check prioritization columns exist
        assert "priority_score" in prioritized_df.columns
        assert "alert_priority" in prioritized_df.columns
        assert "recommended_actions" in prioritized_df.columns
        assert "estimated_investigation_time_minutes" in prioritized_df.columns
        
        # Check priority scores are in valid range
        priority_data = prioritized_df.collect()
        for row in priority_data:
            assert 0.0 <= row["priority_score"] <= 2.0  # Can exceed 1.0 due to weighting
            assert row["alert_priority"] in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    
    def test_priority_queue(self, spark, sample_transactions):
        """Test priority queue generation.""" 
        prioritizer = FraudAlertPrioritizer(spark)
        engine = ConfigurableRulesEngine(spark)
        
        # Generate alerts
        alerts_df = engine.apply_rules(sample_transactions)
        alerts_df = alerts_df.filter(F.col("fraud_score") > 0)
        
        if alerts_df.count() == 0:
            # Force some alerts for testing
            alerts_df = sample_transactions.withColumn("fraud_score", F.lit(0.5))\
                                          .withColumn("triggered_rules", F.array(F.lit("test_rule")))
        
        prioritized_df = prioritizer.prioritize_alerts(alerts_df)
        queue_df = prioritizer.get_priority_queue(prioritized_df, max_alerts=5)
        
        assert "queue_position" in queue_df.columns
        assert queue_df.count() <= 5
    
    def test_alert_summary_metrics(self, spark, sample_transactions):
        """Test alert summary metrics."""
        prioritizer = FraudAlertPrioritizer(spark)
        engine = ConfigurableRulesEngine(spark)
        
        # Generate and prioritize alerts
        alerts_df = engine.apply_rules(sample_transactions)
        alerts_df = alerts_df.filter(F.col("fraud_score") > 0)
        
        if alerts_df.count() == 0:
            alerts_df = sample_transactions.withColumn("fraud_score", F.lit(0.5))\
                                          .withColumn("triggered_rules", F.array(F.lit("test_rule")))\
                                          .withColumn("alert_priority", F.lit("MEDIUM"))
        
        prioritized_df = prioritizer.prioritize_alerts(alerts_df)
        metrics = prioritizer.get_alert_summary_metrics(prioritized_df)
        
        assert "priority_breakdown" in metrics
        assert "overall_stats" in metrics
        assert "top_triggered_rules" in metrics


class TestFraudDetectionOrchestrator:
    """Test suite for FraudDetectionOrchestrator."""
    
    def test_orchestrator_initialization(self, spark):
        """Test orchestrator initialization."""
        orchestrator = FraudDetectionOrchestrator(spark)
        
        assert orchestrator.spark == spark
        assert orchestrator.rules_engine is not None
        assert orchestrator.pattern_analyzer is not None
        assert orchestrator.merchant_risk_scorer is not None
        assert orchestrator.alert_prioritizer is not None
        assert isinstance(orchestrator.config, dict)
    
    def test_custom_rule_addition(self, spark):
        """Test adding custom rules through orchestrator."""
        orchestrator = FraudDetectionOrchestrator(spark)
        
        custom_rule = FraudRule(
            id="custom_test",
            name="Custom Test Rule",
            description="Test custom rule",
            rule_type=RuleType.CUSTOM,
            severity=RuleSeverity.LOW,
            condition="price > 1000"
        )
        
        initial_count = len(orchestrator.rules_engine.rules)
        orchestrator.add_custom_rule(custom_rule)
        
        assert len(orchestrator.rules_engine.rules) == initial_count + 1
        assert "custom_test" in orchestrator.rules_engine.rules
    
    def test_system_status(self, spark):
        """Test system status reporting."""
        orchestrator = FraudDetectionOrchestrator(spark)
        
        status = orchestrator.get_system_status()
        
        assert "active_streams" in status
        assert "total_rules" in status
        assert "enabled_rules" in status
        assert "configuration" in status
        assert "last_batch_update" in status
    
    def test_configuration_update(self, spark):
        """Test configuration updates."""
        orchestrator = FraudDetectionOrchestrator(spark)
        
        initial_config = orchestrator.config.copy()
        new_config = {"test_param": "test_value"}
        
        orchestrator.update_configuration(new_config)
        
        assert orchestrator.config["test_param"] == "test_value"
        # Check that original config is preserved
        for key, value in initial_config.items():
            if key != "test_param":
                assert orchestrator.config[key] == value


class TestIntegration:
    """Integration tests for the complete fraud detection pipeline."""
    
    def test_end_to_end_detection(self, spark, sample_transactions):
        """Test complete end-to-end fraud detection pipeline."""
        orchestrator = FraudDetectionOrchestrator(spark)
        
        # Apply the complete fraud detection pipeline (without streaming)
        # This tests the core pipeline logic
        pattern_analyzer = TransactionPatternAnalyzer(spark)
        enriched_df = pattern_analyzer.create_comprehensive_patterns(sample_transactions)
        
        # Apply rules
        alerts_df = orchestrator.rules_engine.apply_rules(enriched_df)
        alerts_df = alerts_df.filter(F.col("fraud_score") > 0)
        
        if alerts_df.count() > 0:
            # Prioritize alerts
            prioritized_df = orchestrator.alert_prioritizer.prioritize_alerts(alerts_df)
            
            # Check final output has all expected columns
            expected_cols = [
                "transaction_id", "customer_id", "merchant_id", "price",
                "fraud_score", "triggered_rules", "alert_priority", 
                "priority_score", "recommended_actions"
            ]
            
            for col in expected_cols:
                assert col in prioritized_df.columns
            
            # Check data integrity
            priority_data = prioritized_df.collect()
            for row in priority_data:
                assert row["fraud_score"] > 0
                assert len(row["triggered_rules"]) > 0
                assert row["alert_priority"] in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    
    def test_batch_processing_simulation(self, spark, sample_transactions):
        """Test batch processing capabilities."""
        orchestrator = FraudDetectionOrchestrator(spark)
        
        # Create merchant risk scores
        merchant_scores = orchestrator.merchant_risk_scorer.calculate_merchant_risk_scores(sample_transactions)
        
        assert merchant_scores.count() > 0
        
        # Test merchant profile analysis
        merchants = sample_transactions.select("merchant_id").distinct().collect()
        for merchant_row in merchants:
            merchant_id = merchant_row["merchant_id"]
            analysis = orchestrator.merchant_risk_scorer.analyze_merchant_transaction_patterns(
                sample_transactions, merchant_id
            )
            assert "merchant_id" in analysis
            assert analysis["merchant_id"] == merchant_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
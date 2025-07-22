"""
Rule-based fraud detection engine with configurable business rules.
"""

from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import json
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, ArrayType


class RuleSeverity(Enum):
    """Severity levels for fraud rules."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RuleType(Enum):
    """Types of fraud detection rules."""
    AMOUNT_THRESHOLD = "amount_threshold"
    VELOCITY = "velocity"
    PATTERN = "pattern"
    MERCHANT_RISK = "merchant_risk"
    LOCATION = "location"
    TIME_BASED = "time_based"
    CUSTOM = "custom"


@dataclass
class FraudRule:
    """Definition of a fraud detection rule."""
    id: str
    name: str
    description: str
    rule_type: RuleType
    severity: RuleSeverity
    condition: str
    enabled: bool = True
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert rule to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'rule_type': self.rule_type.value,
            'severity': self.severity.value,
            'condition': self.condition,
            'enabled': self.enabled,
            'weight': self.weight,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FraudRule':
        """Create rule from dictionary."""
        return cls(
            id=data['id'],
            name=data['name'],
            description=data['description'],
            rule_type=RuleType(data['rule_type']),
            severity=RuleSeverity(data['severity']),
            condition=data['condition'],
            enabled=data.get('enabled', True),
            weight=data.get('weight', 1.0),
            metadata=data.get('metadata', {})
        )


@dataclass
class FraudAlert:
    """Fraud alert information."""
    transaction_id: str
    alert_id: str
    rule_id: str
    rule_name: str
    severity: RuleSeverity
    score: float
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[str] = None


class ConfigurableRulesEngine:
    """
    Advanced configurable business rules engine for fraud detection.
    
    Features:
    - Dynamic rule loading and management
    - Multiple rule types (amount, velocity, pattern, etc.)
    - Configurable scoring and alerting
    - Real-time processing capability
    - Extensible rule framework
    """
    
    def __init__(self, spark: SparkSession, config_path: Optional[str] = None):
        """
        Initialize the rules engine.
        
        Args:
            spark: SparkSession instance
            config_path: Optional path to rules configuration file
        """
        self.spark = spark
        self.rules: Dict[str, FraudRule] = {}
        self.severity_weights = {
            RuleSeverity.LOW: 0.1,
            RuleSeverity.MEDIUM: 0.3,
            RuleSeverity.HIGH: 0.7,
            RuleSeverity.CRITICAL: 1.0
        }
        
        # Load default rules if config provided
        if config_path:
            self.load_rules_from_config(config_path)
        else:
            self._load_default_rules()
    
    def add_rule(self, rule: FraudRule) -> None:
        """Add a fraud detection rule."""
        self.rules[rule.id] = rule
    
    def remove_rule(self, rule_id: str) -> None:
        """Remove a fraud detection rule."""
        if rule_id in self.rules:
            del self.rules[rule_id]
    
    def enable_rule(self, rule_id: str) -> None:
        """Enable a fraud detection rule."""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = True
    
    def disable_rule(self, rule_id: str) -> None:
        """Disable a fraud detection rule."""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = False
    
    def get_enabled_rules(self) -> List[FraudRule]:
        """Get all enabled rules."""
        return [rule for rule in self.rules.values() if rule.enabled]
    
    def load_rules_from_config(self, config_path: str) -> None:
        """Load rules from configuration file."""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            for rule_data in config.get('rules', []):
                rule = FraudRule.from_dict(rule_data)
                self.add_rule(rule)
                
        except Exception as e:
            raise RuntimeError(f"Failed to load rules from {config_path}: {str(e)}")
    
    def save_rules_to_config(self, config_path: str) -> None:
        """Save current rules to configuration file."""
        config = {
            'rules': [rule.to_dict() for rule in self.rules.values()]
        }
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
    
    def _load_default_rules(self) -> None:
        """Load default fraud detection rules."""
        default_rules = [
            FraudRule(
                id="high_amount_threshold",
                name="High Amount Threshold",
                description="Flag transactions above $10,000",
                rule_type=RuleType.AMOUNT_THRESHOLD,
                severity=RuleSeverity.HIGH,
                condition="price > 10000"
            ),
            FraudRule(
                id="velocity_check_hourly",
                name="Hourly Velocity Check",
                description="Flag users with >5 transactions in 1 hour",
                rule_type=RuleType.VELOCITY,
                severity=RuleSeverity.MEDIUM,
                condition="transaction_count_1h > 5"
            ),
            FraudRule(
                id="round_amount_pattern",
                name="Round Amount Pattern",
                description="Flag transactions with round amounts (multiples of 100)",
                rule_type=RuleType.PATTERN,
                severity=RuleSeverity.LOW,
                condition="price % 100 = 0 AND price > 500"
            ),
            FraudRule(
                id="unusual_time_pattern",
                name="Unusual Time Pattern",
                description="Flag transactions between 2-5 AM",
                rule_type=RuleType.TIME_BASED,
                severity=RuleSeverity.MEDIUM,
                condition="hour(timestamp) >= 2 AND hour(timestamp) <= 5"
            ),
            FraudRule(
                id="multiple_payment_methods",
                name="Multiple Payment Methods",
                description="Flag users using multiple payment methods in short time",
                rule_type=RuleType.PATTERN,
                severity=RuleSeverity.HIGH,
                condition="distinct_payment_methods_1h > 2"
            )
        ]
        
        for rule in default_rules:
            self.add_rule(rule)
    
    def apply_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply all enabled rules to the DataFrame.
        
        Args:
            df: Input transaction DataFrame
            
        Returns:
            DataFrame with fraud detection results
        """
        enabled_rules = self.get_enabled_rules()
        if not enabled_rules:
            return df.withColumn("fraud_score", F.lit(0.0))\
                    .withColumn("fraud_alerts", F.array())\
                    .withColumn("triggered_rules", F.array())
        
        # Start with original DataFrame
        result_df = df
        fraud_score_expr = F.lit(0.0)
        triggered_rules_array = F.array()
        fraud_alerts_array = F.array()
        
        # Apply each enabled rule
        for rule in enabled_rules:
            try:
                # Parse rule condition
                rule_condition = F.expr(rule.condition)
                
                # Add rule trigger column
                rule_trigger_col = f"rule_{rule.id}_triggered"
                result_df = result_df.withColumn(rule_trigger_col, rule_condition)
                
                # Calculate rule score contribution
                severity_weight = self.severity_weights[rule.severity]
                rule_score = severity_weight * rule.weight
                
                # Update fraud score
                fraud_score_expr = fraud_score_expr + F.when(
                    rule_condition, rule_score
                ).otherwise(0.0)
                
                # Add to triggered rules array
                triggered_rules_array = F.when(
                    rule_condition,
                    F.array_union(triggered_rules_array, F.array(F.lit(rule.id)))
                ).otherwise(triggered_rules_array)
                
                # Create alert info
                alert_info = F.struct(
                    F.lit(rule.id).alias("rule_id"),
                    F.lit(rule.name).alias("rule_name"),
                    F.lit(rule.severity.value).alias("severity"),
                    F.lit(rule_score).alias("score"),
                    F.lit(rule.description).alias("message")
                )
                
                # Add to fraud alerts array
                fraud_alerts_array = F.when(
                    rule_condition,
                    F.array_union(fraud_alerts_array, F.array(alert_info))
                ).otherwise(fraud_alerts_array)
                
            except Exception as e:
                # Skip invalid rules but log the error
                print(f"Warning: Skipping rule {rule.id} due to error: {str(e)}")
                continue
        
        # Add final columns
        result_df = result_df.withColumn("fraud_score", fraud_score_expr)
        result_df = result_df.withColumn("triggered_rules", triggered_rules_array)
        result_df = result_df.withColumn("fraud_alerts", fraud_alerts_array)
        
        # Add fraud risk level
        result_df = result_df.withColumn("fraud_risk_level",
            F.when(F.col("fraud_score") >= 0.7, "CRITICAL")
            .when(F.col("fraud_score") >= 0.5, "HIGH")
            .when(F.col("fraud_score") >= 0.3, "MEDIUM")
            .when(F.col("fraud_score") > 0.0, "LOW")
            .otherwise("NONE")
        )
        
        return result_df
    
    def get_rule_statistics(self, df: DataFrame) -> Dict[str, Dict]:
        """
        Get statistics about rule triggers.
        
        Args:
            df: DataFrame with applied rules
            
        Returns:
            Dictionary with rule statistics
        """
        enabled_rules = self.get_enabled_rules()
        stats = {}
        
        for rule in enabled_rules:
            trigger_col = f"rule_{rule.id}_triggered"
            if trigger_col in df.columns:
                rule_stats = df.agg(
                    F.sum(F.when(F.col(trigger_col), 1).otherwise(0)).alias("triggers"),
                    F.count("*").alias("total"),
                    F.avg(F.when(F.col(trigger_col), F.col("fraud_score")).otherwise(0)).alias("avg_score")
                ).collect()[0]
                
                stats[rule.id] = {
                    "name": rule.name,
                    "triggers": rule_stats["triggers"],
                    "total": rule_stats["total"],
                    "trigger_rate": rule_stats["triggers"] / rule_stats["total"] if rule_stats["total"] > 0 else 0,
                    "avg_score_when_triggered": rule_stats["avg_score"]
                }
        
        return stats
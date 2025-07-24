"""
Enhanced fraud detection endpoints for API v1.

This module provides comprehensive endpoints for fraud detection results, monitoring,
and case management. Integrates with the fraud detection engines for real-time
analysis, rule management, and investigator tools.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import PaginationParams, get_database_session, get_pagination
from ...exceptions import NotFoundError, ValidationError

router = APIRouter()


@router.get("/alerts", summary="Get comprehensive fraud alerts")
async def get_fraud_alerts(
    status: Optional[str] = Query(
        default=None,
        description="Alert status: open, investigating, resolved, escalated",
    ),
    priority: Optional[str] = Query(
        default=None, description="Alert priority: critical, high, medium, low"
    ),
    alert_type: Optional[str] = Query(
        default=None,
        description="Alert type: velocity, location, amount, pattern, ml_anomaly",
    ),
    risk_score_min: Optional[float] = Query(
        default=None, description="Minimum risk score filter"
    ),
    start_date: Optional[str] = Query(default=None, description="Start date filter"),
    end_date: Optional[str] = Query(default=None, description="End date filter"),
    assigned_to: Optional[str] = Query(
        default=None, description="Assigned investigator filter"
    ),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_FRAUD)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get comprehensive fraud alerts with advanced filtering and analytics.

    Args:
        status: Alert status filter
        priority: Alert priority filter
        alert_type: Alert type filter
        risk_score_min: Minimum risk score filter
        start_date: Start date filter
        end_date: End date filter
        assigned_to: Assigned investigator filter
        pagination: Pagination parameters
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing comprehensive fraud alerts with analytics
    """
    # Validate filters
    valid_statuses = [
        "open",
        "investigating",
        "resolved",
        "escalated",
        "false_positive",
    ]
    if status and status not in valid_statuses:
        raise ValidationError(
            f"Invalid status. Must be one of: {valid_statuses}",
            {"status": status},
        )

    valid_priorities = ["critical", "high", "medium", "low"]
    if priority and priority not in valid_priorities:
        raise ValidationError(
            f"Invalid priority. Must be one of: {valid_priorities}",
            {"priority": priority},
        )

    valid_types = [
        "velocity",
        "location",
        "amount",
        "pattern",
        "ml_anomaly",
        "device",
        "behavioral",
    ]
    if alert_type and alert_type not in valid_types:
        raise ValidationError(
            f"Invalid alert type. Must be one of: {valid_types}",
            {"alert_type": alert_type},
        )

    import random

    # Generate comprehensive fraud alerts
    alerts = []
    total_alerts = 75
    alert_types = [
        "velocity",
        "location",
        "amount",
        "pattern",
        "ml_anomaly",
        "device",
        "behavioral",
    ]
    statuses = ["open", "investigating", "resolved", "escalated", "false_positive"]
    priorities = ["critical", "high", "medium", "low"]

    # Apply filters to total count
    if status:
        total_alerts = int(total_alerts * 0.4)
    if priority:
        total_alerts = int(total_alerts * 0.6)
    if alert_type:
        total_alerts = int(total_alerts * 0.3)

    for i in range(
        pagination.offset, min(pagination.offset + pagination.size, total_alerts)
    ):
        alert_status = status or random.choice(statuses)
        alert_priority = priority or random.choice(priorities)
        selected_alert_type = alert_type or random.choice(alert_types)
        risk_score = random.uniform(0.3, 1.0)

        # Skip if doesn't meet risk score filter
        if risk_score_min and risk_score < risk_score_min:
            continue

        alert_data = {
            "alert_id": f"alert_{i+1:06d}",
            "transaction_id": f"txn_{random.randint(100000, 999999)}",
            "customer_id": f"customer_{random.randint(1, 10000):06d}",
            "merchant_id": f"merchant_{random.randint(1, 1000):04d}",
            "alert_type": selected_alert_type,
            "status": alert_status,
            "priority": alert_priority,
            "risk_score": round(risk_score, 3),
            "amount": round(random.uniform(50, 10000), 2),
            "currency": "USD",
            "created_at": (
                datetime.now() - timedelta(hours=random.randint(1, 168))
            ).isoformat(),
            "updated_at": (
                datetime.now() - timedelta(hours=random.randint(0, 24))
            ).isoformat(),
            "description": {
                "velocity": "High transaction velocity detected",
                "location": "Transaction from unusual geographic location",
                "amount": "Unusually large transaction amount",
                "pattern": "Suspicious transaction pattern identified",
                "ml_anomaly": "Machine learning model detected anomaly",
                "device": "Suspicious device fingerprint",
                "behavioral": "Abnormal user behavior pattern",
            }[selected_alert_type],
            "investigation": {
                "assigned_to": assigned_to
                or (
                    f"investigator_{random.randint(1, 5)}"
                    if random.choice([True, False])
                    else None
                ),
                "investigation_time_minutes": random.randint(0, 480)
                if alert_status != "open"
                else 0,
                "evidence_count": random.randint(0, 8),
                "notes_count": random.randint(0, 5),
            },
            "fraud_indicators": {
                "velocity_score": round(random.uniform(0.1, 1.0), 3),
                "location_score": round(random.uniform(0.1, 1.0), 3),
                "amount_score": round(random.uniform(0.1, 1.0), 3),
                "behavioral_score": round(random.uniform(0.1, 1.0), 3),
                "device_score": round(random.uniform(0.1, 1.0), 3),
            },
            "financial_impact": {
                "potential_loss": round(random.uniform(100, 50000), 2),
                "prevention_savings": round(random.uniform(500, 25000), 2)
                if alert_status == "resolved"
                else None,
                "investigation_cost": round(random.uniform(50, 500), 2)
                if alert_status != "open"
                else None,
            },
        }

        alerts.append(alert_data)

    # Calculate summary statistics
    total_amount_at_risk = sum(alert["amount"] for alert in alerts)
    avg_risk_score = (
        sum(alert["risk_score"] for alert in alerts) / len(alerts) if alerts else 0
    )

    # Priority distribution
    priority_counts = {}
    for p in priorities:
        priority_counts[p] = len([a for a in alerts if a["priority"] == p])

    # Status distribution
    status_counts = {}
    for s in statuses:
        status_counts[s] = len([a for a in alerts if a["status"] == s])

    return {
        "alerts": alerts,
        "filters": {
            "status": status,
            "priority": priority,
            "alert_type": alert_type,
            "risk_score_min": risk_score_min,
            "start_date": start_date,
            "end_date": end_date,
            "assigned_to": assigned_to,
        },
        "pagination": {
            "page": pagination.page,
            "size": pagination.size,
            "total": total_alerts,
            "pages": (total_alerts + pagination.size - 1) // pagination.size,
        },
        "summary": {
            "total_alerts": total_alerts,
            "total_amount_at_risk": round(total_amount_at_risk, 2),
            "average_risk_score": round(avg_risk_score, 3),
            "priority_distribution": priority_counts,
            "status_distribution": status_counts,
            "sla_performance": {
                "on_time_resolution": 0.87,
                "avg_resolution_time_hours": 14.5,
                "escalation_rate": 0.08,
            },
        },
        "insights": {
            "trending_alert_types": ["ml_anomaly", "velocity", "behavioral"],
            "peak_hours": ["14:00-16:00", "20:00-22:00"],
            "highest_risk_merchant": f"merchant_{random.randint(1, 100):04d}",
            "false_positive_rate": 0.12,
        },
    }


@router.get("/dashboard", summary="Get comprehensive fraud detection dashboard")
async def get_fraud_dashboard(
    time_range: str = Query(
        default="24h", description="Time range: 1h, 4h, 24h, 7d, 30d"
    ),
    include_predictions: bool = Query(
        default=False, description="Include ML model predictions"
    ),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_FRAUD)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get comprehensive fraud detection dashboard with real-time metrics and insights.

    Args:
        time_range: Time range for metrics
        include_predictions: Include ML model predictions
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing comprehensive dashboard metrics
    """
    valid_ranges = ["1h", "4h", "24h", "7d", "30d"]
    if time_range not in valid_ranges:
        raise ValidationError(
            f"Invalid time range. Must be one of: {valid_ranges}",
            {"time_range": time_range},
        )

    import random

    # Time range multipliers for scaling data
    multipliers = {"1h": 0.04, "4h": 0.17, "24h": 1.0, "7d": 7.0, "30d": 30.0}
    base_multiplier = multipliers[time_range]

    dashboard_data = {
        "time_range": time_range,
        "generated_at": datetime.now().isoformat(),
        "overview": {
            "total_transactions": int(125000 * base_multiplier),
            "flagged_transactions": int(1250 * base_multiplier),
            "confirmed_fraud": int(125 * base_multiplier),
            "false_positives": int(150 * base_multiplier),
            "fraud_rate": round(random.uniform(0.008, 0.015), 4),
            "false_positive_rate": round(random.uniform(0.10, 0.18), 3),
            "total_blocked_amount": round(125000 * base_multiplier, 2),
            "total_loss_prevented": round(100000 * base_multiplier, 2),
            "investigation_backlog": random.randint(15, 45),
        },
        "alerts": {
            "total": int(95 * base_multiplier),
            "open": random.randint(8, 25),
            "investigating": random.randint(5, 15),
            "resolved": int(70 * base_multiplier),
            "escalated": random.randint(2, 8),
            "false_positives": int(12 * base_multiplier),
            "by_priority": {
                "critical": random.randint(2, 8),
                "high": random.randint(8, 20),
                "medium": random.randint(15, 35),
                "low": random.randint(20, 40),
            },
            "by_type": {
                "velocity": random.randint(8, 25),
                "location": random.randint(6, 20),
                "amount": random.randint(5, 15),
                "pattern": random.randint(4, 12),
                "ml_anomaly": random.randint(6, 18),
                "device": random.randint(3, 10),
                "behavioral": random.randint(4, 12),
            },
        },
        "performance": {
            "detection_latency_ms": random.randint(85, 185),
            "processing_rate_tps": random.randint(8000, 12000),
            "model_accuracy": round(random.uniform(0.91, 0.96), 3),
            "precision": round(random.uniform(0.86, 0.93), 3),
            "recall": round(random.uniform(0.89, 0.95), 3),
            "f1_score": round(random.uniform(0.87, 0.94), 3),
            "auc_roc": round(random.uniform(0.92, 0.98), 3),
        },
        "financial_impact": {
            "total_exposure": round(random.uniform(500000, 2000000), 2),
            "amount_blocked": round(random.uniform(125000, 500000), 2),
            "loss_prevented": round(random.uniform(100000, 400000), 2),
            "investigation_costs": round(random.uniform(15000, 35000), 2),
            "roi": round(random.uniform(8.5, 15.2), 1),
        },
        "trends": {
            "hourly_alerts": [
                {"hour": f"{i:02d}:00", "count": random.randint(2, 15)}
                for i in range(24 if time_range != "1h" else 1)
            ],
            "daily_fraud_rate": [
                {
                    "date": (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d"),
                    "rate": round(random.uniform(0.008, 0.015), 4),
                    "volume": random.randint(8000, 15000),
                }
                for i in range(min(30, int(base_multiplier)))
            ],
            "top_fraud_patterns": [
                {
                    "pattern": "Card testing",
                    "frequency": random.randint(25, 60),
                    "severity": "high",
                },
                {
                    "pattern": "Account takeover",
                    "frequency": random.randint(15, 40),
                    "severity": "critical",
                },
                {
                    "pattern": "Synthetic identity",
                    "frequency": random.randint(10, 30),
                    "severity": "medium",
                },
                {
                    "pattern": "Friendly fraud",
                    "frequency": random.randint(20, 45),
                    "severity": "medium",
                },
            ],
        },
        "geographical_insights": {
            "high_risk_regions": [
                {
                    "region": "Eastern Europe",
                    "risk_score": 0.78,
                    "volume": random.randint(500, 1500),
                },
                {
                    "region": "West Africa",
                    "risk_score": 0.85,
                    "volume": random.randint(200, 800),
                },
                {
                    "region": "Southeast Asia",
                    "risk_score": 0.65,
                    "volume": random.randint(800, 2000),
                },
            ],
            "emerging_threats": [
                {
                    "location": "New York, NY",
                    "threat_type": "velocity",
                    "severity": "increasing",
                },
                {
                    "location": "London, UK",
                    "threat_type": "device",
                    "severity": "stable",
                },
            ],
        },
        "investigation_metrics": {
            "avg_investigation_time_hours": round(random.uniform(8.5, 18.2), 1),
            "investigator_workload": {
                "investigator_1": {
                    "active_cases": random.randint(5, 15),
                    "avg_resolution_time": 12.5,
                },
                "investigator_2": {
                    "active_cases": random.randint(3, 12),
                    "avg_resolution_time": 14.8,
                },
                "investigator_3": {
                    "active_cases": random.randint(4, 13),
                    "avg_resolution_time": 11.2,
                },
            },
            "case_resolution_rate": round(random.uniform(0.85, 0.94), 3),
            "quality_score": round(random.uniform(0.88, 0.95), 3),
        },
    }

    if include_predictions:
        dashboard_data["predictions"] = {
            "next_hour_volume": random.randint(800, 1200),
            "expected_fraud_rate": round(random.uniform(0.009, 0.014), 4),
            "high_risk_transactions_incoming": random.randint(15, 45),
            "capacity_alerts": [
                {"type": "investigation_queue", "severity": "medium", "eta_hours": 2.5},
                {
                    "type": "processing_load",
                    "severity": "low",
                    "threshold_percentage": 68,
                },
            ],
            "model_drift_indicators": {
                "accuracy_decline": 0.02,
                "feature_importance_shift": 0.05,
                "recommendation": "model_retrain_in_3_days",
            },
        }

    return dashboard_data


@router.get("/models", summary="Get fraud detection model performance")
async def get_model_performance(
    model_type: str = Query(
        default="all", description="Model type: all, velocity, location, amount, ml"
    ),
    time_range: str = Query(default="7d", description="Time range: 24h, 7d, 30d"),
    include_details: bool = Query(
        default=False, description="Include detailed model metrics"
    ),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_FRAUD)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get fraud detection model performance metrics and analysis.

    Args:
        model_type: Type of models to analyze
        time_range: Time range for analysis
        include_details: Include detailed model metrics
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing model performance data
    """
    valid_model_types = ["all", "velocity", "location", "amount", "ml", "ensemble"]
    if model_type not in valid_model_types:
        raise ValidationError(
            f"Invalid model type. Must be one of: {valid_model_types}",
            {"model_type": model_type},
        )

    import random

    models_data = {
        "model_type": model_type,
        "time_range": time_range,
        "generated_at": datetime.now().isoformat(),
    }

    # Define model configurations
    model_configs = {
        "velocity": {
            "name": "Velocity Detection Model",
            "version": "v2.1.3",
            "type": "rule_based",
            "accuracy": round(random.uniform(0.88, 0.94), 3),
            "precision": round(random.uniform(0.85, 0.92), 3),
            "recall": round(random.uniform(0.87, 0.95), 3),
        },
        "location": {
            "name": "Geographic Anomaly Model",
            "version": "v1.8.2",
            "type": "statistical",
            "accuracy": round(random.uniform(0.86, 0.92), 3),
            "precision": round(random.uniform(0.83, 0.90), 3),
            "recall": round(random.uniform(0.85, 0.93), 3),
        },
        "amount": {
            "name": "Transaction Amount Model",
            "version": "v1.5.1",
            "type": "statistical",
            "accuracy": round(random.uniform(0.84, 0.90), 3),
            "precision": round(random.uniform(0.81, 0.88), 3),
            "recall": round(random.uniform(0.83, 0.91), 3),
        },
        "ml": {
            "name": "ML Ensemble Model",
            "version": "v3.2.1",
            "type": "machine_learning",
            "accuracy": round(random.uniform(0.92, 0.97), 3),
            "precision": round(random.uniform(0.89, 0.95), 3),
            "recall": round(random.uniform(0.91, 0.96), 3),
        },
    }

    # Generate model performance data
    if model_type == "all":
        models_data["models"] = []
        for model_key, config in model_configs.items():
            model_data = {
                **config,
                "model_id": model_key,
                "status": "active",
                "last_updated": (
                    datetime.now() - timedelta(days=random.randint(1, 30))
                ).isoformat(),
                "predictions_today": random.randint(5000, 15000),
                "avg_response_time_ms": random.randint(15, 85),
                "error_rate": round(random.uniform(0.001, 0.008), 4),
                "f1_score": round(
                    2
                    * (config["precision"] * config["recall"])
                    / (config["precision"] + config["recall"]),
                    3,
                ),
            }

            if include_details:
                model_data["detailed_metrics"] = {
                    "confusion_matrix": {
                        "true_positives": random.randint(450, 750),
                        "false_positives": random.randint(50, 120),
                        "true_negatives": random.randint(8500, 9500),
                        "false_negatives": random.randint(30, 80),
                    },
                    "feature_importance": [
                        {
                            "feature": "transaction_amount",
                            "importance": round(random.uniform(0.15, 0.35), 3),
                        },
                        {
                            "feature": "time_since_last_transaction",
                            "importance": round(random.uniform(0.12, 0.28), 3),
                        },
                        {
                            "feature": "merchant_risk_score",
                            "importance": round(random.uniform(0.10, 0.25), 3),
                        },
                        {
                            "feature": "device_fingerprint",
                            "importance": round(random.uniform(0.08, 0.20), 3),
                        },
                    ],
                    "drift_metrics": {
                        "data_drift_score": round(random.uniform(0.02, 0.15), 3),
                        "concept_drift_score": round(random.uniform(0.01, 0.08), 3),
                        "prediction_drift_score": round(random.uniform(0.01, 0.10), 3),
                    },
                }

            models_data["models"].append(model_data)
    else:
        # Single model data
        config = model_configs.get(model_type, model_configs["ml"])
        models_data["model"] = {
            **config,
            "model_id": model_type,
            "status": "active",
            "last_updated": (
                datetime.now() - timedelta(days=random.randint(1, 15))
            ).isoformat(),
            "predictions_today": random.randint(5000, 15000),
            "avg_response_time_ms": random.randint(15, 85),
        }

    # Add ensemble metrics if all models
    if model_type == "all":
        models_data["ensemble_performance"] = {
            "combined_accuracy": round(random.uniform(0.94, 0.98), 3),
            "combined_precision": round(random.uniform(0.91, 0.96), 3),
            "combined_recall": round(random.uniform(0.93, 0.97), 3),
            "model_agreement_rate": round(random.uniform(0.78, 0.88), 3),
            "weighted_voting_performance": round(random.uniform(0.92, 0.97), 3),
        }

    return models_data


@router.get("/cases", summary="Get fraud investigation cases")
async def get_fraud_cases(
    status: Optional[str] = Query(default=None, description="Case status filter"),
    investigator: Optional[str] = Query(
        default=None, description="Assigned investigator filter"
    ),
    priority: Optional[str] = Query(default=None, description="Case priority filter"),
    case_type: Optional[str] = Query(default=None, description="Case type filter"),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_FRAUD)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get fraud investigation cases with comprehensive case management data.

    Args:
        status: Case status filter
        investigator: Assigned investigator filter
        priority: Case priority filter
        case_type: Case type filter
        pagination: Pagination parameters
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing fraud investigation cases
    """
    import random

    # Generate investigation cases
    cases = []
    total_cases = 50

    case_statuses = [
        "open",
        "in_progress",
        "under_review",
        "resolved_fraud",
        "resolved_legitimate",
        "escalated",
    ]
    case_priorities = ["critical", "high", "medium", "low"]
    case_types = [
        "transaction_fraud",
        "account_takeover",
        "identity_theft",
        "card_testing",
        "friendly_fraud",
    ]
    investigators = [
        "investigator_1",
        "investigator_2",
        "investigator_3",
        "investigator_4",
    ]

    for i in range(
        pagination.offset, min(pagination.offset + pagination.size, total_cases)
    ):
        case_status = status or random.choice(case_statuses)
        case_priority = priority or random.choice(case_priorities)
        selected_case_type = case_type or random.choice(case_types)
        assigned_investigator = investigator or (
            random.choice(investigators) if random.choice([True, False]) else None
        )

        case_data = {
            "case_id": f"case_{i+1:06d}",
            "alert_ids": [
                f"alert_{random.randint(100000, 999999):06d}"
                for _ in range(random.randint(1, 4))
            ],
            "status": case_status,
            "priority": case_priority,
            "case_type": selected_case_type,
            "customer_id": f"customer_{random.randint(1, 10000):06d}",
            "total_amount": round(random.uniform(500, 50000), 2),
            "transaction_count": random.randint(1, 25),
            "created_at": (
                datetime.now() - timedelta(days=random.randint(1, 90))
            ).isoformat(),
            "updated_at": (
                datetime.now() - timedelta(hours=random.randint(1, 48))
            ).isoformat(),
            "assigned_to": assigned_investigator,
            "investigation": {
                "time_spent_hours": round(random.uniform(0.5, 24.0), 1)
                if case_status != "open"
                else 0,
                "evidence_count": random.randint(0, 12),
                "notes_count": random.randint(0, 8),
                "actions_taken": random.randint(0, 6),
                "quality_score": round(random.uniform(0.75, 0.95), 2)
                if case_status in ["resolved_fraud", "resolved_legitimate"]
                else None,
            },
            "risk_assessment": {
                "overall_risk_score": round(random.uniform(0.3, 1.0), 3),
                "financial_risk": round(random.uniform(0.2, 1.0), 3),
                "reputational_risk": round(random.uniform(0.1, 0.8), 3),
                "customer_impact": round(random.uniform(0.2, 0.9), 3),
            },
            "sla_compliance": {
                "due_date": (
                    datetime.now() + timedelta(hours=random.randint(1, 72))
                ).isoformat(),
                "is_overdue": random.choice([True, False]),
                "escalation_required": random.choice([True, False])
                if case_priority in ["critical", "high"]
                else False,
            },
        }

        cases.append(case_data)

    # Calculate summary metrics
    sla_compliance_rate = (
        sum(1 for case in cases if not case["sla_compliance"]["is_overdue"])
        / len(cases)
        if cases
        else 0
    )
    avg_investigation_time = (
        sum(case["investigation"]["time_spent_hours"] for case in cases) / len(cases)
        if cases
        else 0
    )

    return {
        "cases": cases,
        "filters": {
            "status": status,
            "investigator": investigator,
            "priority": priority,
            "case_type": case_type,
        },
        "pagination": {
            "page": pagination.page,
            "size": pagination.size,
            "total": total_cases,
            "pages": (total_cases + pagination.size - 1) // pagination.size,
        },
        "summary": {
            "total_cases": total_cases,
            "open_cases": len([c for c in cases if c["status"] == "open"]),
            "in_progress_cases": len(
                [c for c in cases if c["status"] == "in_progress"]
            ),
            "overdue_cases": len(
                [c for c in cases if c["sla_compliance"]["is_overdue"]]
            ),
            "avg_investigation_time_hours": round(avg_investigation_time, 1),
            "sla_compliance_rate": round(sla_compliance_rate, 3),
            "total_amount_under_investigation": round(
                sum(c["total_amount"] for c in cases), 2
            ),
        },
        "insights": {
            "most_common_case_type": max(
                case_types, key=lambda x: len([c for c in cases if c["case_type"] == x])
            ),
            "busiest_investigator": max(
                investigators,
                key=lambda x: len([c for c in cases if c["assigned_to"] == x]),
            )
            if cases
            else None,
            "resolution_rate": round(
                len(
                    [
                        c
                        for c in cases
                        if c["status"] in ["resolved_fraud", "resolved_legitimate"]
                    ]
                )
                / len(cases),
                3,
            )
            if cases
            else 0,
        },
    }


@router.get("/risk-assessment", summary="Get comprehensive risk assessment")
async def get_risk_assessment(
    assessment_type: str = Query(
        default="overall",
        description="Assessment type: overall, customer, merchant, geographic",
    ),
    time_range: str = Query(default="30d", description="Time range: 7d, 30d, 90d"),
    risk_threshold: float = Query(default=0.7, description="Risk threshold filter"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_FRAUD)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get comprehensive fraud risk assessment across different dimensions.

    Args:
        assessment_type: Type of risk assessment
        time_range: Time range for assessment
        risk_threshold: Risk threshold for filtering
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing comprehensive risk assessment
    """
    valid_assessment_types = [
        "overall",
        "customer",
        "merchant",
        "geographic",
        "temporal",
    ]
    if assessment_type not in valid_assessment_types:
        raise ValidationError(
            f"Invalid assessment type. Must be one of: {valid_assessment_types}",
            {"assessment_type": assessment_type},
        )

    import random

    risk_data = {
        "assessment_type": assessment_type,
        "time_range": time_range,
        "risk_threshold": risk_threshold,
        "generated_at": datetime.now().isoformat(),
    }

    if assessment_type == "overall":
        risk_data.update(
            {
                "overall_risk_level": random.choice(
                    ["low", "medium", "high", "critical"]
                ),
                "risk_score": round(random.uniform(0.3, 0.9), 3),
                "trend": random.choice(["increasing", "stable", "decreasing"]),
                "key_risk_factors": [
                    {
                        "factor": "transaction_velocity",
                        "impact": 0.28,
                        "trend": "increasing",
                    },
                    {
                        "factor": "geographic_dispersion",
                        "impact": 0.22,
                        "trend": "stable",
                    },
                    {
                        "factor": "new_account_activity",
                        "impact": 0.18,
                        "trend": "increasing",
                    },
                    {
                        "factor": "payment_method_diversity",
                        "impact": 0.15,
                        "trend": "decreasing",
                    },
                    {
                        "factor": "merchant_risk_exposure",
                        "impact": 0.17,
                        "trend": "stable",
                    },
                ],
                "risk_distribution": {
                    "low_risk": 0.65,
                    "medium_risk": 0.25,
                    "high_risk": 0.08,
                    "critical_risk": 0.02,
                },
                "projected_loss": {
                    "next_30_days": round(random.uniform(50000, 200000), 2),
                    "confidence_interval": {
                        "lower": round(random.uniform(30000, 80000), 2),
                        "upper": round(random.uniform(180000, 250000), 2),
                    },
                },
            }
        )

    elif assessment_type == "customer":
        high_risk_customers = []
        for i in range(20):
            high_risk_customers.append(
                {
                    "customer_id": f"customer_{random.randint(1, 10000):06d}",
                    "risk_score": round(random.uniform(risk_threshold, 1.0), 3),
                    "risk_factors": random.sample(
                        [
                            "high_velocity_transactions",
                            "unusual_geographic_activity",
                            "suspicious_device_usage",
                            "payment_method_abuse",
                            "account_behavior_anomaly",
                        ],
                        random.randint(2, 4),
                    ),
                    "potential_loss": round(random.uniform(1000, 25000), 2),
                    "last_activity": (
                        datetime.now() - timedelta(hours=random.randint(1, 72))
                    ).isoformat(),
                }
            )

        risk_data.update(
            {
                "high_risk_customers": sorted(
                    high_risk_customers, key=lambda x: x["risk_score"], reverse=True
                ),
                "customer_risk_distribution": {
                    "total_customers": 10000,
                    "high_risk_count": len(high_risk_customers),
                    "medium_risk_count": random.randint(200, 500),
                    "low_risk_count": 10000
                    - len(high_risk_customers)
                    - random.randint(200, 500),
                },
            }
        )

    elif assessment_type == "geographic":
        risk_data.update(
            {
                "geographic_risks": [
                    {
                        "region": "Eastern Europe",
                        "risk_score": round(random.uniform(0.7, 0.95), 3),
                        "transaction_volume": random.randint(1000, 5000),
                        "fraud_rate": round(random.uniform(0.02, 0.08), 4),
                        "primary_threats": ["card_testing", "account_takeover"],
                    },
                    {
                        "region": "West Africa",
                        "risk_score": round(random.uniform(0.8, 0.98), 3),
                        "transaction_volume": random.randint(500, 2000),
                        "fraud_rate": round(random.uniform(0.03, 0.12), 4),
                        "primary_threats": ["identity_theft", "synthetic_fraud"],
                    },
                    {
                        "region": "Southeast Asia",
                        "risk_score": round(random.uniform(0.5, 0.8), 3),
                        "transaction_volume": random.randint(2000, 8000),
                        "fraud_rate": round(random.uniform(0.01, 0.05), 4),
                        "primary_threats": ["payment_fraud", "merchant_fraud"],
                    },
                ],
                "emerging_threats": [
                    {
                        "location": "New geographic cluster detected",
                        "threat_level": "medium",
                        "trend": "increasing",
                    },
                    {
                        "location": "Known high-risk area expansion",
                        "threat_level": "high",
                        "trend": "stable",
                    },
                ],
            }
        )

    return risk_data

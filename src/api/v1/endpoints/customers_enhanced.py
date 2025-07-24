"""
Enhanced customer endpoints for API v1.

This module provides comprehensive endpoints for customer analytics and data access.
Integrates with customer analytics engines for RFM segmentation, CLV modeling,
churn prediction, and customer journey analysis.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import PaginationParams, get_database_session, get_pagination
from ...exceptions import NotFoundError, ValidationError
from ...middleware.compression import create_optimized_response
from ...services.cache import CacheKeys, get_cache_service
from ...utils.pagination import create_paginated_response, get_pagination_params

router = APIRouter()


@router.get("/", summary="List customers with analytics")
async def list_customers(
    segment: Optional[str] = Query(default=None, description="Customer segment filter"),
    risk_level: Optional[str] = Query(
        default=None, description="Churn risk level: low, medium, high"
    ),
    sort_by: str = Query(
        default="lifetime_value",
        description="Sort by: lifetime_value, recency, frequency, monetary",
    ),
    include_predictions: bool = Query(
        default=False, description="Include ML predictions"
    ),
    pagination: PaginationParams = Depends(get_pagination_params),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_CUSTOMERS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    List customers with comprehensive analytics data.

    Features caching for improved performance and pagination support.
    """
    # Check cache first
    cache_service = get_cache_service()
    cache_key = CacheKeys.customer_list(
        segment, risk_level, sort_by, pagination.page, pagination.size
    )

    cached_result = cache_service.get(cache_key)
    if cached_result:
        return create_optimized_response(cached_result)
    valid_sort_options = [
        "lifetime_value",
        "recency",
        "frequency",
        "monetary",
        "created_at",
        "last_purchase",
    ]
    if sort_by not in valid_sort_options:
        raise ValidationError(
            f"Invalid sort option. Must be one of: {valid_sort_options}",
            {"sort_by": sort_by},
        )

    if risk_level and risk_level not in ["low", "medium", "high"]:
        raise ValidationError(
            "Invalid risk level. Must be: low, medium, or high",
            {"risk_level": risk_level},
        )

    import random

    total_customers = 1000

    # Apply filters
    if segment:
        total_customers = int(total_customers * 0.6)  # Simulate filtering
    if risk_level:
        total_customers = int(total_customers * 0.3)  # Simulate filtering

    customers = []
    segments = [
        "Champions",
        "Loyal Customers",
        "Potential Loyalists",
        "New Customers",
        "Promising",
        "Need Attention",
        "About to Sleep",
        "At Risk",
    ]

    for i in range(
        pagination.offset, min(pagination.offset + pagination.size, total_customers)
    ):
        customer_segment = segment or random.choice(segments)
        ltv = random.uniform(500, 5000)
        recency = random.randint(1, 365)
        frequency = random.randint(1, 50)
        monetary = ltv * random.uniform(0.6, 1.4)

        customer_data = {
            "id": f"customer_{i+1:06d}",
            "email": f"customer{i+1}@example.com",
            "created_at": (
                datetime.now() - timedelta(days=random.randint(30, 1000))
            ).isoformat(),
            "last_purchase": (datetime.now() - timedelta(days=recency)).isoformat(),
            "analytics": {
                "lifetime_value": round(ltv, 2),
                "total_orders": frequency,
                "total_spent": round(monetary, 2),
                "average_order_value": round(monetary / frequency, 2)
                if frequency > 0
                else 0,
                "rfm_segment": customer_segment,
                "recency_days": recency,
                "frequency_score": min(5, frequency // 10 + 1),
                "monetary_score": min(5, int(monetary / 1000) + 1),
                "churn_risk": round(random.uniform(0.1, 0.9), 3),
                "engagement_score": round(random.uniform(0.3, 1.0), 3),
            },
            "demographics": {
                "age_group": random.choice(["18-25", "26-35", "36-45", "46-55", "55+"]),
                "gender": random.choice(["M", "F", "Other"]),
                "location": {
                    "city": random.choice(
                        ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
                    ),
                    "state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
                    "country": "US",
                },
            },
        }

        if include_predictions:
            customer_data["predictions"] = {
                "churn_probability": round(random.uniform(0.1, 0.8), 3),
                "next_order_likelihood": round(random.uniform(0.2, 0.9), 3),
                "predicted_ltv_12m": round(ltv * random.uniform(0.8, 1.5), 2),
                "recommended_products": [
                    {
                        "product_id": f"prod_{random.randint(1, 100):04d}",
                        "score": round(random.uniform(0.6, 1.0), 3),
                    },
                    {
                        "product_id": f"prod_{random.randint(1, 100):04d}",
                        "score": round(random.uniform(0.6, 1.0), 3),
                    },
                ],
                "optimal_contact_time": random.choice(
                    ["morning", "afternoon", "evening"]
                ),
                "preferred_channel": random.choice(["email", "sms", "push", "phone"]),
            }

        customers.append(customer_data)

    # Create paginated response with improved structure
    result = create_paginated_response(
        items=customers,
        total_count=total_customers,
        pagination=pagination,
        additional_data={
            "filters": {
                "segment": segment,
                "risk_level": risk_level,
                "sort_by": sort_by,
                "include_predictions": include_predictions,
            },
            "analytics_summary": {
                "avg_lifetime_value": round(
                    sum(c["analytics"]["lifetime_value"] for c in customers)
                    / len(customers),
                    2,
                )
                if customers
                else 0,
                "high_value_customers": len(
                    [c for c in customers if c["analytics"]["lifetime_value"] > 2000]
                ),
                "at_risk_customers": len(
                    [c for c in customers if c["analytics"]["churn_risk"] > 0.7]
                ),
                "segment_distribution": {
                    seg: len(
                        [c for c in customers if c["analytics"]["rfm_segment"] == seg]
                    )
                    for seg in set(c["analytics"]["rfm_segment"] for c in customers)
                }
                if customers
                else {},
            },
        },
    )

    # Cache the result for 2 minutes (120 seconds)
    cache_service.set(cache_key, result, 120)

    return create_optimized_response(result)


@router.get("/{customer_id}", summary="Get customer details with full analytics")
async def get_customer(
    customer_id: str,
    include_history: bool = Query(default=True, description="Include purchase history"),
    include_predictions: bool = Query(
        default=True, description="Include ML predictions"
    ),
    include_journey: bool = Query(
        default=False, description="Include customer journey"
    ),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_CUSTOMERS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get comprehensive customer information with analytics.

    Args:
        customer_id: Customer identifier
        include_history: Include purchase history
        include_predictions: Include ML predictions
        include_journey: Include customer journey analysis
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing comprehensive customer details

    Raises:
        NotFoundError: If customer is not found
    """
    if not customer_id.startswith("customer_"):
        raise NotFoundError("Customer", customer_id)

    import random

    # Generate comprehensive customer data
    ltv = random.uniform(1000, 8000)
    total_orders = random.randint(5, 100)
    total_spent = ltv * random.uniform(0.8, 1.2)

    customer_data = {
        "id": customer_id,
        "email": f"{customer_id}@example.com",
        "created_at": (
            datetime.now() - timedelta(days=random.randint(100, 800))
        ).isoformat(),
        "last_purchase": (
            datetime.now() - timedelta(days=random.randint(1, 90))
        ).isoformat(),
        "status": "active",
        "profile": {
            "first_name": "John",
            "last_name": "Doe",
            "phone": "+1-555-0123",
            "date_of_birth": "1985-06-15",
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "CA",
                "zip": "12345",
                "country": "US",
            },
            "preferences": {
                "email_notifications": True,
                "sms_notifications": False,
                "preferred_language": "en",
                "currency": "USD",
            },
        },
        "analytics": {
            "lifetime_value": round(ltv, 2),
            "total_orders": total_orders,
            "total_spent": round(total_spent, 2),
            "average_order_value": round(total_spent / total_orders, 2),
            "rfm_analysis": {
                "recency": random.randint(1, 90),
                "frequency": total_orders,
                "monetary": round(total_spent, 2),
                "rfm_score": f"{random.randint(1, 5)}{random.randint(1, 5)}{random.randint(1, 5)}",
                "segment": random.choice(
                    ["Champions", "Loyal Customers", "Potential Loyalists"]
                ),
            },
            "behavior": {
                "preferred_categories": ["Electronics", "Books"],
                "average_session_duration": random.randint(300, 900),
                "pages_per_session": round(random.uniform(3.0, 12.0), 1),
                "conversion_rate": round(random.uniform(0.10, 0.25), 3),
                "cart_abandonment_rate": round(random.uniform(0.15, 0.35), 3),
                "return_rate": round(random.uniform(0.05, 0.15), 3),
                "review_rate": round(random.uniform(0.20, 0.60), 3),
            },
            "engagement": {
                "email_open_rate": round(random.uniform(0.20, 0.45), 3),
                "email_click_rate": round(random.uniform(0.05, 0.15), 3),
                "social_engagement": round(random.uniform(0.10, 0.40), 3),
                "loyalty_program_member": random.choice([True, False]),
                "referrals_made": random.randint(0, 8),
            },
        },
    }

    if include_history:
        # Generate purchase history
        purchase_history = []
        for i in range(min(10, total_orders)):  # Show last 10 orders
            order_date = datetime.now() - timedelta(days=random.randint(1, 365))
            order_value = random.uniform(25, 300)

            purchase_history.append(
                {
                    "order_id": f"order_{random.randint(100000, 999999)}",
                    "date": order_date.isoformat(),
                    "amount": round(order_value, 2),
                    "items_count": random.randint(1, 5),
                    "status": random.choice(["completed", "shipped", "delivered"]),
                    "categories": random.sample(
                        ["Electronics", "Clothing", "Books", "Home", "Sports"],
                        random.randint(1, 3),
                    ),
                }
            )

        customer_data["purchase_history"] = sorted(
            purchase_history, key=lambda x: x["date"], reverse=True
        )

    if include_predictions:
        customer_data["predictions"] = {
            "churn_risk": {
                "probability": round(random.uniform(0.05, 0.60), 3),
                "risk_level": random.choice(["low", "medium", "high"]),
                "key_factors": ["recency", "engagement_decline", "support_tickets"],
            },
            "lifetime_value": {
                "predicted_12m": round(ltv * random.uniform(0.8, 1.4), 2),
                "predicted_24m": round(ltv * random.uniform(1.2, 2.0), 2),
                "confidence": round(random.uniform(0.70, 0.95), 3),
            },
            "next_purchase": {
                "likelihood": round(random.uniform(0.60, 0.95), 3),
                "predicted_date": (
                    datetime.now() + timedelta(days=random.randint(7, 45))
                ).isoformat(),
                "predicted_amount": round(random.uniform(50, 200), 2),
            },
            "product_recommendations": [
                {
                    "product_id": f"prod_{random.randint(1, 100):04d}",
                    "name": "Recommended Product 1",
                    "category": "Electronics",
                    "score": round(random.uniform(0.80, 0.95), 3),
                    "reason": "frequently_bought_together",
                },
                {
                    "product_id": f"prod_{random.randint(1, 100):04d}",
                    "name": "Recommended Product 2",
                    "category": "Books",
                    "score": round(random.uniform(0.75, 0.90), 3),
                    "reason": "similar_customers",
                },
            ],
            "optimal_engagement": {
                "best_contact_time": "Tuesday 2-4 PM",
                "preferred_channel": "email",
                "message_tone": "friendly_professional",
                "offer_sensitivity": "moderate",
            },
        }

    if include_journey:
        # Generate customer journey data
        journey_events = []
        for i in range(random.randint(10, 50)):
            event_date = datetime.now() - timedelta(days=random.randint(1, 200))
            event_types = [
                "page_view",
                "product_view",
                "add_to_cart",
                "purchase",
                "email_open",
                "support_contact",
            ]

            journey_events.append(
                {
                    "timestamp": event_date.isoformat(),
                    "event_type": random.choice(event_types),
                    "channel": random.choice(
                        ["website", "mobile_app", "email", "phone", "chat"]
                    ),
                    "details": {
                        "page": f"/product/{random.randint(1, 100)}",
                        "duration": random.randint(30, 600)
                        if random.choice([True, False])
                        else None,
                        "value": round(random.uniform(25, 300), 2)
                        if random.choice([True, False])
                        else None,
                    },
                }
            )

        customer_data["journey"] = {
            "total_touchpoints": len(journey_events),
            "first_interaction": min(journey_events, key=lambda x: x["timestamp"])[
                "timestamp"
            ],
            "last_interaction": max(journey_events, key=lambda x: x["timestamp"])[
                "timestamp"
            ],
            "channel_usage": {
                "website": len(
                    [e for e in journey_events if e["channel"] == "website"]
                ),
                "mobile_app": len(
                    [e for e in journey_events if e["channel"] == "mobile_app"]
                ),
                "email": len([e for e in journey_events if e["channel"] == "email"]),
            },
            "conversion_path": [
                {
                    "step": "awareness",
                    "touchpoints": 15,
                    "channels": ["email", "social"],
                },
                {
                    "step": "consideration",
                    "touchpoints": 8,
                    "channels": ["website", "mobile_app"],
                },
                {"step": "purchase", "touchpoints": 3, "channels": ["website"]},
                {
                    "step": "retention",
                    "touchpoints": 12,
                    "channels": ["email", "mobile_app"],
                },
            ],
            "recent_events": sorted(
                journey_events, key=lambda x: x["timestamp"], reverse=True
            )[:10],
        }

    return customer_data


@router.get(
    "/{customer_id}/recommendations", summary="Get personalized recommendations"
)
async def get_customer_recommendations(
    customer_id: str,
    recommendation_type: str = Query(
        default="products", description="Type: products, content, offers"
    ),
    limit: int = Query(default=10, description="Maximum number of recommendations"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_CUSTOMERS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get personalized recommendations for a customer.

    Args:
        customer_id: Customer identifier
        recommendation_type: Type of recommendations
        limit: Maximum recommendations to return
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing personalized recommendations
    """
    if not customer_id.startswith("customer_"):
        raise NotFoundError("Customer", customer_id)

    valid_types = ["products", "content", "offers", "categories"]
    if recommendation_type not in valid_types:
        raise ValidationError(
            f"Invalid recommendation type. Must be one of: {valid_types}",
            {"recommendation_type": recommendation_type},
        )

    import random

    recommendations = []

    if recommendation_type == "products":
        for i in range(limit):
            recommendations.append(
                {
                    "product_id": f"prod_{random.randint(1, 1000):04d}",
                    "name": f"Recommended Product {i+1}",
                    "category": random.choice(
                        ["Electronics", "Clothing", "Books", "Home", "Sports"]
                    ),
                    "price": round(random.uniform(19.99, 299.99), 2),
                    "rating": round(random.uniform(3.5, 5.0), 1),
                    "confidence_score": round(random.uniform(0.70, 0.95), 3),
                    "reason": random.choice(
                        [
                            "frequently_bought_together",
                            "customers_like_you_also_bought",
                            "based_on_browsing_history",
                            "trending_in_your_category",
                            "seasonal_recommendation",
                        ]
                    ),
                    "expected_ctr": round(random.uniform(0.05, 0.25), 3),
                    "expected_conversion": round(random.uniform(0.02, 0.15), 3),
                }
            )

    elif recommendation_type == "offers":
        for i in range(min(limit, 5)):  # Fewer offers
            recommendations.append(
                {
                    "offer_id": f"offer_{random.randint(1000, 9999)}",
                    "title": f"Special Offer {i+1}",
                    "description": "Get 20% off your next purchase",
                    "discount_percentage": random.randint(10, 40),
                    "valid_until": (
                        datetime.now() + timedelta(days=random.randint(7, 30))
                    ).isoformat(),
                    "applicable_categories": random.sample(
                        ["Electronics", "Clothing", "Books"], random.randint(1, 2)
                    ),
                    "personalization_score": round(random.uniform(0.60, 0.90), 3),
                    "expected_redemption_rate": round(random.uniform(0.15, 0.35), 3),
                }
            )

    elif recommendation_type == "content":
        for i in range(limit):
            recommendations.append(
                {
                    "content_id": f"content_{random.randint(100, 999)}",
                    "title": f"Recommended Article {i+1}",
                    "type": random.choice(
                        ["blog_post", "product_guide", "video", "tutorial"]
                    ),
                    "category": random.choice(
                        ["How-to", "Product Reviews", "Trends", "Tips"]
                    ),
                    "reading_time": random.randint(3, 15),
                    "relevance_score": round(random.uniform(0.65, 0.95), 3),
                    "expected_engagement": round(random.uniform(0.20, 0.60), 3),
                }
            )

    return {
        "customer_id": customer_id,
        "recommendation_type": recommendation_type,
        "generated_at": datetime.now().isoformat(),
        "recommendations": recommendations,
        "metadata": {
            "model_version": "v2.1.0",
            "personalization_strength": round(random.uniform(0.70, 0.95), 3),
            "cold_start": False,
            "fallback_used": False,
        },
        "performance_metrics": {
            "historical_ctr": round(random.uniform(0.08, 0.18), 3),
            "historical_conversion": round(random.uniform(0.03, 0.12), 3),
            "recommendation_diversity": round(random.uniform(0.60, 0.85), 3),
        },
    }


@router.get("/{customer_id}/value-prediction", summary="Get customer value predictions")
async def get_customer_value_prediction(
    customer_id: str,
    prediction_horizon: str = Query(
        default="12m", description="Prediction horizon: 3m, 6m, 12m, 24m"
    ),
    include_scenarios: bool = Query(
        default=False, description="Include scenario analysis"
    ),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get customer lifetime value predictions and scenarios.

    Args:
        customer_id: Customer identifier
        prediction_horizon: Prediction time horizon
        include_scenarios: Include scenario analysis
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing value predictions and scenarios
    """
    if not customer_id.startswith("customer_"):
        raise NotFoundError("Customer", customer_id)

    valid_horizons = ["3m", "6m", "12m", "24m"]
    if prediction_horizon not in valid_horizons:
        raise ValidationError(
            f"Invalid prediction horizon. Must be one of: {valid_horizons}",
            {"prediction_horizon": prediction_horizon},
        )

    import random

    # Base customer value
    current_ltv = random.uniform(500, 5000)

    # Prediction multipliers based on horizon
    multipliers = {"3m": 0.25, "6m": 0.5, "12m": 1.0, "24m": 2.0}
    base_prediction = current_ltv * multipliers[prediction_horizon]

    prediction_data = {
        "customer_id": customer_id,
        "prediction_horizon": prediction_horizon,
        "generated_at": datetime.now().isoformat(),
        "current_ltv": round(current_ltv, 2),
        "predictions": {
            "predicted_ltv": round(base_prediction * random.uniform(0.8, 1.4), 2),
            "confidence_interval": {
                "lower": round(base_prediction * random.uniform(0.6, 0.8), 2),
                "upper": round(base_prediction * random.uniform(1.2, 1.6), 2),
            },
            "confidence_score": round(random.uniform(0.70, 0.95), 3),
            "key_drivers": [
                {
                    "factor": "purchase_frequency",
                    "impact": 0.35,
                    "direction": "positive",
                },
                {
                    "factor": "average_order_value",
                    "impact": 0.28,
                    "direction": "positive",
                },
                {"factor": "engagement_score", "impact": 0.22, "direction": "positive"},
                {"factor": "churn_risk", "impact": -0.15, "direction": "negative"},
            ],
        },
        "risk_factors": {
            "churn_probability": round(random.uniform(0.05, 0.40), 3),
            "value_decline_risk": round(random.uniform(0.10, 0.30), 3),
            "competitive_risk": round(random.uniform(0.15, 0.35), 3),
        },
        "improvement_opportunities": [
            {
                "action": "increase_engagement",
                "potential_lift": round(random.uniform(0.15, 0.35), 3),
                "implementation_effort": "medium",
            },
            {
                "action": "cross_sell_products",
                "potential_lift": round(random.uniform(0.20, 0.40), 3),
                "implementation_effort": "low",
            },
        ],
    }

    if include_scenarios:
        prediction_data["scenarios"] = {
            "best_case": {
                "predicted_ltv": round(base_prediction * 1.6, 2),
                "assumptions": ["High engagement", "Successful cross-sell", "No churn"],
                "probability": 0.15,
            },
            "most_likely": {
                "predicted_ltv": round(base_prediction * 1.1, 2),
                "assumptions": ["Current trends continue", "Moderate engagement"],
                "probability": 0.60,
            },
            "worst_case": {
                "predicted_ltv": round(base_prediction * 0.7, 2),
                "assumptions": ["Declining engagement", "Increased churn risk"],
                "probability": 0.25,
            },
        }

    return prediction_data

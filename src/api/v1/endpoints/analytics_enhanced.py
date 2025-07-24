"""
Enhanced analytics endpoints for API v1.

This module provides comprehensive endpoints for business intelligence and analytics data.
Integrates with the analytics engine modules for customer segmentation, revenue analysis,
product performance, geographic analytics, and marketing attribution.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ...auth.dependencies import get_current_active_user, require_permission
from ...auth.models import Permission, User
from ...dependencies import PaginationParams, get_database_session, get_pagination
from ...exceptions import ValidationError
from ...middleware.compression import create_optimized_response
from ...services.cache import CacheKeys, cached, get_cache_service
from ...utils.pagination import create_paginated_response, get_pagination_params

router = APIRouter()


# Mock data generators for enhanced endpoints
def generate_time_series_data(
    start_date: str, end_date: str, granularity: str
) -> List[Dict]:
    """Generate mock time series data for analytics."""
    import random
    from datetime import datetime, timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if granularity == "daily":
        delta = timedelta(days=1)
    elif granularity == "weekly":
        delta = timedelta(weeks=1)
    else:  # monthly
        delta = timedelta(days=30)

    data = []
    current = start
    base_value = 4000

    while current <= end:
        value = base_value + random.randint(-500, 1000)
        data.append(
            {
                "date": current.strftime("%Y-%m-%d"),
                "revenue": round(value, 2),
                "orders": value // 50,
                "average_order_value": round(value / (value // 50), 2)
                if value > 0
                else 0,
                "profit_margin": round(random.uniform(0.15, 0.35), 3),
                "growth_rate": round(random.uniform(-0.1, 0.2), 3),
            }
        )
        current += delta
        base_value = value  # Use previous value as base for next

    return data


@router.get("/revenue", summary="Get comprehensive revenue analytics")
async def get_revenue_analytics(
    start_date: Optional[str] = Query(
        default=None, description="Start date (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = Query(default=None, description="End date (YYYY-MM-DD)"),
    granularity: str = Query(
        default="daily", description="Data granularity: daily, weekly, monthly"
    ),
    region: Optional[str] = Query(default=None, description="Geographic region filter"),
    category: Optional[str] = Query(
        default=None, description="Product category filter"
    ),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
):
    """
    Get comprehensive revenue analytics with time series data, forecasting, and breakdowns.

    Features caching for improved performance and response compression.
    """
    # Check cache first
    cache_service = get_cache_service()
    cache_key = CacheKeys.analytics_revenue(
        start_date, end_date, granularity, region, category
    )

    cached_result = cache_service.get(cache_key)
    if cached_result:
        return create_optimized_response(cached_result)

    # Validate granularity
    valid_granularities = ["daily", "weekly", "monthly"]
    if granularity not in valid_granularities:
        raise ValidationError(
            f"Invalid granularity. Must be one of: {valid_granularities}",
            {"granularity": granularity},
        )

    # Default date range (last 30 days)
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    # Generate enhanced time series data
    time_series = generate_time_series_data(start_date, end_date, granularity)

    total_revenue = sum(point["revenue"] for point in time_series)
    total_orders = sum(point["orders"] for point in time_series)

    # Generate response data
    result = {
        "period": {
            "start_date": start_date,
            "end_date": end_date,
            "granularity": granularity,
        },
        "filters": {
            "region": region,
            "category": category,
        },
        "summary": {
            "total_revenue": round(total_revenue, 2),
            "total_orders": total_orders,
            "average_order_value": round(total_revenue / total_orders, 2)
            if total_orders > 0
            else 0,
            "profit_margin": round(
                sum(p["profit_margin"] for p in time_series) / len(time_series), 3
            ),
            "growth_rate": round(
                sum(p["growth_rate"] for p in time_series) / len(time_series), 3
            ),
        },
        "time_series": time_series,
        "forecast": {
            "next_period_revenue": round(total_revenue * 1.05, 2),
            "confidence_interval": {
                "lower": round(total_revenue * 0.95, 2),
                "upper": round(total_revenue * 1.15, 2),
            },
            "trend": "increasing"
            if time_series[-1]["growth_rate"] > 0
            else "decreasing",
        },
        "breakdown": {
            "by_category": [
                {
                    "category": "Electronics",
                    "revenue": round(total_revenue * 0.36, 2),
                    "percentage": 36.0,
                },
                {
                    "category": "Clothing",
                    "revenue": round(total_revenue * 0.28, 2),
                    "percentage": 28.0,
                },
                {
                    "category": "Books",
                    "revenue": round(total_revenue * 0.20, 2),
                    "percentage": 20.0,
                },
                {
                    "category": "Other",
                    "revenue": round(total_revenue * 0.16, 2),
                    "percentage": 16.0,
                },
            ],
            "by_region": [
                {
                    "region": "North America",
                    "revenue": round(total_revenue * 0.50, 2),
                    "percentage": 50.0,
                },
                {
                    "region": "Europe",
                    "revenue": round(total_revenue * 0.30, 2),
                    "percentage": 30.0,
                },
                {
                    "region": "Asia",
                    "revenue": round(total_revenue * 0.20, 2),
                    "percentage": 20.0,
                },
            ],
            "by_channel": [
                {
                    "channel": "Online",
                    "revenue": round(total_revenue * 0.70, 2),
                    "percentage": 70.0,
                },
                {
                    "channel": "Mobile App",
                    "revenue": round(total_revenue * 0.25, 2),
                    "percentage": 25.0,
                },
                {
                    "channel": "In-Store",
                    "revenue": round(total_revenue * 0.05, 2),
                    "percentage": 5.0,
                },
            ],
        },
        "cohort_analysis": {
            "retention_rates": {
                "month_1": 0.85,
                "month_3": 0.65,
                "month_6": 0.45,
                "month_12": 0.25,
            },
            "ltv_by_cohort": {
                "jan_2024": 1250.00,
                "feb_2024": 1180.00,
                "mar_2024": 1320.00,
            },
        },
    }

    # Cache the result for 5 minutes (300 seconds)
    cache_service.set(cache_key, result, 300)

    return create_optimized_response(result)


@router.get("/customers/segments", summary="Get customer segmentation analytics")
async def get_customer_segmentation(
    segment_type: str = Query(
        default="rfm", description="Segmentation type: rfm, behavior, geographic"
    ),
    include_predictions: bool = Query(
        default=False, description="Include ML predictions"
    ),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get customer segmentation analytics including RFM analysis and predictions.

    Args:
        segment_type: Type of segmentation analysis
        include_predictions: Whether to include ML predictions
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing customer segmentation data
    """
    valid_types = ["rfm", "behavior", "geographic", "value"]
    if segment_type not in valid_types:
        raise ValidationError(
            f"Invalid segment type. Must be one of: {valid_types}",
            {"segment_type": segment_type},
        )

    base_segments = {
        "Champions": {
            "count": 1250,
            "percentage": 12.5,
            "avg_clv": 3500.00,
            "avg_orders": 25,
        },
        "Loyal Customers": {
            "count": 2100,
            "percentage": 21.0,
            "avg_clv": 2800.00,
            "avg_orders": 18,
        },
        "Potential Loyalists": {
            "count": 1800,
            "percentage": 18.0,
            "avg_clv": 2200.00,
            "avg_orders": 12,
        },
        "New Customers": {
            "count": 1500,
            "percentage": 15.0,
            "avg_clv": 800.00,
            "avg_orders": 3,
        },
        "Promising": {
            "count": 1200,
            "percentage": 12.0,
            "avg_clv": 1500.00,
            "avg_orders": 8,
        },
        "Need Attention": {
            "count": 900,
            "percentage": 9.0,
            "avg_clv": 1800.00,
            "avg_orders": 15,
        },
        "About to Sleep": {
            "count": 750,
            "percentage": 7.5,
            "avg_clv": 1200.00,
            "avg_orders": 10,
        },
        "At Risk": {
            "count": 400,
            "percentage": 4.0,
            "avg_clv": 2000.00,
            "avg_orders": 20,
        },
    }

    result = {
        "segment_type": segment_type,
        "generated_at": datetime.now().isoformat(),
        "total_customers": sum(seg["count"] for seg in base_segments.values()),
        "segments": [
            {
                "name": name,
                "count": data["count"],
                "percentage": data["percentage"],
                "characteristics": {
                    "avg_customer_lifetime_value": data["avg_clv"],
                    "avg_orders_per_customer": data["avg_orders"],
                    "revenue_contribution": round(data["count"] * data["avg_clv"], 2),
                },
            }
            for name, data in base_segments.items()
        ],
        "insights": {
            "top_segment": "Loyal Customers",
            "growth_opportunity": "Potential Loyalists",
            "at_risk_revenue": sum(
                seg["count"] * seg["avg_clv"]
                for name, seg in base_segments.items()
                if name in ["About to Sleep", "At Risk"]
            ),
        },
    }

    if segment_type == "rfm":
        result["rfm_breakdown"] = {
            "recency_distribution": {
                "recent": 0.35,
                "moderate": 0.45,
                "distant": 0.20,
            },
            "frequency_distribution": {
                "high": 0.25,
                "medium": 0.50,
                "low": 0.25,
            },
            "monetary_distribution": {
                "high_value": 0.20,
                "medium_value": 0.60,
                "low_value": 0.20,
            },
        }

    if include_predictions:
        result["predictions"] = {
            "churn_risk": {
                "high_risk": 450,
                "medium_risk": 1200,
                "low_risk": 8350,
            },
            "upsell_opportunities": 2800,
            "cross_sell_potential": 3200,
            "predicted_ltv_increase": {
                "Champions": 15.2,
                "Loyal Customers": 12.8,
                "Potential Loyalists": 25.5,
            },
        }

    return result


@router.get("/products/performance", summary="Get product performance analytics")
async def get_product_performance(
    category: Optional[str] = Query(
        default=None, description="Product category filter"
    ),
    sort_by: str = Query(
        default="revenue", description="Sort by: revenue, units, margin, rating, growth"
    ),
    time_range: str = Query(default="30d", description="Time range: 7d, 30d, 90d, 1y"),
    include_recommendations: bool = Query(
        default=False, description="Include recommendation insights"
    ),
    pagination: PaginationParams = Depends(get_pagination),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get comprehensive product performance analytics with recommendations.

    Args:
        category: Product category filter
        sort_by: Sort criteria
        time_range: Analysis time range
        include_recommendations: Include product recommendations
        pagination: Pagination parameters
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing product performance analytics
    """
    valid_sort_options = [
        "revenue",
        "units",
        "margin",
        "rating",
        "growth",
        "inventory_turnover",
    ]
    if sort_by not in valid_sort_options:
        raise ValidationError(
            f"Invalid sort option. Must be one of: {valid_sort_options}",
            {"sort_by": sort_by},
        )

    valid_time_ranges = ["7d", "30d", "90d", "1y"]
    if time_range not in valid_time_ranges:
        raise ValidationError(
            f"Invalid time range. Must be one of: {valid_time_ranges}",
            {"time_range": time_range},
        )

    # Generate mock product data
    import random

    products = []
    total_products = 100 if not category else 25

    for i in range(
        pagination.offset, min(pagination.offset + pagination.size, total_products)
    ):
        revenue = (i + 1) * 125.50 + random.randint(-50, 200)
        units = (i + 1) * 10 + random.randint(-5, 15)
        margin = 0.25 + (i % 10) * 0.02 + random.uniform(-0.05, 0.05)

        products.append(
            {
                "product_id": f"prod_{i+1:04d}",
                "name": f"Product {i+1}",
                "category": category
                or random.choice(["Electronics", "Clothing", "Books", "Home"]),
                "sku": f"SKU-{i+1:04d}",
                "revenue": round(revenue, 2),
                "units_sold": max(0, units),
                "average_rating": round(4.0 + random.uniform(-0.8, 1.0), 1),
                "margin": round(max(0, margin), 3),
                "growth_rate": round(random.uniform(-0.2, 0.3), 3),
                "inventory_status": random.choice(
                    ["in_stock", "low_stock", "out_of_stock"]
                ),
                "inventory_turnover": round(random.uniform(2.0, 12.0), 1),
                "return_rate": round(random.uniform(0.02, 0.15), 3),
                "conversion_rate": round(random.uniform(0.05, 0.25), 3),
            }
        )

    # Calculate summary metrics
    total_revenue = sum(p["revenue"] for p in products)
    total_units = sum(p["units_sold"] for p in products)
    avg_margin = sum(p["margin"] for p in products) / len(products) if products else 0

    result = {
        "products": products,
        "filters": {
            "category": category,
            "sort_by": sort_by,
            "time_range": time_range,
        },
        "pagination": {
            "page": pagination.page,
            "size": pagination.size,
            "total": total_products,
            "pages": (total_products + pagination.size - 1) // pagination.size,
        },
        "summary": {
            "total_revenue": round(total_revenue, 2),
            "total_units": total_units,
            "average_margin": round(avg_margin, 3),
            "avg_rating": round(
                sum(p["average_rating"] for p in products) / len(products), 1
            )
            if products
            else 0,
            "top_performing_category": "Electronics",
            "fastest_growing_product": products[0]["product_id"] if products else None,
        },
        "trends": {
            "revenue_growth": 0.125,
            "margin_trend": "stable",
            "inventory_health": "good",
            "return_rate_trend": "decreasing",
        },
    }

    if include_recommendations:
        result["recommendations"] = {
            "upsell_opportunities": [
                {
                    "product_id": "prod_0001",
                    "potential_revenue": 1250.00,
                    "confidence": 0.85,
                },
                {
                    "product_id": "prod_0015",
                    "potential_revenue": 980.00,
                    "confidence": 0.78,
                },
            ],
            "cross_sell_bundles": [
                {
                    "primary_product": "prod_0010",
                    "recommended_products": ["prod_0011", "prod_0012"],
                    "bundle_revenue": 450.00,
                    "lift": 0.25,
                },
            ],
            "inventory_optimization": [
                {
                    "product_id": "prod_0020",
                    "action": "increase_stock",
                    "priority": "high",
                },
                {
                    "product_id": "prod_0035",
                    "action": "reduce_stock",
                    "priority": "medium",
                },
            ],
            "pricing_recommendations": [
                {
                    "product_id": "prod_0008",
                    "current_price": 99.99,
                    "recommended_price": 109.99,
                    "expected_lift": 0.15,
                },
            ],
        }

    return result


@router.get("/real-time/metrics", summary="Get real-time analytics metrics")
async def get_realtime_metrics(
    metric_types: Optional[str] = Query(
        default="all",
        description="Metric types: sales, traffic, inventory, quality, all",
    ),
    time_window: str = Query(default="1h", description="Time window: 5m, 15m, 1h, 4h"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get real-time analytics metrics for operational monitoring.

    Args:
        metric_types: Types of metrics to include
        time_window: Time window for metrics aggregation
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing real-time metrics
    """
    valid_types = ["sales", "traffic", "inventory", "quality", "all"]
    if metric_types not in valid_types:
        raise ValidationError(
            f"Invalid metric types. Must be one of: {valid_types}",
            {"metric_types": metric_types},
        )

    valid_windows = ["5m", "15m", "1h", "4h"]
    if time_window not in valid_windows:
        raise ValidationError(
            f"Invalid time window. Must be one of: {valid_windows}",
            {"time_window": time_window},
        )

    import random

    current_time = datetime.now()

    metrics = {
        "timestamp": current_time.isoformat(),
        "time_window": time_window,
        "metric_types": metric_types,
    }

    if metric_types in ["sales", "all"]:
        metrics["sales"] = {
            "current_revenue": round(random.uniform(1000, 5000), 2),
            "orders_per_minute": random.randint(5, 25),
            "conversion_rate": round(random.uniform(0.08, 0.18), 3),
            "average_order_value": round(random.uniform(45, 85), 2),
            "abandoned_carts": random.randint(15, 40),
            "top_selling_products": [
                {"product_id": "prod_0001", "units": 25, "revenue": 1250.00},
                {"product_id": "prod_0015", "units": 18, "revenue": 980.00},
            ],
        }

    if metric_types in ["traffic", "all"]:
        metrics["traffic"] = {
            "active_users": random.randint(500, 2000),
            "page_views": random.randint(5000, 15000),
            "session_duration_avg": round(random.uniform(180, 420), 0),
            "bounce_rate": round(random.uniform(0.25, 0.45), 3),
            "new_vs_returning": {
                "new_users": random.randint(200, 800),
                "returning_users": random.randint(300, 1200),
            },
            "traffic_sources": {
                "organic": 0.35,
                "direct": 0.25,
                "social": 0.20,
                "paid": 0.15,
                "email": 0.05,
            },
        }

    if metric_types in ["inventory", "all"]:
        metrics["inventory"] = {
            "low_stock_items": random.randint(5, 25),
            "out_of_stock_items": random.randint(0, 8),
            "inventory_turnover": round(random.uniform(6.0, 12.0), 1),
            "stockout_risk": random.randint(10, 35),
            "fast_moving_items": [
                {"product_id": "prod_0010", "velocity": 2.5},
                {"product_id": "prod_0025", "velocity": 2.1},
            ],
        }

    if metric_types in ["quality", "all"]:
        metrics["data_quality"] = {
            "stream_health": "healthy",
            "processing_latency_ms": random.randint(50, 200),
            "error_rate": round(random.uniform(0.001, 0.01), 4),
            "data_completeness": round(random.uniform(0.95, 0.99), 3),
            "anomaly_count": random.randint(0, 5),
            "validation_errors": random.randint(0, 10),
        }

    # Add performance indicators
    metrics["system_health"] = {
        "api_response_time_ms": random.randint(50, 150),
        "database_connections": random.randint(15, 45),
        "cache_hit_rate": round(random.uniform(0.85, 0.95), 3),
        "queue_depth": random.randint(0, 100),
    }

    return metrics


@router.get("/cohort-analysis", summary="Get customer cohort analysis")
async def get_cohort_analysis(
    cohort_type: str = Query(
        default="monthly", description="Cohort type: weekly, monthly, quarterly"
    ),
    metric: str = Query(
        default="retention", description="Metric: retention, revenue, orders"
    ),
    start_date: Optional[str] = Query(default=None, description="Analysis start date"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get customer cohort analysis for retention and revenue tracking.

    Args:
        cohort_type: Type of cohort grouping
        metric: Metric to analyze
        start_date: Analysis start date
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing cohort analysis data
    """
    valid_cohort_types = ["weekly", "monthly", "quarterly"]
    if cohort_type not in valid_cohort_types:
        raise ValidationError(
            f"Invalid cohort type. Must be one of: {valid_cohort_types}",
            {"cohort_type": cohort_type},
        )

    valid_metrics = ["retention", "revenue", "orders"]
    if metric not in valid_metrics:
        raise ValidationError(
            f"Invalid metric. Must be one of: {valid_metrics}",
            {"metric": metric},
        )

    import random

    # Generate cohort data
    cohorts = []
    periods = 12 if cohort_type == "monthly" else (52 if cohort_type == "weekly" else 4)

    for i in range(6):  # 6 cohorts
        cohort_name = f"2024-{i+1:02d}" if cohort_type == "monthly" else f"Q{i+1}-2024"
        size = random.randint(800, 1500)

        cohort_data = {
            "cohort": cohort_name,
            "size": size,
            "periods": [],
        }

        retention_rate = 1.0
        for period in range(min(periods, 12)):  # Show up to 12 periods
            if metric == "retention":
                retention_rate *= random.uniform(0.75, 0.95)
                value = round(retention_rate, 3)
            elif metric == "revenue":
                value = round(size * retention_rate * random.uniform(50, 150), 2)
            else:  # orders
                value = round(size * retention_rate * random.uniform(0.5, 2.0), 1)

            cohort_data["periods"].append(
                {
                    "period": period,
                    "value": value,
                    "percentage": round(retention_rate * 100, 1)
                    if metric == "retention"
                    else None,
                }
            )

        cohorts.append(cohort_data)

    return {
        "cohort_type": cohort_type,
        "metric": metric,
        "generated_at": datetime.now().isoformat(),
        "cohorts": cohorts,
        "summary": {
            "avg_retention_period_1": round(
                sum(c["periods"][0]["value"] if c["periods"] else 0 for c in cohorts)
                / len(cohorts),
                3,
            ),
            "avg_retention_period_6": round(
                sum(
                    c["periods"][5]["value"] if len(c["periods"]) > 5 else 0
                    for c in cohorts
                )
                / len(cohorts),
                3,
            ),
            "best_performing_cohort": cohorts[0]["cohort"],
            "cohort_health": "good",
        },
        "insights": {
            "retention_trend": "stable",
            "churn_pattern": "early_dropoff",
            "revenue_opportunity": "increase_engagement_period_2_to_4",
        },
    }


@router.get("/funnels", summary="Get conversion funnel analysis")
async def get_funnel_analysis(
    funnel_type: str = Query(
        default="purchase",
        description="Funnel type: purchase, registration, subscription",
    ),
    time_range: str = Query(default="30d", description="Time range: 7d, 30d, 90d"),
    segment: Optional[str] = Query(default=None, description="Customer segment filter"),
    current_user: User = Depends(get_current_active_user),
    _: None = Depends(require_permission(Permission.READ_ANALYTICS)),
    db: Session = Depends(get_database_session),
) -> Dict[str, Any]:
    """
    Get conversion funnel analysis for different user journeys.

    Args:
        funnel_type: Type of funnel to analyze
        time_range: Analysis time range
        segment: Customer segment filter
        current_user: Current authenticated user
        db: Database session

    Returns:
        Dict containing funnel analysis data
    """
    valid_funnel_types = ["purchase", "registration", "subscription", "checkout"]
    if funnel_type not in valid_funnel_types:
        raise ValidationError(
            f"Invalid funnel type. Must be one of: {valid_funnel_types}",
            {"funnel_type": funnel_type},
        )

    import random

    if funnel_type == "purchase":
        steps = [
            {"name": "Landing Page", "users": 10000, "conversion_rate": 1.0},
            {"name": "Product View", "users": 6500, "conversion_rate": 0.65},
            {"name": "Add to Cart", "users": 3250, "conversion_rate": 0.50},
            {"name": "Checkout Started", "users": 2275, "conversion_rate": 0.70},
            {"name": "Payment Info", "users": 1820, "conversion_rate": 0.80},
            {"name": "Purchase Complete", "users": 1456, "conversion_rate": 0.80},
        ]
    elif funnel_type == "registration":
        steps = [
            {"name": "Registration Page", "users": 5000, "conversion_rate": 1.0},
            {"name": "Email Entered", "users": 3500, "conversion_rate": 0.70},
            {"name": "Form Completed", "users": 2800, "conversion_rate": 0.80},
            {"name": "Email Verified", "users": 2240, "conversion_rate": 0.80},
            {"name": "Profile Setup", "users": 1792, "conversion_rate": 0.80},
        ]
    else:  # subscription or checkout
        steps = [
            {"name": "Pricing Page", "users": 3000, "conversion_rate": 1.0},
            {"name": "Plan Selected", "users": 1800, "conversion_rate": 0.60},
            {"name": "Account Info", "users": 1440, "conversion_rate": 0.80},
            {"name": "Payment Method", "users": 1152, "conversion_rate": 0.80},
            {"name": "Subscription Active", "users": 921, "conversion_rate": 0.80},
        ]

    # Add drop-off analysis
    for i, step in enumerate(steps):
        if i > 0:
            step["drop_off"] = steps[i - 1]["users"] - step["users"]
            step["drop_off_rate"] = round(
                (steps[i - 1]["users"] - step["users"]) / steps[i - 1]["users"], 3
            )
        else:
            step["drop_off"] = 0
            step["drop_off_rate"] = 0.0

    overall_conversion = steps[-1]["users"] / steps[0]["users"]

    return {
        "funnel_type": funnel_type,
        "time_range": time_range,
        "segment": segment,
        "generated_at": datetime.now().isoformat(),
        "steps": steps,
        "summary": {
            "total_entries": steps[0]["users"],
            "total_conversions": steps[-1]["users"],
            "overall_conversion_rate": round(overall_conversion, 3),
            "biggest_drop_off_step": max(steps[1:], key=lambda x: x["drop_off"])[
                "name"
            ],
            "optimization_priority": "Add to Cart to Checkout",
        },
        "insights": {
            "funnel_health": "good"
            if overall_conversion > 0.10
            else "needs_improvement",
            "key_bottleneck": steps[2]["name"],  # Usually Add to Cart
            "improvement_potential": round((1 - overall_conversion) * 100, 1),
        },
        "recommendations": [
            {
                "step": "Add to Cart",
                "action": "Reduce friction in cart experience",
                "expected_lift": 0.15,
            },
            {
                "step": "Checkout Started",
                "action": "Implement guest checkout option",
                "expected_lift": 0.12,
            },
        ],
    }

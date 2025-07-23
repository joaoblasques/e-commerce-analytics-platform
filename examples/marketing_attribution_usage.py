"""
Marketing Attribution Engine Usage Example

This example demonstrates how to use the MarketingAttribution class for
comprehensive marketing attribution analysis, including multi-touch attribution,
campaign performance tracking, customer acquisition cost analysis, and ROI calculations.
"""

import logging
from datetime import datetime, timedelta
from decimal import Decimal

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.business_intelligence.marketing_attribution import (
    MarketingAttribution,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create and configure Spark session for marketing attribution analysis."""
    return (
        SparkSession.builder.appName("MarketingAttributionExample")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def create_sample_touchpoint_data(spark):
    """Create comprehensive sample touchpoint data."""
    schema = StructType(
        [
            StructField("touchpoint_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("campaign_id", StringType(), False),
            StructField("campaign_name", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("cost", DecimalType(10, 2), True),
            StructField("touchpoint_type", StringType(), False),
            StructField("creative_id", StringType(), True),
            StructField("placement", StringType(), True),
        ]
    )

    # Generate realistic touchpoint data for multiple customer journeys
    base_date = datetime(2024, 1, 1)
    data = []

    # Customer Journey 1: Multi-channel path to conversion
    user_id = "user_001"
    data.extend(
        [
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "organic",
                "seo_campaign",
                "SEO Campaign",
                base_date,
                Decimal("0.00"),
                "click",
                "organic_result",
                "google_search",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "paid_search",
                "ppc_brand",
                "Brand PPC Campaign",
                base_date + timedelta(days=2),
                Decimal("3.50"),
                "click",
                "ad_001",
                "google_ads",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "email",
                "newsletter",
                "Weekly Newsletter",
                base_date + timedelta(days=5),
                Decimal("0.25"),
                "click",
                "email_001",
                "inbox",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "social",
                "facebook_retarget",
                "Facebook Retargeting",
                base_date + timedelta(days=7),
                Decimal("2.75"),
                "click",
                "fb_ad_001",
                "facebook_feed",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "paid_search",
                "ppc_generic",
                "Generic PPC Campaign",
                base_date + timedelta(days=10),
                Decimal("4.20"),
                "click",
                "ad_002",
                "google_ads",
            ),
        ]
    )

    # Customer Journey 2: Social media driven conversion
    user_id = "user_002"
    data.extend(
        [
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "social",
                "instagram_video",
                "Instagram Video Campaign",
                base_date + timedelta(days=1),
                Decimal("1.80"),
                "view",
                "video_001",
                "instagram_stories",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "social",
                "instagram_video",
                "Instagram Video Campaign",
                base_date + timedelta(days=3),
                Decimal("2.10"),
                "click",
                "video_001",
                "instagram_feed",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "email",
                "welcome_series",
                "Welcome Email Series",
                base_date + timedelta(days=4),
                Decimal("0.15"),
                "click",
                "email_002",
                "inbox",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "social",
                "facebook_lookalike",
                "Facebook Lookalike",
                base_date + timedelta(days=6),
                Decimal("1.95"),
                "click",
                "fb_ad_002",
                "facebook_feed",
            ),
        ]
    )

    # Customer Journey 3: Direct and paid combination
    user_id = "user_003"
    data.extend(
        [
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "direct",
                "direct_traffic",
                "Direct Traffic",
                base_date + timedelta(days=1),
                Decimal("0.00"),
                "visit",
                None,
                "direct",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "display",
                "banner_campaign",
                "Display Banner Campaign",
                base_date + timedelta(days=3),
                Decimal("1.25"),
                "view",
                "banner_001",
                "partner_site",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "display",
                "banner_campaign",
                "Display Banner Campaign",
                base_date + timedelta(days=5),
                Decimal("1.45"),
                "click",
                "banner_001",
                "partner_site",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "paid_search",
                "ppc_competitor",
                "Competitor PPC Campaign",
                base_date + timedelta(days=8),
                Decimal("5.80"),
                "click",
                "ad_003",
                "google_ads",
            ),
        ]
    )

    # Customer Journey 4: Email marketing focused
    user_id = "user_004"
    data.extend(
        [
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "email",
                "promotional",
                "Promotional Email Campaign",
                base_date + timedelta(days=2),
                Decimal("0.30"),
                "click",
                "email_003",
                "inbox",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "email",
                "abandoned_cart",
                "Abandoned Cart Email",
                base_date + timedelta(days=4),
                Decimal("0.20"),
                "click",
                "email_004",
                "inbox",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "email",
                "discount_offer",
                "Discount Offer Email",
                base_date + timedelta(days=6),
                Decimal("0.25"),
                "click",
                "email_005",
                "inbox",
            ),
        ]
    )

    # Customer Journey 5: Affiliate marketing
    user_id = "user_005"
    data.extend(
        [
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "affiliate",
                "affiliate_blog",
                "Affiliate Blog Campaign",
                base_date + timedelta(days=1),
                Decimal("12.50"),
                "click",
                "affiliate_001",
                "blog_review",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "email",
                "follow_up",
                "Follow-up Email",
                base_date + timedelta(days=3),
                Decimal("0.18"),
                "click",
                "email_006",
                "inbox",
            ),
            (
                f"tp_{len(data)+1:03d}",
                user_id,
                "social",
                "facebook_organic",
                "Facebook Organic Post",
                base_date + timedelta(days=5),
                Decimal("0.00"),
                "click",
                None,
                "facebook_feed",
            ),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_conversion_data(spark):
    """Create sample conversion data corresponding to touchpoint journeys."""
    schema = StructType(
        [
            StructField("conversion_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("conversion_timestamp", TimestampType(), False),
            StructField("conversion_value", DecimalType(10, 2), False),
            StructField("conversion_type", StringType(), False),
            StructField("order_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )

    base_date = datetime(2024, 1, 1)
    data = [
        (
            "conv_001",
            "user_001",
            base_date + timedelta(days=12),
            Decimal("299.99"),
            "purchase",
            "order_001",
            "electronics",
            1,
        ),
        (
            "conv_002",
            "user_002",
            base_date + timedelta(days=8),
            Decimal("149.50"),
            "purchase",
            "order_002",
            "clothing",
            2,
        ),
        (
            "conv_003",
            "user_003",
            base_date + timedelta(days=10),
            Decimal("89.99"),
            "purchase",
            "order_003",
            "home_garden",
            1,
        ),
        (
            "conv_004",
            "user_004",
            base_date + timedelta(days=7),
            Decimal("199.00"),
            "purchase",
            "order_004",
            "sports",
            1,
        ),
        (
            "conv_005",
            "user_005",
            base_date + timedelta(days=6),
            Decimal("449.99"),
            "purchase",
            "order_005",
            "electronics",
            1,
        ),
        # Add some additional conversions for analysis
        (
            "conv_006",
            "user_001",
            base_date + timedelta(days=25),
            Decimal("159.99"),
            "purchase",
            "order_006",
            "clothing",
            1,
        ),
        (
            "conv_007",
            "user_002",
            base_date + timedelta(days=20),
            Decimal("75.50"),
            "purchase",
            "order_007",
            "home_garden",
            1,
        ),
    ]

    return spark.createDataFrame(data, schema)


def create_sample_campaign_data(spark):
    """Create comprehensive campaign data."""
    schema = StructType(
        [
            StructField("campaign_id", StringType(), False),
            StructField("campaign_name", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("start_date", TimestampType(), False),
            StructField("end_date", TimestampType(), True),
            StructField("budget", DecimalType(10, 2), False),
            StructField("daily_budget", DecimalType(10, 2), True),
            StructField("target_audience", StringType(), True),
            StructField("campaign_objective", StringType(), False),
            StructField("status", StringType(), False),
        ]
    )

    base_date = datetime(2024, 1, 1)
    data = [
        (
            "seo_campaign",
            "SEO Campaign",
            "organic",
            base_date,
            None,
            Decimal("0.00"),
            None,
            "broad",
            "awareness",
            "active",
        ),
        (
            "ppc_brand",
            "Brand PPC Campaign",
            "paid_search",
            base_date,
            base_date + timedelta(days=30),
            Decimal("3000.00"),
            Decimal("100.00"),
            "brand_searchers",
            "conversions",
            "active",
        ),
        (
            "ppc_generic",
            "Generic PPC Campaign",
            "paid_search",
            base_date,
            base_date + timedelta(days=30),
            Decimal("5000.00"),
            Decimal("166.67"),
            "broad",
            "conversions",
            "active",
        ),
        (
            "ppc_competitor",
            "Competitor PPC Campaign",
            "paid_search",
            base_date,
            base_date + timedelta(days=30),
            Decimal("2000.00"),
            Decimal("66.67"),
            "competitor_searchers",
            "conversions",
            "active",
        ),
        (
            "newsletter",
            "Weekly Newsletter",
            "email",
            base_date,
            None,
            Decimal("200.00"),
            Decimal("6.67"),
            "subscribers",
            "retention",
            "active",
        ),
        (
            "welcome_series",
            "Welcome Email Series",
            "email",
            base_date,
            None,
            Decimal("150.00"),
            Decimal("5.00"),
            "new_subscribers",
            "nurturing",
            "active",
        ),
        (
            "promotional",
            "Promotional Email Campaign",
            "email",
            base_date,
            base_date + timedelta(days=15),
            Decimal("300.00"),
            Decimal("20.00"),
            "customers",
            "conversions",
            "completed",
        ),
        (
            "abandoned_cart",
            "Abandoned Cart Email",
            "email",
            base_date,
            None,
            Decimal("100.00"),
            Decimal("3.33"),
            "cart_abandoners",
            "conversions",
            "active",
        ),
        (
            "discount_offer",
            "Discount Offer Email",
            "email",
            base_date,
            base_date + timedelta(days=10),
            Decimal("250.00"),
            Decimal("25.00"),
            "price_sensitive",
            "conversions",
            "completed",
        ),
        (
            "follow_up",
            "Follow-up Email",
            "email",
            base_date,
            None,
            Decimal("75.00"),
            Decimal("2.50"),
            "prospects",
            "nurturing",
            "active",
        ),
        (
            "facebook_retarget",
            "Facebook Retargeting",
            "social",
            base_date,
            base_date + timedelta(days=30),
            Decimal("1500.00"),
            Decimal("50.00"),
            "website_visitors",
            "conversions",
            "active",
        ),
        (
            "facebook_lookalike",
            "Facebook Lookalike",
            "social",
            base_date,
            base_date + timedelta(days=30),
            Decimal("2000.00"),
            Decimal("66.67"),
            "lookalike_audience",
            "awareness",
            "active",
        ),
        (
            "facebook_organic",
            "Facebook Organic Post",
            "social",
            base_date,
            None,
            Decimal("0.00"),
            None,
            "followers",
            "engagement",
            "active",
        ),
        (
            "instagram_video",
            "Instagram Video Campaign",
            "social",
            base_date,
            base_date + timedelta(days=20),
            Decimal("1200.00"),
            Decimal("60.00"),
            "young_adults",
            "awareness",
            "completed",
        ),
        (
            "banner_campaign",
            "Display Banner Campaign",
            "display",
            base_date,
            base_date + timedelta(days=30),
            Decimal("1800.00"),
            Decimal("60.00"),
            "retargeting",
            "conversions",
            "active",
        ),
        (
            "affiliate_blog",
            "Affiliate Blog Campaign",
            "affiliate",
            base_date,
            None,
            Decimal("1000.00"),
            None,
            "blog_readers",
            "conversions",
            "active",
        ),
        (
            "direct_traffic",
            "Direct Traffic",
            "direct",
            base_date,
            None,
            Decimal("0.00"),
            None,
            "existing_customers",
            "organic",
            "active",
        ),
    ]

    return spark.createDataFrame(data, schema)


def create_sample_marketing_spend_data(spark):
    """Create detailed marketing spend data."""
    schema = StructType(
        [
            StructField("spend_date", TimestampType(), False),
            StructField("campaign_id", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("spend_amount", DecimalType(10, 2), False),
            StructField("impressions", IntegerType(), True),
            StructField("clicks", IntegerType(), True),
            StructField("conversions", IntegerType(), True),
            StructField("spend_type", StringType(), False),
        ]
    )

    base_date = datetime(2024, 1, 1)
    data = []

    # Generate daily spend data for paid campaigns
    paid_campaigns = [
        ("ppc_brand", "paid_search", 100.00, 8000, 320, 8),
        ("ppc_generic", "paid_search", 166.67, 12000, 480, 6),
        ("ppc_competitor", "paid_search", 66.67, 5000, 200, 3),
        ("facebook_retarget", "social", 50.00, 15000, 600, 12),
        ("facebook_lookalike", "social", 66.67, 20000, 800, 8),
        ("instagram_video", "social", 60.00, 25000, 1000, 10),
        ("banner_campaign", "display", 60.00, 30000, 900, 7),
    ]

    # Generate 30 days of spend data
    for day in range(30):
        current_date = base_date + timedelta(days=day)
        for (
            campaign_id,
            channel,
            daily_spend,
            daily_impressions,
            daily_clicks,
            daily_conversions,
        ) in paid_campaigns:
            # Add some randomness to daily performance
            variance = 0.8 + (day % 5) * 0.1  # 0.8 to 1.2 multiplier
            data.append(
                (
                    current_date,
                    campaign_id,
                    channel,
                    Decimal(str(round(daily_spend * variance, 2))),
                    int(daily_impressions * variance),
                    int(daily_clicks * variance),
                    int(daily_conversions * variance),
                    "media",
                )
            )

    # Add email campaign costs (lower daily costs)
    email_campaigns = [
        ("newsletter", "email", 6.67, 5000, 250, 5),
        ("welcome_series", "email", 5.00, 1000, 150, 3),
        ("promotional", "email", 20.00, 3000, 180, 8),
        ("abandoned_cart", "email", 3.33, 500, 75, 4),
        ("discount_offer", "email", 25.00, 2000, 120, 6),
        ("follow_up", "email", 2.50, 800, 40, 2),
    ]

    for day in range(30):
        current_date = base_date + timedelta(days=day)
        for (
            campaign_id,
            channel,
            daily_spend,
            daily_impressions,
            daily_clicks,
            daily_conversions,
        ) in email_campaigns:
            variance = 0.9 + (day % 3) * 0.1  # 0.9 to 1.1 multiplier
            data.append(
                (
                    current_date,
                    campaign_id,
                    channel,
                    Decimal(str(round(daily_spend * variance, 2))),
                    int(daily_impressions * variance),
                    int(daily_clicks * variance),
                    int(daily_conversions * variance),
                    "email",
                )
            )

    # Add affiliate costs (commission-based)
    for day in range(30):
        current_date = base_date + timedelta(days=day)
        # Affiliate costs are typically commission-based, so lower daily amounts
        data.append(
            (
                current_date,
                "affiliate_blog",
                "affiliate",
                Decimal("33.33"),  # ~$1000/month
                0,
                50,
                2,  # No impressions tracked, fewer clicks, higher conversion rate
                "commission",
            )
        )

    return spark.createDataFrame(data, schema)


def create_sample_acquisition_data(spark):
    """Create customer acquisition data."""
    schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("acquisition_date", TimestampType(), False),
            StructField("acquisition_channel", StringType(), False),
            StructField("acquisition_campaign", StringType(), True),
            StructField("first_purchase_value", DecimalType(10, 2), True),
            StructField("customer_segment", StringType(), True),
            StructField("lifetime_value_30d", DecimalType(10, 2), True),
            StructField("lifetime_value_90d", DecimalType(10, 2), True),
        ]
    )

    base_date = datetime(2024, 1, 1)
    data = [
        (
            "user_001",
            base_date + timedelta(days=12),
            "paid_search",
            "ppc_generic",
            Decimal("299.99"),
            "high_value",
            Decimal("299.99"),
            Decimal("459.98"),
        ),
        (
            "user_002",
            base_date + timedelta(days=8),
            "social",
            "instagram_video",
            Decimal("149.50"),
            "medium_value",
            Decimal("149.50"),
            Decimal("225.00"),
        ),
        (
            "user_003",
            base_date + timedelta(days=10),
            "display",
            "banner_campaign",
            Decimal("89.99"),
            "medium_value",
            Decimal("89.99"),
            Decimal("89.99"),
        ),
        (
            "user_004",
            base_date + timedelta(days=7),
            "email",
            "discount_offer",
            Decimal("199.00"),
            "high_value",
            Decimal("199.00"),
            Decimal("199.00"),
        ),
        (
            "user_005",
            base_date + timedelta(days=6),
            "affiliate",
            "affiliate_blog",
            Decimal("449.99"),
            "high_value",
            Decimal("449.99"),
            Decimal("449.99"),
        ),
    ]

    return spark.createDataFrame(data, schema)


def create_sample_revenue_data(spark):
    """Create comprehensive revenue data."""
    schema = StructType(
        [
            StructField("date", TimestampType(), False),
            StructField("user_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("revenue", DecimalType(10, 2), False),
            StructField("channel", StringType(), True),
            StructField("campaign_id", StringType(), True),
            StructField("product_category", StringType(), True),
        ]
    )

    base_date = datetime(2024, 1, 1)
    data = [
        # Initial conversions
        (
            base_date + timedelta(days=12),
            "user_001",
            "order_001",
            Decimal("299.99"),
            "paid_search",
            "ppc_generic",
            "electronics",
        ),
        (
            base_date + timedelta(days=8),
            "user_002",
            "order_002",
            Decimal("149.50"),
            "social",
            "instagram_video",
            "clothing",
        ),
        (
            base_date + timedelta(days=10),
            "user_003",
            "order_003",
            Decimal("89.99"),
            "display",
            "banner_campaign",
            "home_garden",
        ),
        (
            base_date + timedelta(days=7),
            "user_004",
            "order_004",
            Decimal("199.00"),
            "email",
            "discount_offer",
            "sports",
        ),
        (
            base_date + timedelta(days=6),
            "user_005",
            "order_005",
            Decimal("449.99"),
            "affiliate",
            "affiliate_blog",
            "electronics",
        ),
        # Repeat purchases
        (
            base_date + timedelta(days=25),
            "user_001",
            "order_006",
            Decimal("159.99"),
            "email",
            "newsletter",
            "clothing",
        ),
        (
            base_date + timedelta(days=20),
            "user_002",
            "order_007",
            Decimal("75.50"),
            "direct",
            "direct_traffic",
            "home_garden",
        ),
        # Additional revenue from other customers (simulated)
        (
            base_date + timedelta(days=15),
            "user_006",
            "order_008",
            Decimal("125.00"),
            "organic",
            "seo_campaign",
            "books",
        ),
        (
            base_date + timedelta(days=18),
            "user_007",
            "order_009",
            Decimal("89.99"),
            "social",
            "facebook_organic",
            "clothing",
        ),
        (
            base_date + timedelta(days=22),
            "user_008",
            "order_010",
            Decimal("199.99"),
            "paid_search",
            "ppc_brand",
            "electronics",
        ),
    ]

    return spark.createDataFrame(data, schema)


def demonstrate_multi_touch_attribution(
    marketing_attribution, touchpoint_df, conversion_df
):
    """Demonstrate multi-touch attribution analysis across different models."""
    print("\n" + "=" * 60)
    print("MULTI-TOUCH ATTRIBUTION ANALYSIS")
    print("=" * 60)

    attribution_models = [
        "first_touch",
        "last_touch",
        "linear",
        "time_decay",
        "position_based",
    ]

    for model in attribution_models:
        print(f"\n--- {model.upper().replace('_', ' ')} ATTRIBUTION MODEL ---")

        try:
            result = marketing_attribution.analyze_multi_touch_attribution(
                touchpoint_df,
                conversion_df,
                attribution_model=model,
                attribution_window_days=30,
            )

            attribution_df = result["attribution_results"]
            model_summary = result["model_summary"]

            print(f"Total attributions: {attribution_df.count()}")
            print(f"Model: {model_summary['model']}")
            print(
                f"Attribution window: {model_summary['attribution_window_days']} days"
            )

            # Show attribution by channel
            channel_attribution = (
                attribution_df.groupBy("channel")
                .agg(
                    F.sum("attributed_revenue").alias("total_attributed_revenue"),
                    F.sum("attribution_credit").alias("total_attribution_credit"),
                    F.count("*").alias("touchpoint_count"),
                )
                .orderBy(F.desc("total_attributed_revenue"))
            )

            print("\nAttribution by Channel:")
            channel_attribution.show(truncate=False)

            # Show top conversion paths
            print("\nTop Customer Journeys:")
            attribution_paths = result["attribution_paths"]
            attribution_paths.select(
                "user_id", "conversion_value", "path_summary", "path_length"
            ).show(truncate=False)

        except Exception as e:
            print(f"Error with {model} model: {e}")


def demonstrate_campaign_performance(
    marketing_attribution, campaign_df, touchpoint_df, conversion_df
):
    """Demonstrate campaign performance tracking."""
    print("\n" + "=" * 60)
    print("CAMPAIGN PERFORMANCE TRACKING")
    print("=" * 60)

    try:
        result = marketing_attribution.track_campaign_performance(
            campaign_df, touchpoint_df, conversion_df, time_period="week"
        )

        campaign_metrics = result["campaign_metrics"]
        channel_performance = result["channel_performance"]
        time_series_performance = result["time_series_performance"]

        print(f"\nCampaign Performance Overview:")
        print(f"Total campaigns analyzed: {campaign_metrics.count()}")

        # Show top performing campaigns
        print("\nTop Performing Campaigns (by Revenue):")
        campaign_metrics.select(
            "campaign_name",
            "channel",
            "total_conversions",
            "total_revenue",
            "conversion_rate",
            "revenue_per_conversion",
        ).orderBy(F.desc("total_revenue")).show(10, truncate=False)

        # Show channel performance
        print("\nChannel Performance Summary:")
        channel_performance.select(
            "channel",
            "total_campaigns",
            "total_conversions",
            "total_revenue",
            "avg_conversion_rate",
            "avg_revenue_per_conversion",
        ).orderBy(F.desc("total_revenue")).show(truncate=False)

        # Show time series trends
        print("\nWeekly Performance Trends (last 5 weeks):")
        time_series_performance.select(
            "time_dimension", "total_conversions", "total_revenue"
        ).orderBy(F.desc("time_dimension")).limit(5).show(truncate=False)

    except Exception as e:
        print(f"Error in campaign performance tracking: {e}")


def demonstrate_customer_acquisition_cost(
    marketing_attribution, spend_df, acquisition_df, attribution_df
):
    """Demonstrate customer acquisition cost analysis."""
    print("\n" + "=" * 60)
    print("CUSTOMER ACQUISITION COST ANALYSIS")
    print("=" * 60)

    try:
        result = marketing_attribution.calculate_customer_acquisition_cost(
            spend_df, acquisition_df, attribution_df
        )

        cac_by_channel = result["cac_by_channel"]
        cac_by_campaign = result["cac_by_campaign"]
        cac_summary = result["cac_summary"]

        print(f"\nCAC Analysis Overview:")
        print(f"Channels analyzed: {cac_by_channel.count()}")
        print(f"Campaigns analyzed: {cac_by_campaign.count()}")

        # Show CAC by channel
        print("\nCustomer Acquisition Cost by Channel:")
        cac_by_channel.select(
            "channel",
            "total_spend",
            "total_customers",
            "customer_acquisition_cost",
            "ltv_cac_ratio",
            "payback_period_days",
        ).orderBy("customer_acquisition_cost").show(truncate=False)

        # Show CAC by campaign (top 10)
        print("\nTop 10 Campaigns by CAC Efficiency:")
        cac_by_campaign.select(
            "campaign_id", "channel", "customer_acquisition_cost", "ltv_cac_ratio"
        ).orderBy("customer_acquisition_cost").limit(10).show(truncate=False)

        # Show cohort analysis if available
        if "cohort_analysis" in result:
            print("\nCohort Analysis:")
            cohort_analysis = result["cohort_analysis"]
            cohort_analysis.select(
                "acquisition_cohort", "customers_acquired", "average_cac", "cohort_ltv"
            ).orderBy("acquisition_cohort").show(truncate=False)

        # Show overall summary
        print("\nOverall CAC Summary:")
        cac_summary.select("metric", "value", "benchmark_status").show(truncate=False)

    except Exception as e:
        print(f"Error in CAC analysis: {e}")


def demonstrate_marketing_roi(
    marketing_attribution, spend_df, revenue_df, attribution_df
):
    """Demonstrate marketing ROI analysis."""
    print("\n" + "=" * 60)
    print("MARKETING ROI ANALYSIS")
    print("=" * 60)

    try:
        result = marketing_attribution.calculate_marketing_roi(
            spend_df, revenue_df, attribution_df, time_period="month"
        )

        roi_by_channel = result["roi_by_channel"]
        roi_by_campaign = result["roi_by_campaign"]
        roi_time_series = result["roi_time_series"]

        print(f"\nROI Analysis Overview:")
        print(f"Channels analyzed: {roi_by_channel.count()}")
        print(f"Campaigns analyzed: {roi_by_campaign.count()}")

        # Show ROI by channel
        print("\nReturn on Investment by Channel:")
        roi_by_channel.select(
            "channel",
            "total_spend",
            "attributed_revenue",
            "return_on_ad_spend",
            "roi_percentage",
            "profit_margin",
        ).orderBy(F.desc("return_on_ad_spend")).show(truncate=False)

        # Show top performing campaigns by ROI
        print("\nTop 10 Campaigns by ROAS:")
        roi_by_campaign.select(
            "campaign_id", "channel", "return_on_ad_spend", "roi_percentage"
        ).orderBy(F.desc("return_on_ad_spend")).limit(10).show(truncate=False)

        # Show budget optimization recommendations
        if "budget_optimization" in result:
            print("\nBudget Optimization Recommendations:")
            budget_opt = result["budget_optimization"]
            budget_opt.select(
                "channel",
                "current_budget_percent",
                "recommended_budget_allocation",
                "efficiency_score",
                "optimization_potential",
            ).orderBy(F.desc("efficiency_score")).show(truncate=False)

        # Show incremental analysis
        if "incremental_analysis" in result:
            print("\nIncremental ROI Analysis:")
            incremental = result["incremental_analysis"]
            incremental.select(
                "channel", "incremental_revenue", "incremental_roi", "baseline_revenue"
            ).orderBy(F.desc("incremental_roi")).show(truncate=False)

    except Exception as e:
        print(f"Error in ROI analysis: {e}")


def demonstrate_advanced_analytics(marketing_attribution, touchpoint_df, conversion_df):
    """Demonstrate advanced marketing analytics features."""
    print("\n" + "=" * 60)
    print("ADVANCED MARKETING ANALYTICS")
    print("=" * 60)

    try:
        # Attribution path analysis
        print("\n--- ATTRIBUTION PATH ANALYSIS ---")
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            touchpoint_df, conversion_df, attribution_model="linear"
        )

        attribution_paths = attribution_result["attribution_paths"]

        # Show path complexity distribution
        print("\nCustomer Journey Complexity:")
        path_complexity = (
            attribution_paths.groupBy("path_length")
            .agg(
                F.count("*").alias("customer_count"),
                F.avg("conversion_value").alias("avg_conversion_value"),
            )
            .orderBy("path_length")
        )
        path_complexity.show()

        # Show channel combinations
        print("\nTop Channel Combinations in Customer Journeys:")
        attribution_paths.select("path_summary", "conversion_value").orderBy(
            F.desc("conversion_value")
        ).limit(10).show(truncate=False)

        # Channel transition analysis
        print("\n--- CHANNEL TRANSITION ANALYSIS ---")

        # Analyze how customers move between channels
        touchpoint_analysis = touchpoint_df.withColumn("date", F.to_date("timestamp"))

        # Window function to get previous channel for each user
        from pyspark.sql.window import Window

        user_window = Window.partitionBy("user_id").orderBy("timestamp")

        channel_transitions = touchpoint_analysis.withColumn(
            "prev_channel", F.lag("channel").over(user_window)
        ).filter(F.col("prev_channel").isNotNull())

        transition_summary = (
            channel_transitions.groupBy("prev_channel", "channel")
            .agg(F.count("*").alias("transition_count"))
            .orderBy(F.desc("transition_count"))
        )

        print("\nTop Channel Transitions:")
        transition_summary.limit(15).show(truncate=False)

        # Time between touchpoints analysis
        print("\n--- TOUCHPOINT TIMING ANALYSIS ---")

        touchpoint_timing = (
            touchpoint_analysis.withColumn(
                "prev_timestamp", F.lag("timestamp").over(user_window)
            )
            .withColumn(
                "time_between_touchpoints",
                (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"))
                / 86400,  # Convert to days
            )
            .filter(F.col("prev_timestamp").isNotNull())
        )

        timing_stats = touchpoint_timing.agg(
            F.avg("time_between_touchpoints").alias("avg_days_between_touchpoints"),
            F.expr("percentile_approx(time_between_touchpoints, 0.5)").alias(
                "median_days_between_touchpoints"
            ),
            F.min("time_between_touchpoints").alias("min_days_between_touchpoints"),
            F.max("time_between_touchpoints").alias("max_days_between_touchpoints"),
        )

        print("\nTouchpoint Timing Statistics:")
        timing_stats.show()

    except Exception as e:
        print(f"Error in advanced analytics: {e}")


def main():
    """Main function to demonstrate marketing attribution engine capabilities."""
    print("=" * 80)
    print("MARKETING ATTRIBUTION ENGINE - COMPREHENSIVE USAGE EXAMPLE")
    print("=" * 80)
    print(
        "This example demonstrates the full capabilities of the MarketingAttribution class"
    )
    print(
        "including multi-touch attribution, campaign performance, CAC analysis, and ROI calculation."
    )

    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created successfully")

    try:
        # Create sample data
        print("\nCreating comprehensive sample data...")
        touchpoint_df = create_sample_touchpoint_data(spark)
        conversion_df = create_sample_conversion_data(spark)
        campaign_df = create_sample_campaign_data(spark)
        spend_df = create_sample_marketing_spend_data(spark)
        acquisition_df = create_sample_acquisition_data(spark)
        revenue_df = create_sample_revenue_data(spark)

        print(f"✓ Created {touchpoint_df.count()} touchpoints")
        print(f"✓ Created {conversion_df.count()} conversions")
        print(f"✓ Created {campaign_df.count()} campaigns")
        print(f"✓ Created {spend_df.count()} spend records")
        print(f"✓ Created {acquisition_df.count()} acquisition records")
        print(f"✓ Created {revenue_df.count()} revenue records")

        # Initialize marketing attribution engine
        print("\nInitializing Marketing Attribution Engine...")
        marketing_attribution = MarketingAttribution(spark)
        logger.info("MarketingAttribution engine initialized")

        # Demonstrate multi-touch attribution
        demonstrate_multi_touch_attribution(
            marketing_attribution, touchpoint_df, conversion_df
        )

        # Demonstrate campaign performance tracking
        demonstrate_campaign_performance(
            marketing_attribution, campaign_df, touchpoint_df, conversion_df
        )

        # Get attribution data for CAC and ROI analysis
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            touchpoint_df, conversion_df, attribution_model="linear"
        )
        attribution_df = attribution_result["attribution_results"]

        # Demonstrate customer acquisition cost analysis
        demonstrate_customer_acquisition_cost(
            marketing_attribution, spend_df, acquisition_df, attribution_df
        )

        # Demonstrate marketing ROI analysis
        demonstrate_marketing_roi(
            marketing_attribution, spend_df, revenue_df, attribution_df
        )

        # Demonstrate advanced analytics
        demonstrate_advanced_analytics(
            marketing_attribution, touchpoint_df, conversion_df
        )

        print("\n" + "=" * 80)
        print("MARKETING ATTRIBUTION ANALYSIS COMPLETE")
        print("=" * 80)
        print(
            "All marketing attribution capabilities have been successfully demonstrated!"
        )
        print("The engine provides comprehensive insights into:")
        print("• Multi-touch attribution across 5 different models")
        print("• Campaign performance tracking with detailed metrics")
        print("• Customer acquisition cost analysis with LTV ratios")
        print("• Marketing ROI calculation with budget optimization")
        print("• Advanced analytics including channel transitions and timing")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    finally:
        # Clean up Spark session
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

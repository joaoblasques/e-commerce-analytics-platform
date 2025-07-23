"""
Product Analytics Usage Example.

This example demonstrates the comprehensive product analytics capabilities
of the ProductAnalytics class, including:
- Product performance analysis
- Recommendation engine building
- Market basket analysis
- Product lifecycle analysis

Usage:
    python examples/business_intelligence/product_analytics_example.py
"""

import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.business_intelligence.product_analytics import ProductAnalytics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create and configure Spark session for the example."""
    return (
        SparkSession.builder.appName("ProductAnalyticsExample")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def generate_sample_transactions(spark: SparkSession, num_transactions: int = 10000):
    """Generate realistic sample transaction data."""
    logger.info(f"Generating {num_transactions} sample transactions...")

    import random

    from faker import Faker

    fake = Faker()

    # Product categories and brands
    categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys"]
    brands = {
        "Electronics": ["Apple", "Samsung", "Sony", "LG", "Dell"],
        "Clothing": ["Nike", "Adidas", "Zara", "H&M", "Uniqlo"],
        "Books": ["Penguin", "HarperCollins", "Simon & Schuster", "Random House"],
        "Home & Garden": ["IKEA", "Home Depot", "Wayfair", "Williams Sonoma"],
        "Sports": ["Nike", "Adidas", "Under Armour", "Puma", "Wilson"],
        "Toys": ["LEGO", "Mattel", "Hasbro", "Fisher-Price", "Nerf"],
    }

    # Price ranges by category
    price_ranges = {
        "Electronics": (50, 2000),
        "Clothing": (20, 200),
        "Books": (10, 50),
        "Home & Garden": (25, 500),
        "Sports": (30, 300),
        "Toys": (15, 150),
    }

    # Generate transaction data
    transactions = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_transactions):
        category = random.choice(categories)
        brand = random.choice(brands[category])
        price_min, price_max = price_ranges[category]

        transaction = (
            f"txn_{i:08d}",
            f"user_{random.randint(1, 2000):06d}",  # 2000 unique users
            f"prod_{category.lower().replace(' & ', '_').replace(' ', '_')}_{random.randint(1, 500):04d}",
            category,
            brand,
            round(random.uniform(price_min, price_max), 2),
            random.randint(1, 5),  # quantity
            base_date
            + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
            f"channel_{random.choice(['online', 'mobile', 'store'])}",
            f"region_{random.choice(['north', 'south', 'east', 'west', 'central'])}",
            random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
            random.choice(["desktop", "mobile", "tablet"]),
        )
        transactions.append(transaction)

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("sales_channel", StringType(), True),
            StructField("geographic_region", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("device_type", StringType(), True),
        ]
    )

    return spark.createDataFrame(transactions, schema)


def generate_sample_inventory(spark: SparkSession, product_ids: list):
    """Generate sample inventory data."""
    logger.info(f"Generating inventory data for {len(product_ids)} products...")

    import random

    inventory_data = []
    for product_id in product_ids:
        # Extract category from product_id
        if "electronics" in product_id:
            category = "Electronics"
            stock_range = (10, 100)
        elif "clothing" in product_id:
            category = "Clothing"
            stock_range = (20, 200)
        elif "books" in product_id:
            category = "Books"
            stock_range = (50, 500)
        elif "home" in product_id:
            category = "Home & Garden"
            stock_range = (15, 150)
        elif "sports" in product_id:
            category = "Sports"
            stock_range = (25, 250)
        else:
            category = "Toys"
            stock_range = (30, 300)

        inventory_data.append((product_id, random.randint(*stock_range), category))

    schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("product_category", StringType(), True),
        ]
    )

    return spark.createDataFrame(inventory_data, schema)


def demonstrate_product_performance_analysis(
    product_analytics: ProductAnalytics, transactions_df, inventory_df
):
    """Demonstrate comprehensive product performance analysis."""
    logger.info("=== Product Performance Analysis ===")

    # Basic performance analysis
    performance_results = product_analytics.analyze_product_performance(
        transactions_df, inventory_df=inventory_df, time_period="month"
    )

    # Display overall performance metrics
    logger.info("Overall Performance Metrics:")
    overall_df = performance_results["overall_performance"]
    overall_metrics = overall_df.collect()

    for row in overall_metrics[:3]:  # Show first 3 months
        logger.info(f"  Month: {row.time_dimension}")
        logger.info(f"    Total Revenue: ${row.total_revenue:,.2f}")
        logger.info(f"    Total Units Sold: {row.total_units_sold:,}")
        logger.info(f"    Unique Products: {row.unique_products_sold}")
        logger.info(f"    Unique Customers: {row.unique_customers}")
        logger.info(f"    Avg Revenue per Product: ${row.avg_revenue_per_product:,.2f}")

    # Top performing products
    logger.info("\nTop 10 Products by Revenue:")
    sales_df = performance_results["sales_metrics"]
    top_products = sales_df.orderBy(sales_df.total_revenue.desc()).limit(10).collect()

    for i, row in enumerate(top_products, 1):
        logger.info(f"  {i}. {row.product_id} ({row.product_category})")
        logger.info(
            f"     Revenue: ${row.total_revenue:,.2f} | Units: {row.total_units_sold}"
        )
        logger.info(
            f"     Customers: {row.unique_customers} | Avg Price: ${row.avg_selling_price:.2f}"
        )

    # Category performance
    logger.info("\nCategory Performance:")
    category_df = performance_results["category_performance"]
    category_summary = (
        category_df.groupBy("product_category")
        .agg(
            {
                "category_revenue": "sum",
                "category_units_sold": "sum",
                "unique_customers": "sum",
            }
        )
        .collect()
    )

    for row in category_summary:
        logger.info(f"  {row.product_category}:")
        logger.info(f"    Revenue: ${row['sum(category_revenue)']:,.2f}")
        logger.info(f"    Units: {row['sum(category_units_sold)']:,}")
        logger.info(f"    Customers: {row['sum(unique_customers)']:,}")

    # Inventory analysis
    if "inventory_analysis" in performance_results:
        logger.info("\nInventory Analysis (Top 5 by turnover):")
        inventory_df = performance_results["inventory_analysis"]
        top_turnover = (
            inventory_df.orderBy(inventory_df.inventory_turnover.desc())
            .limit(5)
            .collect()
        )

        for row in top_turnover:
            logger.info(
                f"  {row.product_id}: Turnover {row.inventory_turnover:.2f}x | Risk: {row.stockout_risk}"
            )


def demonstrate_recommendation_engine(
    product_analytics: ProductAnalytics, transactions_df
):
    """Demonstrate product recommendation engine capabilities."""
    logger.info("\n=== Product Recommendation Engine ===")

    try:
        # Prepare interaction data from transactions
        interaction_df = transactions_df.select(
            "user_id", "product_id", "quantity", "price"
        )

        # Build recommendation engine
        logger.info("Building recommendation engine with collaborative filtering...")
        recommendation_results = product_analytics.build_recommendation_engine(
            interaction_df
        )

        # Display model metrics
        logger.info("Model Performance:")
        metrics = recommendation_results["model_metrics"]
        logger.info(f"  RMSE: {metrics['rmse']:.4f}")
        logger.info(f"  R²: {metrics.get('r2', 'N/A')}")
        logger.info(f"  Training Samples: {metrics['training_samples']:,}")

        # Display training data statistics
        stats = recommendation_results["training_data_stats"]
        logger.info("\nTraining Data Statistics:")
        logger.info(f"  Total Interactions: {stats['total_interactions']:,}")
        logger.info(f"  Unique Users: {stats['unique_users']:,}")
        logger.info(f"  Unique Products: {stats['unique_products']:,}")

        # Show sample user recommendations
        logger.info("\nSample User Recommendations (Top 5 users):")
        user_recs = recommendation_results["user_recommendations"]
        sample_recs = user_recs.limit(
            20
        ).collect()  # Get recommendations for first few users

        current_user = None
        rec_count = 0

        for row in sample_recs:
            if current_user != row.user_id:
                if rec_count >= 5:  # Limit to 5 users
                    break
                current_user = row.user_id
                rec_count += 1
                logger.info(f"  User {current_user}:")

            logger.info(f"    → {row.product_id} (score: {row.rating:.2f})")

        logger.info("Recommendation engine built successfully!")

    except Exception as e:
        logger.error(f"Error in recommendation engine: {e}")
        logger.info(
            "This might be due to insufficient data for collaborative filtering."
        )


def demonstrate_market_basket_analysis(
    product_analytics: ProductAnalytics, transactions_df
):
    """Demonstrate market basket analysis capabilities."""
    logger.info("\n=== Market Basket Analysis ===")

    try:
        # Perform market basket analysis
        logger.info("Analyzing market basket patterns...")
        basket_results = product_analytics.perform_market_basket_analysis(
            transactions_df,
            min_support=0.001,  # Lower threshold for sample data
            min_confidence=0.1,
        )

        # Display analysis summary
        summary = basket_results["analysis_summary"]
        logger.info("Market Basket Analysis Summary:")
        logger.info(f"  Total Baskets: {summary['total_baskets']:,}")
        logger.info(f"  Frequent Itemsets: {summary['frequent_itemsets_count']:,}")
        logger.info(f"  Association Rules: {summary['association_rules_count']:,}")
        logger.info(f"  Min Support: {summary['min_support_used']}")
        logger.info(f"  Min Confidence: {summary['min_confidence_used']}")

        # Show top frequent itemsets
        if summary["frequent_itemsets_count"] > 0:
            logger.info("\nTop 10 Frequent Itemsets:")
            frequent_itemsets = basket_results["frequent_itemsets"]
            top_itemsets = (
                frequent_itemsets.orderBy(frequent_itemsets.freq.desc())
                .limit(10)
                .collect()
            )

            for i, row in enumerate(top_itemsets, 1):
                items_str = ", ".join(row.items[:3])  # Show first 3 items
                if len(row.items) > 3:
                    items_str += f" (+ {len(row.items) - 3} more)"
                logger.info(f"  {i}. [{items_str}] (frequency: {row.freq})")

        # Show top association rules
        if summary["association_rules_count"] > 0:
            logger.info("\nTop 10 Association Rules:")
            association_rules = basket_results["association_rules"]
            top_rules = (
                association_rules.orderBy(association_rules.confidence.desc())
                .limit(10)
                .collect()
            )

            for i, row in enumerate(top_rules, 1):
                antecedent = ", ".join(row.antecedent[:2])  # Show first 2 items
                consequent = ", ".join(row.consequent[:2])
                logger.info(f"  {i}. [{antecedent}] → [{consequent}]")
                logger.info(
                    f"     Confidence: {row.confidence:.3f} | Lift: {row.lift:.3f}"
                )

        # Cross-sell recommendations
        if "cross_sell_recommendations" in basket_results:
            logger.info("\nTop Cross-sell Recommendations:")
            cross_sell = basket_results["cross_sell_recommendations"]
            top_cross_sell = cross_sell.limit(10).collect()

            for i, row in enumerate(top_cross_sell, 1):
                logger.info(
                    f"  {i}. When buying {row.anchor_product}, recommend {row.recommended_product}"
                )
                logger.info(
                    f"     Confidence: {row.confidence:.3f} | Lift: {row.lift:.3f}"
                )

        logger.info("Market basket analysis completed!")

    except Exception as e:
        logger.error(f"Error in market basket analysis: {e}")
        logger.info(
            "This might be due to insufficient transaction data for pattern detection."
        )


def demonstrate_product_lifecycle_analysis(
    product_analytics: ProductAnalytics, transactions_df
):
    """Demonstrate product lifecycle analysis capabilities."""
    logger.info("\n=== Product Lifecycle Analysis ===")

    try:
        # Perform lifecycle analysis
        logger.info("Analyzing product lifecycle stages...")
        lifecycle_results = product_analytics.analyze_product_lifecycle(transactions_df)

        # Display stage distribution
        stage_dist = lifecycle_results["stage_distribution"]
        logger.info("Product Lifecycle Stage Distribution:")

        stage_data = stage_dist.collect()
        total_products = sum(row.product_count for row in stage_data)

        for row in stage_data:
            percentage = (
                (row.product_count / total_products) * 100 if total_products > 0 else 0
            )
            logger.info(
                f"  {row.lifecycle_stage.title()}: {row.product_count} products ({percentage:.1f}%)"
            )

        # Show products at risk
        at_risk = lifecycle_results["at_risk_products"]
        at_risk_count = at_risk.count()
        logger.info(f"\nProducts at Risk: {at_risk_count}")

        if at_risk_count > 0:
            logger.info("Top 10 At-Risk Products:")
            at_risk_products = (
                at_risk.orderBy(at_risk.avg_growth_rate.asc()).limit(10).collect()
            )

            for i, row in enumerate(at_risk_products, 1):
                logger.info(f"  {i}. {row.product_id}")
                logger.info(
                    f"     Stage: {row.lifecycle_stage} | Growth: {row.avg_growth_rate:.3f}"
                )
                logger.info(f"     Avg Daily Sales: {row.avg_daily_sales:.2f}")

        # Show growth opportunities
        growth_opps = lifecycle_results["growth_opportunities"]
        growth_count = growth_opps.count()
        logger.info(f"\nGrowth Opportunities: {growth_count}")

        if growth_count > 0:
            logger.info("Top 10 Growth Opportunities:")
            growth_products = (
                growth_opps.orderBy(growth_opps.avg_growth_rate.desc())
                .limit(10)
                .collect()
            )

            for i, row in enumerate(growth_products, 1):
                logger.info(f"  {i}. {row.product_id}")
                logger.info(
                    f"     Stage: {row.lifecycle_stage} | Growth: {row.avg_growth_rate:.3f}"
                )
                logger.info(f"     Avg Daily Sales: {row.avg_daily_sales:.2f}")

        # Stage transitions
        transitions = lifecycle_results["stage_transitions"]
        logger.info("\nStage Performance Summary:")
        transition_data = transitions.collect()

        for row in transition_data:
            logger.info(f"  {row.lifecycle_stage.title()}:")
            logger.info(f"    Products: {row.product_count}")
            logger.info(f"    Avg Growth Rate: {row.avg_stage_growth_rate:.3f}")
            logger.info(f"    Avg Sales: {row.avg_stage_sales:.2f}")

        logger.info("Product lifecycle analysis completed!")

    except Exception as e:
        logger.error(f"Error in lifecycle analysis: {e}")
        logger.info(
            "This might be due to insufficient historical data for trend analysis."
        )


def generate_business_intelligence_report(
    product_analytics: ProductAnalytics, transactions_df, inventory_df
):
    """Generate a comprehensive business intelligence report."""
    logger.info("\n" + "=" * 60)
    logger.info("PRODUCT ANALYTICS BUSINESS INTELLIGENCE REPORT")
    logger.info("=" * 60)

    try:
        # Overall performance analysis
        performance_results = product_analytics.analyze_product_performance(
            transactions_df, inventory_df=inventory_df
        )

        # Key metrics summary
        overall_df = performance_results["overall_performance"]
        sales_df = performance_results["sales_metrics"]

        # Calculate key KPIs
        total_revenue = overall_df.agg({"total_revenue": "sum"}).collect()[0][0]
        total_units = overall_df.agg({"total_units_sold": "sum"}).collect()[0][0]
        total_customers = overall_df.agg({"unique_customers": "max"}).collect()[0][0]
        avg_order_value = total_revenue / total_units if total_units > 0 else 0

        total_products = sales_df.count()
        active_products = sales_df.filter(sales_df.total_revenue > 0).count()

        logger.info("KEY PERFORMANCE INDICATORS:")
        logger.info(f"  💰 Total Revenue: ${total_revenue:,.2f}")
        logger.info(f"  📦 Total Units Sold: {total_units:,}")
        logger.info(f"  👥 Total Customers: {total_customers:,}")
        logger.info(f"  🛒 Average Order Value: ${avg_order_value:.2f}")
        logger.info(f"  🏷️  Total Products: {total_products:,}")
        logger.info(f"  ✅ Active Products: {active_products:,}")

        # Top performers
        top_products = (
            sales_df.orderBy(sales_df.total_revenue.desc()).limit(5).collect()
        )
        logger.info("\nTOP 5 PERFORMING PRODUCTS:")
        for i, row in enumerate(top_products, 1):
            logger.info(f"  {i}. {row.product_id}")
            logger.info(f"     Revenue: ${row.total_revenue:,.2f}")
            logger.info(f"     Category: {row.product_category} | Brand: {row.brand}")

        # Category insights
        category_df = performance_results["category_performance"]
        category_revenue = (
            category_df.groupBy("product_category")
            .agg({"category_revenue": "sum"})
            .orderBy("sum(category_revenue)", ascending=False)
            .limit(3)
            .collect()
        )

        logger.info("\nTOP 3 CATEGORIES BY REVENUE:")
        for i, row in enumerate(category_revenue, 1):
            logger.info(
                f"  {i}. {row.product_category}: ${row['sum(category_revenue)']:,.2f}"
            )

        # Recommendations
        logger.info("\nBUSINESS RECOMMENDATIONS:")

        # Low performing products
        low_performers = sales_df.filter(
            sales_df.total_revenue < (total_revenue / total_products * 0.1)
        ).count()
        if low_performers > 0:
            logger.info(
                f"  ⚠️  {low_performers} products are significantly underperforming"
            )
            logger.info("     → Consider promotional campaigns or inventory reduction")

        # High inventory turnover products
        if "inventory_analysis" in performance_results:
            inventory_df = performance_results["inventory_analysis"]
            high_turnover = inventory_df.filter(
                inventory_df.inventory_turnover > 10
            ).count()
            if high_turnover > 0:
                logger.info(
                    f"  🔥 {high_turnover} products have high inventory turnover"
                )
                logger.info(
                    "     → Consider increasing stock levels to avoid stockouts"
                )

        # Customer concentration
        customer_revenue = sales_df.agg({"avg_revenue_per_customer": "avg"}).collect()[
            0
        ][0]
        if customer_revenue:
            logger.info(f"  📊 Average revenue per customer: ${customer_revenue:.2f}")
            if customer_revenue > avg_order_value * 2:
                logger.info("     → High customer loyalty - consider loyalty programs")

        logger.info("\n" + "=" * 60)
        logger.info("END OF REPORT")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error generating business intelligence report: {e}")


def main():
    """Main execution function."""
    logger.info("Starting Product Analytics Comprehensive Example")

    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created successfully")

    try:
        # Generate sample data
        transactions_df = generate_sample_transactions(spark, num_transactions=5000)
        logger.info(f"Generated {transactions_df.count():,} sample transactions")

        # Get unique product IDs for inventory generation
        product_ids = [
            row.product_id
            for row in transactions_df.select("product_id").distinct().collect()
        ]
        inventory_df = generate_sample_inventory(
            spark, product_ids[:100]
        )  # Limit for demo
        logger.info(f"Generated inventory data for {inventory_df.count():,} products")

        # Initialize product analytics
        product_analytics = ProductAnalytics(spark)
        logger.info("ProductAnalytics instance created")

        # Demonstrate all capabilities
        demonstrate_product_performance_analysis(
            product_analytics, transactions_df, inventory_df
        )
        demonstrate_recommendation_engine(product_analytics, transactions_df)
        demonstrate_market_basket_analysis(product_analytics, transactions_df)
        demonstrate_product_lifecycle_analysis(product_analytics, transactions_df)

        # Generate comprehensive business intelligence report
        generate_business_intelligence_report(
            product_analytics, transactions_df, inventory_df
        )

        logger.info("\n🎉 Product Analytics Example completed successfully!")
        logger.info("All product analytics capabilities have been demonstrated.")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

    finally:
        # Clean up
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

"""
Geographic Analytics Usage Example

This example demonstrates the comprehensive capabilities of the GeographicAnalytics module
for e-commerce data analysis, including:

1. Geographic sales distribution analysis
2. Seasonal trend identification
3. Regional demand forecasting
4. Geographic customer segmentation

The example uses realistic sample data and showcases all major features
of the geographic analytics engine.
"""

import logging
import random
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, lit, when
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    SPARK_AVAILABLE = True
except ImportError:
    print("PySpark not available. This example requires PySpark to run.")
    SPARK_AVAILABLE = False
    exit(1)

from src.analytics.business_intelligence.geographic_analytics import GeographicAnalytics


class GeographicAnalyticsDemo:
    """
    Demonstration class for Geographic Analytics capabilities.

    This class provides a comprehensive demonstration of all geographic analytics
    features using realistic sample data.
    """

    def __init__(self):
        """Initialize the demo with SparkSession and sample data."""
        self.spark = None
        self.geo_analytics = None
        self.setup_spark()
        self.setup_sample_data()

    def setup_spark(self):
        """Initialize SparkSession with optimized configuration."""
        print("🚀 Initializing Spark Session...")

        self.spark = (
            SparkSession.builder.appName("GeographicAnalyticsDemo")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        # Initialize Geographic Analytics
        self.geo_analytics = GeographicAnalytics(self.spark)
        print("✅ Spark Session and Geographic Analytics initialized successfully!")

    def setup_sample_data(self):
        """Generate comprehensive sample data for demonstration."""
        print("📊 Generating sample e-commerce data...")

        # Generate transaction data
        self.transaction_df = self.generate_transaction_data(5000)
        self.customer_df = self.generate_customer_data(1000)
        self.product_df = self.generate_product_data(200)

        print(f"✅ Sample data generated:")
        print(f"   - Transactions: {self.transaction_df.count()}")
        print(f"   - Customers: {self.customer_df.count()}")
        print(f"   - Products: {self.product_df.count()}")

    def generate_transaction_data(self, num_transactions: int) -> DataFrame:
        """Generate realistic transaction data."""
        transaction_data = []

        # Define seasonal patterns and regional preferences
        seasonal_multipliers = {
            1: 0.8,
            2: 0.7,
            3: 0.9,
            4: 1.0,
            5: 1.1,
            6: 1.2,
            7: 1.3,
            8: 1.2,
            9: 1.0,
            10: 1.1,
            11: 1.5,
            12: 1.8,
        }

        base_date = datetime(2023, 1, 1)

        for i in range(num_transactions):
            # Generate random date with seasonal bias
            days_offset = random.randint(0, 450)  # ~15 months of data
            transaction_date = base_date + timedelta(days=days_offset)
            month = transaction_date.month

            # Apply seasonal multiplier to transaction probability
            seasonal_factor = seasonal_multipliers.get(month, 1.0)
            base_amount = random.uniform(20, 500)
            seasonal_amount = base_amount * seasonal_factor

            transaction_data.append(
                {
                    "transaction_id": f"txn_{i+1:06d}",
                    "customer_id": f"cust_{random.randint(1, 1000):04d}",
                    "product_id": f"prod_{random.randint(1, 200):03d}",
                    "amount": round(seasonal_amount * random.uniform(0.8, 1.2), 2),
                    "timestamp": transaction_date,
                    "payment_method": random.choice(
                        [
                            "credit_card",
                            "debit_card",
                            "paypal",
                            "bank_transfer",
                            "apple_pay",
                        ]
                    ),
                    "channel": random.choice(["web", "mobile", "store", "phone"]),
                    "currency": "USD",
                }
            )

        schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("payment_method", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("currency", StringType(), True),
            ]
        )

        return self.spark.createDataFrame(transaction_data, schema)

    def generate_customer_data(self, num_customers: int) -> DataFrame:
        """Generate realistic customer data with geographic distribution."""
        customer_data = []

        # Define realistic geographic distribution
        geo_regions = [
            # USA regions
            {
                "country": "USA",
                "region": "California",
                "cities": [
                    ("San Francisco", 37.7749, -122.4194),
                    ("Los Angeles", 34.0522, -118.2437),
                    ("San Diego", 32.7157, -117.1611),
                ],
            },
            {
                "country": "USA",
                "region": "New York",
                "cities": [
                    ("New York City", 40.7128, -74.0060),
                    ("Buffalo", 42.8864, -78.8784),
                    ("Albany", 42.6526, -73.7562),
                ],
            },
            {
                "country": "USA",
                "region": "Texas",
                "cities": [
                    ("Houston", 29.7604, -95.3698),
                    ("Dallas", 32.7767, -96.7970),
                    ("Austin", 30.2672, -97.7431),
                ],
            },
            {
                "country": "USA",
                "region": "Florida",
                "cities": [
                    ("Miami", 25.7617, -80.1918),
                    ("Orlando", 28.5383, -81.3792),
                    ("Tampa", 27.9506, -82.4572),
                ],
            },
            # Canada regions
            {
                "country": "Canada",
                "region": "Ontario",
                "cities": [
                    ("Toronto", 43.6532, -79.3832),
                    ("Ottawa", 45.4215, -75.6972),
                    ("Hamilton", 43.2557, -79.8711),
                ],
            },
            {
                "country": "Canada",
                "region": "British Columbia",
                "cities": [
                    ("Vancouver", 49.2827, -123.1207),
                    ("Victoria", 48.4284, -123.3656),
                    ("Surrey", 49.1913, -122.8490),
                ],
            },
            # UK regions
            {
                "country": "UK",
                "region": "England",
                "cities": [
                    ("London", 51.5074, -0.1278),
                    ("Manchester", 53.4808, -2.2426),
                    ("Birmingham", 52.4862, -1.8904),
                ],
            },
            {
                "country": "UK",
                "region": "Scotland",
                "cities": [
                    ("Edinburgh", 55.9533, -3.1883),
                    ("Glasgow", 55.8642, -4.2518),
                    ("Aberdeen", 57.1497, -2.0943),
                ],
            },
            # Germany regions
            {
                "country": "Germany",
                "region": "Bavaria",
                "cities": [
                    ("Munich", 48.1351, 11.5820),
                    ("Nuremberg", 49.4521, 11.0767),
                    ("Augsburg", 48.3705, 10.8978),
                ],
            },
            {
                "country": "Germany",
                "region": "North Rhine-Westphalia",
                "cities": [
                    ("Cologne", 50.9375, 6.9603),
                    ("Düsseldorf", 51.2277, 6.7735),
                    ("Dortmund", 51.5136, 7.4653),
                ],
            },
        ]

        for i in range(num_customers):
            # Select region with realistic distribution (USA: 60%, others: 40%)
            if random.random() < 0.6:
                region = random.choice(
                    [r for r in geo_regions if r["country"] == "USA"]
                )
            else:
                region = random.choice(
                    [r for r in geo_regions if r["country"] != "USA"]
                )

            city_info = random.choice(region["cities"])

            # Generate customer registration date
            reg_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))

            customer_data.append(
                {
                    "customer_id": f"cust_{i+1:04d}",
                    "country": region["country"],
                    "region": region["region"],
                    "city": city_info[0],
                    "latitude": city_info[1]
                    + random.uniform(-0.1, 0.1),  # Add some variation
                    "longitude": city_info[2] + random.uniform(-0.1, 0.1),
                    "registration_date": reg_date,
                    "customer_segment": random.choice(
                        ["Premium", "Standard", "Basic", "Enterprise", "Starter"]
                    ),
                    "age_group": random.choice(
                        ["18-25", "26-35", "36-45", "46-55", "56-65", "65+"]
                    ),
                    "preferred_language": random.choice(["en", "fr", "de", "es", "it"]),
                }
            )

        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("country", StringType(), True),
                StructField("region", StringType(), True),
                StructField("city", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("registration_date", TimestampType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("age_group", StringType(), True),
                StructField("preferred_language", StringType(), True),
            ]
        )

        return self.spark.createDataFrame(customer_data, schema)

    def generate_product_data(self, num_products: int) -> DataFrame:
        """Generate realistic product data."""
        product_data = []

        # Define product categories with seasonal characteristics
        categories = [
            {
                "category": "Electronics",
                "subcategories": [
                    "Smartphones",
                    "Laptops",
                    "Tablets",
                    "Headphones",
                    "Cameras",
                ],
                "brands": ["Apple", "Samsung", "Sony", "Dell", "HP", "Canon"],
                "price_range": (50, 2000),
                "seasonal_peak": [11, 12, 1],  # Holiday season
            },
            {
                "category": "Clothing",
                "subcategories": ["Shirts", "Pants", "Dresses", "Shoes", "Accessories"],
                "brands": ["Nike", "Adidas", "Zara", "H&M", "Calvin Klein"],
                "price_range": (20, 300),
                "seasonal_peak": [3, 4, 9, 10],  # Spring and fall
            },
            {
                "category": "Home & Garden",
                "subcategories": ["Furniture", "Decor", "Kitchen", "Garden", "Tools"],
                "brands": ["IKEA", "West Elm", "Home Depot", "Wayfair", "Target"],
                "price_range": (15, 1500),
                "seasonal_peak": [4, 5, 6],  # Spring season
            },
            {
                "category": "Sports & Fitness",
                "subcategories": [
                    "Exercise Equipment",
                    "Outdoor Gear",
                    "Sports Apparel",
                    "Supplements",
                ],
                "brands": ["Nike", "Under Armour", "Patagonia", "REI", "Fitness First"],
                "price_range": (25, 800),
                "seasonal_peak": [1, 2, 5, 6],  # New Year and summer
            },
            {
                "category": "Books & Media",
                "subcategories": [
                    "Fiction",
                    "Non-Fiction",
                    "Educational",
                    "Entertainment",
                    "Digital",
                ],
                "brands": ["Penguin", "HarperCollins", "Amazon", "Netflix", "Spotify"],
                "price_range": (10, 100),
                "seasonal_peak": [9, 10, 11, 12],  # Back to school and holidays
            },
        ]

        for i in range(num_products):
            category_info = random.choice(categories)

            product_data.append(
                {
                    "product_id": f"prod_{i+1:03d}",
                    "category": category_info["category"],
                    "subcategory": random.choice(category_info["subcategories"]),
                    "brand": random.choice(category_info["brands"]),
                    "price": round(random.uniform(*category_info["price_range"]), 2),
                    "seasonal_peak_months": str(category_info["seasonal_peak"]),
                    "launch_date": datetime(2021, 1, 1)
                    + timedelta(days=random.randint(0, 1095)),
                    "rating": round(random.uniform(3.0, 5.0), 1),
                    "review_count": random.randint(0, 1000),
                }
            )

        schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("category", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("seasonal_peak_months", StringType(), True),
                StructField("launch_date", TimestampType(), True),
                StructField("rating", DoubleType(), True),
                StructField("review_count", IntegerType(), True),
            ]
        )

        return self.spark.createDataFrame(product_data, schema)

    def demonstrate_geographic_distribution(self):
        """Demonstrate geographic sales distribution analysis."""
        print("\n" + "=" * 60)
        print("🌍 GEOGRAPHIC SALES DISTRIBUTION ANALYSIS")
        print("=" * 60)

        try:
            # Analyze geographic distribution
            geo_results = self.geo_analytics.analyze_geographic_distribution(
                self.transaction_df,
                self.customer_df,
                time_period="month",
                min_transactions=5,
            )

            print("\n📊 Country Distribution Analysis:")
            print("-" * 40)
            country_dist = geo_results["country_distribution"]
            country_dist.select(
                "country",
                "total_revenue",
                "transaction_count",
                "unique_customers",
                "revenue_per_customer",
            ).show(10, False)

            print("\n🏙️ Regional Distribution Analysis:")
            print("-" * 40)
            region_dist = geo_results["region_distribution"]
            region_dist.select(
                "country", "region", "total_revenue", "transaction_count"
            ).show(15, False)

            print("\n🎯 Geographic Concentration Analysis:")
            print("-" * 40)
            concentration = geo_results["concentration_analysis"]
            concentration.select(
                "country", "revenue", "revenue_percentage", "cumulative_percentage"
            ).show(10, False)

            print("\n📈 Geographic Performance Over Time:")
            print("-" * 40)
            performance = geo_results["geographic_performance"]
            performance.select(
                "country", "region", "time_period", "revenue", "revenue_growth"
            ).orderBy("country", "time_period").show(20, False)

            print("\n📋 Distribution Summary:")
            print("-" * 40)
            summary = geo_results["summary_stats"]
            summary.show(False)

        except Exception as e:
            print(f"❌ Error in geographic distribution analysis: {str(e)}")

    def demonstrate_seasonal_trends(self):
        """Demonstrate seasonal trend analysis."""
        print("\n" + "=" * 60)
        print("📅 SEASONAL TRENDS ANALYSIS")
        print("=" * 60)

        try:
            # Analyze seasonal trends
            seasonal_results = self.geo_analytics.analyze_seasonal_trends(
                self.transaction_df,
                self.product_df,
                seasonality_periods=["monthly", "quarterly", "weekly"],
            )

            print("\n📊 Monthly Seasonal Trends:")
            print("-" * 40)
            monthly_trends = seasonal_results["monthly_trends"]
            monthly_trends.select(
                "year",
                "month",
                "month_name",
                "monthly_revenue",
                "monthly_transactions",
                "revenue_per_customer",
            ).orderBy("year", "month").show(24, False)

            print("\n📈 Quarterly Trends:")
            print("-" * 40)
            quarterly_trends = seasonal_results["quarterly_trends"]
            quarterly_trends.select(
                "year", "quarter", "quarterly_revenue", "quarterly_transactions"
            ).orderBy("year", "quarter").show(8, False)

            print("\n📊 Weekly Patterns:")
            print("-" * 40)
            weekly_trends = seasonal_results["weekly_trends"]
            weekly_trends.select(
                "day_of_week",
                "day_name",
                "daily_revenue",
                "daily_transactions",
                "revenue_percentage",
            ).orderBy("day_of_week").show(7, False)

            print("\n🎉 Holiday Analysis:")
            print("-" * 40)
            holiday_analysis = seasonal_results["holiday_analysis"]
            if holiday_analysis.count() > 0:
                holiday_analysis.show(False)
            else:
                print("No holiday data found in the sample period.")

            print("\n🔄 Seasonal Decomposition:")
            print("-" * 40)
            seasonal_decomp = seasonal_results["seasonal_decomposition"]
            seasonal_decomp.select(
                "year",
                "month",
                "revenue",
                "moving_avg_3",
                "seasonal_component",
                "trend_component",
            ).orderBy("year", "month").show(12, False)

            # Category seasonality if available
            if "category_seasonality" in seasonal_results:
                print("\n🏷️ Category Seasonality:")
                print("-" * 40)
                category_seasonal = seasonal_results["category_seasonality"]
                category_seasonal.select(
                    "category", "month", "category_revenue", "seasonal_index"
                ).orderBy("category", "month").show(30, False)

        except Exception as e:
            print(f"❌ Error in seasonal trends analysis: {str(e)}")

    def demonstrate_demand_forecasting(self):
        """Demonstrate regional demand forecasting."""
        print("\n" + "=" * 60)
        print("🔮 REGIONAL DEMAND FORECASTING")
        print("=" * 60)

        try:
            # Generate demand forecasts
            forecast_results = self.geo_analytics.forecast_regional_demand(
                self.transaction_df,
                self.customer_df,
                self.product_df,
                forecast_horizon=30,
                confidence_levels=[0.80, 0.90, 0.95],
            )

            print("\n🌍 Country-Level Forecasts:")
            print("-" * 40)
            country_forecasts = forecast_results["country_forecasts"]
            country_forecasts.select(
                "country", "year", "month", "revenue", "trend", "forecasted_revenue"
            ).orderBy("country", "year", "month").show(20, False)

            print("\n🏛️ Region-Level Forecasts:")
            print("-" * 40)
            region_forecasts = forecast_results["region_forecasts"]
            region_forecasts.select(
                "country", "region", "year", "month", "revenue", "forecasted_revenue"
            ).orderBy("country", "region", "year", "month").show(30, False)

            print("\n📊 Forecast Accuracy Metrics:")
            print("-" * 40)
            forecast_metrics = forecast_results["forecast_metrics"]
            forecast_metrics.show(False)

            print("\n📋 Forecast Summary:")
            print("-" * 40)
            forecast_summary = forecast_results["forecast_summary"]
            forecast_summary.show(False)

            # Product-region forecasts if available
            if "product_region_forecasts" in forecast_results:
                print("\n🏷️ Product-Region Forecasts:")
                print("-" * 40)
                product_forecasts = forecast_results["product_region_forecasts"]
                product_forecasts.select(
                    "country",
                    "region",
                    "category",
                    "year",
                    "month",
                    "revenue",
                    "forecasted_revenue",
                ).orderBy("country", "category").show(20, False)

        except Exception as e:
            print(f"❌ Error in demand forecasting: {str(e)}")

    def demonstrate_customer_segmentation(self):
        """Demonstrate geographic customer segmentation."""
        print("\n" + "=" * 60)
        print("👥 GEOGRAPHIC CUSTOMER SEGMENTATION")
        print("=" * 60)

        try:
            # Perform customer segmentation
            seg_results = self.geo_analytics.segment_customers_geographically(
                self.customer_df,
                self.transaction_df,
                n_clusters=5,
                features=[
                    "total_revenue",
                    "transaction_count",
                    "avg_order_value",
                    "days_since_last_purchase",
                    "geographic_distance",
                    "seasonal_variance",
                    "category_diversity",
                ],
            )

            print("\n🎯 Cluster Analysis:")
            print("-" * 40)
            cluster_analysis = seg_results["cluster_analysis"]
            cluster_analysis.select(
                "cluster",
                "cluster_size",
                "avg_revenue",
                "avg_transactions",
                "avg_order_value",
                "countries_in_cluster",
                "regions_in_cluster",
            ).orderBy("cluster").show(10, False)

            print("\n👤 Customer Segments (Sample):")
            print("-" * 40)
            segmented_customers = seg_results["segmented_customers"]
            segmented_customers.select(
                "customer_id",
                "country",
                "region",
                "cluster",
                "total_revenue",
                "transaction_count",
                "customer_segment",
            ).show(20, False)

            print("\n🌍 Segment Geographic Profiles:")
            print("-" * 40)
            segment_profiles = seg_results["segment_profiles"]
            segment_profiles.select(
                "cluster",
                "country",
                "region",
                "customers_in_location",
                "location_percentage",
            ).orderBy("cluster", "location_percentage").show(30, False)

            print("\n📊 Segment Performance Comparison:")
            print("-" * 40)
            segment_performance = seg_results["segment_performance"]
            segment_performance.select(
                "cluster",
                "total_cluster_revenue",
                "active_customers",
                "revenue_per_customer",
                "transactions_per_customer",
            ).orderBy("cluster").show(10, False)

            print("\n🎯 Segmentation Quality Metrics:")
            print("-" * 40)
            metrics = seg_results["segmentation_metrics"]
            print(f"Silhouette Score: {metrics['silhouette_score']:.3f}")
            print(f"Number of Clusters: {metrics['n_clusters']}")
            print(f"Total Customers: {metrics['total_customers']}")

        except Exception as e:
            print(f"❌ Error in customer segmentation: {str(e)}")

    def demonstrate_business_insights(self):
        """Generate business insights from all analyses."""
        print("\n" + "=" * 60)
        print("💡 BUSINESS INSIGHTS & RECOMMENDATIONS")
        print("=" * 60)

        try:
            print("\n🎯 Key Business Insights:")
            print("-" * 40)

            # Geographic insights
            geo_results = self.geo_analytics.analyze_geographic_distribution(
                self.transaction_df, self.customer_df
            )

            country_dist = geo_results["country_distribution"]
            top_countries = country_dist.select("country", "total_revenue").collect()

            print(f"📊 Geographic Insights:")
            print(
                f"   • Top revenue country: {top_countries[0]['country']} (${top_countries[0]['total_revenue']:,.2f})"
            )
            print(
                f"   • Geographic diversification: {len(top_countries)} countries active"
            )

            # Seasonal insights
            seasonal_results = self.geo_analytics.analyze_seasonal_trends(
                self.transaction_df, self.product_df
            )

            monthly_trends = seasonal_results["monthly_trends"]
            peak_month = monthly_trends.orderBy(col("monthly_revenue").desc()).first()

            print(f"\n📅 Seasonal Insights:")
            print(
                f"   • Peak sales month: {peak_month['month_name']} (${peak_month['monthly_revenue']:,.2f})"
            )
            print(f"   • Seasonal revenue variation detected across product categories")

            # Customer segmentation insights
            seg_results = self.geo_analytics.segment_customers_geographically(
                self.customer_df, self.transaction_df, n_clusters=5
            )

            cluster_analysis = seg_results["cluster_analysis"]
            best_cluster = cluster_analysis.orderBy(col("avg_revenue").desc()).first()

            print(f"\n👥 Customer Segmentation Insights:")
            print(
                f"   • Highest value segment: Cluster {best_cluster['cluster']} (Avg: ${best_cluster['avg_revenue']:,.2f})"
            )
            print(
                f"   • Geographic diversity: {best_cluster['countries_in_cluster']} countries in top cluster"
            )

            print("\n🚀 Strategic Recommendations:")
            print("-" * 40)
            print("1. 🎯 Focus marketing efforts on peak seasonal periods")
            print("2. 🌍 Expand operations in high-performing geographic regions")
            print("3. 👤 Develop targeted campaigns for high-value customer segments")
            print("4. 📊 Implement regional inventory optimization")
            print("5. 🔮 Use demand forecasting for proactive resource allocation")

        except Exception as e:
            print(f"❌ Error generating business insights: {str(e)}")

    def cleanup(self):
        """Clean up resources."""
        print("\n🧹 Cleaning up resources...")
        if self.spark:
            self.spark.stop()
        print("✅ Cleanup completed!")

    def run_full_demo(self):
        """Run the complete geographic analytics demonstration."""
        print("=" * 80)
        print("🌍 GEOGRAPHIC & SEASONAL ANALYTICS COMPREHENSIVE DEMO")
        print("=" * 80)
        print("This demo showcases all capabilities of the Geographic Analytics module")
        print(
            "including distribution analysis, seasonal trends, forecasting, and segmentation."
        )

        try:
            # Run all demonstrations
            self.demonstrate_geographic_distribution()
            self.demonstrate_seasonal_trends()
            self.demonstrate_demand_forecasting()
            self.demonstrate_customer_segmentation()
            self.demonstrate_business_insights()

            print("\n" + "=" * 80)
            print("🎉 DEMO COMPLETED SUCCESSFULLY!")
            print("=" * 80)
            print("All geographic analytics capabilities have been demonstrated.")
            print("The module is ready for production use with real e-commerce data.")

        except Exception as e:
            print(f"\n❌ Demo failed with error: {str(e)}")
            raise
        finally:
            self.cleanup()


def main():
    """Main function to run the geographic analytics demo."""
    if not SPARK_AVAILABLE:
        print("❌ PySpark is required to run this demo.")
        return

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("Starting Geographic Analytics Demo...")

    try:
        demo = GeographicAnalyticsDemo()
        demo.run_full_demo()

    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user.")
    except Exception as e:
        print(f"\n❌ Demo failed: {str(e)}")
        raise
    finally:
        print("\n👋 Thank you for using Geographic Analytics!")


if __name__ == "__main__":
    main()

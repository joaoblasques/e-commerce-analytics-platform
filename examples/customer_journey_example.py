
from pyspark.sql import SparkSession
from src.analytics.customer_journey import CustomerJourney
from datetime import datetime

def main():
    """Main function to run the customer journey analytics example."""
    spark = SparkSession.builder.appName("CustomerJourneyExample").getOrCreate()
    customer_journey = CustomerJourney(spark)

    # Create a dummy DataFrame for demonstration
    data = [
        ("sess1", "user1", "page_view", datetime(2025, 1, 1, 10, 0, 0)),
        ("sess1", "user1", "add_to_cart", datetime(2025, 1, 1, 10, 5, 0)),
        ("sess1", "user1", "purchase", datetime(2025, 1, 1, 10, 10, 0)),
        ("sess2", "user2", "page_view", datetime(2025, 1, 1, 11, 0, 0)),
        ("sess2", "user2", "add_to_cart", datetime(2025, 1, 1, 11, 5, 0)),
        ("sess3", "user3", "page_view", datetime(2025, 1, 1, 12, 0, 0)),
    ]
    schema = ["session_id", "user_id", "event_type", "timestamp"]
    df = spark.createDataFrame(data, schema)

    print("\n--- Tracking Touchpoints ---")
    df_with_touchpoints = customer_journey.track_touchpoints(df)
    df_with_touchpoints.show()

    print("\n--- Funnel Analysis (page_view -> add_to_cart -> purchase) ---")
    funnel_steps = ["page_view", "add_to_cart", "purchase"]
    funnel_results = customer_journey.analyze_funnel(df, funnel_steps)
    funnel_results.show()

    print("\n--- Conversion Rate (add_to_cart to purchase) ---")
    conversion_rate = customer_journey.calculate_conversion_rate(df, "add_to_cart", "purchase")
    print(f"Conversion rate from add_to_cart to purchase: {conversion_rate:.2f}")

if __name__ == "__main__":
    main()
